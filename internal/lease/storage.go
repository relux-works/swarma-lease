package lease

import (
	"crypto/rand"
	"encoding/hex"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"github.com/relux-works/swarma-lease/internal/sqlite"
)

var (
	errUnsafePath        = errors.New("unsafe_state_path")
	errSocketPathTooLong = errors.New("socket_path_too_long")
	errBadMode           = errors.New("permissions_invalid")
	errCorrupt           = errors.New("state_corrupt")
	errIncompatible      = errors.New("state_incompatible")
	errStorage           = errors.New("storage_unavailable")
	errClockUnavailable  = errors.New("clock_unavailable")
)

type Store struct {
	db       *sqlite.DB
	ownerUID int64
	clock    Clock
	notifier *changeNotifier

	// beforeClockTransactionForTest is a barrier for deterministic transaction-order tests.
	beforeClockTransactionForTest func()
}

func openStore(stateDir string, ownerUID int64, clock Clock) (*Store, error) {
	dbPath := filepath.Join(stateDir, "leases.sqlite")
	createdDatabase := false
	info, err := os.Lstat(dbPath)
	if errors.Is(err, os.ErrNotExist) {
		if err := rejectFinalSidecars(stateDir); err != nil {
			return nil, err
		}
		if err := cleanupBootstrapFiles(stateDir, uint32(ownerUID)); err != nil {
			return nil, err
		}
		if err := bootstrapDatabase(stateDir, dbPath, ownerUID, clock); err != nil {
			return nil, err
		}
		createdDatabase = true
	} else if err != nil {
		return nil, errUnsafePath
	} else if err := checkOwnedFile(dbPath, info, uint32(ownerUID), 0600); err != nil {
		return nil, err
	}
	if err := checkSQLiteSidecars(stateDir, uint32(ownerUID)); err != nil {
		return nil, err
	}

	db, err := sqlite.Open(dbPath)
	if err != nil {
		return nil, errStorage
	}
	if err := db.ExecScript("PRAGMA foreign_keys=ON;"); err != nil {
		_ = db.Close()
		return nil, errStorage
	}
	store := &Store{db: db, ownerUID: ownerUID, clock: clock, notifier: newChangeNotifier()}
	if createdDatabase {
		if err := validateDatabase(db, ownerUID); err != nil {
			_ = db.Close()
			return nil, err
		}
	} else {
		if err := store.migrateAndRecover(); err != nil {
			_ = db.Close()
			return nil, err
		}
	}
	if err := db.ExecScript("PRAGMA journal_mode=WAL; PRAGMA synchronous=FULL;"); err != nil {
		_ = db.Close()
		return nil, errStorage
	}
	if err := validateDatabase(db, ownerUID); err != nil {
		_ = db.Close()
		return nil, err
	}
	if err := checkSQLiteSidecars(stateDir, uint32(ownerUID)); err != nil {
		_ = db.Close()
		return nil, err
	}
	return store, nil
}

func preflightStoreBeforeStaleSocketRemoval(stateDir string, ownerUID int64) error {
	dbPath := filepath.Join(stateDir, "leases.sqlite")
	info, err := os.Lstat(dbPath)
	if errors.Is(err, os.ErrNotExist) {
		return rejectFinalSidecars(stateDir)
	}
	if err != nil {
		return errUnsafePath
	}
	if err := checkOwnedFile(dbPath, info, uint32(ownerUID), 0600); err != nil {
		return err
	}
	if err := checkSQLiteSidecars(stateDir, uint32(ownerUID)); err != nil {
		return err
	}
	db, err := sqlite.OpenReadOnly(dbPath)
	if err != nil {
		return errStorage
	}
	validateErr := validateDatabase(db, ownerUID)
	closeErr := db.Close()
	if validateErr != nil {
		return validateErr
	}
	if closeErr != nil {
		return errStorage
	}
	if err := checkSQLiteSidecars(stateDir, uint32(ownerUID)); err != nil {
		return err
	}
	currentInfo, err := os.Lstat(dbPath)
	if err != nil || !os.SameFile(info, currentInfo) {
		return errUnsafePath
	}
	if err := checkOwnedFile(dbPath, currentInfo, uint32(ownerUID), 0600); err != nil {
		return err
	}
	return nil
}

func (s *Store) Close() error { return s.db.Close() }

func bootstrapDatabase(stateDir, finalPath string, ownerUID int64, clock Clock) error {
	var nonce [16]byte
	if _, err := rand.Read(nonce[:]); err != nil {
		return errStorage
	}
	tempPath := filepath.Join(stateDir, ".leases.sqlite.init-"+hex.EncodeToString(nonce[:]))
	file, err := createOwnedFile(tempPath, uint32(ownerUID), 0600)
	if err != nil {
		return err
	}
	if err := file.Close(); err != nil {
		return errStorage
	}
	db, err := sqlite.Open(tempPath)
	if err != nil {
		_ = cleanupBootstrapFiles(stateDir, uint32(ownerUID))
		return errStorage
	}
	if err := db.ExecScript("PRAGMA journal_mode=DELETE; PRAGMA synchronous=FULL; PRAGMA foreign_keys=ON;"); err != nil {
		_ = db.Close()
		_ = cleanupBootstrapFiles(stateDir, uint32(ownerUID))
		return errStorage
	}
	err = db.Transaction(func(tx *sqlite.Tx) error {
		sample, err := readClockSample(clock)
		if err != nil {
			return err
		}
		if err := tx.ExecScript(schemaSQL); err != nil {
			return err
		}
		_, err = tx.Exec("INSERT INTO settings(singleton_id,schema_version,owner_uid,max_lease_ns,max_wait_ns,boot_session_id,created_at) VALUES(1,?,?,?,?,?,?)", schemaVersion, ownerUID, maxLeaseNS, maxWaitNS, sample.BootSessionID, timestamp(sample.Wall))
		if err != nil {
			return err
		}
		for _, class := range []struct {
			name       string
			priority   int64
			maxLeaseNS int64
		}{{"go-test", 1, maxLeaseNS}, {"jvm-build", 2, int64(30 * time.Minute)}} {
			if _, err := tx.Exec("INSERT INTO lease_classes(class,priority,max_active,max_lease_ns,next_queue_sequence,created_at) VALUES(?,?,1,?,1,?)", class.name, class.priority, class.maxLeaseNS, timestamp(sample.Wall)); err != nil {
				return err
			}
		}
		for _, pool := range []struct{ tag, resource string }{{"mac.go-test", "go-test-01"}, {"mac.swift-test", "swift-test-01"}} {
			if _, err := tx.Exec("INSERT INTO pools(tag,mode,next_queue_sequence,created_at) VALUES(?, 'command', 1, ?)", pool.tag, timestamp(sample.Wall)); err != nil {
				return err
			}
			if _, err := tx.Exec("INSERT INTO resources(id,tag,state,fencing_token,created_at,updated_at) VALUES(?,?,'available',0,?,?)", pool.resource, pool.tag, timestamp(sample.Wall), timestamp(sample.Wall)); err != nil {
				return err
			}
		}
		return nil
	})
	closeErr := db.Close()
	if err != nil || closeErr != nil {
		if cleanupErr := cleanupBootstrapFiles(stateDir, uint32(ownerUID)); cleanupErr != nil {
			return cleanupErr
		}
		if errors.Is(err, errClockUnavailable) && closeErr == nil {
			return errClockUnavailable
		}
		return errStorage
	}
	file, err = os.OpenFile(tempPath, os.O_RDWR, 0)
	if err != nil {
		_ = cleanupBootstrapFiles(stateDir, uint32(ownerUID))
		return errStorage
	}
	if err := file.Sync(); err != nil {
		_ = file.Close()
		_ = cleanupBootstrapFiles(stateDir, uint32(ownerUID))
		return errStorage
	}
	if err := file.Close(); err != nil {
		_ = cleanupBootstrapFiles(stateDir, uint32(ownerUID))
		return errStorage
	}
	if err := os.Rename(tempPath, finalPath); err != nil {
		_ = cleanupBootstrapFiles(stateDir, uint32(ownerUID))
		return errStorage
	}
	return syncDirectory(stateDir)
}

type databaseQueryer interface {
	Query(string, ...any) ([]sqlite.Row, error)
}

func migrateDatabase(tx *sqlite.Tx, ownerUID int64) error {
	rows, err := tx.Query("PRAGMA user_version")
	if err != nil || len(rows) != 1 {
		return errIncompatible
	}
	version := rowInt(rows[0], "user_version")
	switch version {
	case schemaVersion:
		return validateDatabaseVersion(tx, ownerUID, schemaVersion)
	case schemaV1Version:
		if err := validateDatabaseVersion(tx, ownerUID, schemaV1Version); err != nil {
			return err
		}
		if err := migrateV1ToV2(tx); err != nil {
			return err
		}
		if err := validateDatabaseVersion(tx, ownerUID, schemaV2Version); err != nil {
			return err
		}
	case schemaV2Version:
		if err := validateDatabaseVersion(tx, ownerUID, schemaV2Version); err != nil {
			return err
		}
	default:
		return errIncompatible
	}
	if err := migrateV2ToV3(tx); err != nil {
		return err
	}
	return validateDatabaseVersion(tx, ownerUID, schemaVersion)
}

func migrateV1ToV2(tx *sqlite.Tx) error {
	if err := tx.ExecScript("ALTER TABLE settings RENAME TO settings_v1;\n" + settingsV2SQL); err != nil {
		return err
	}
	if _, err := tx.Exec(`INSERT INTO settings(singleton_id,schema_version,owner_uid,max_lease_ns,max_wait_ns,boot_session_id,created_at)
SELECT singleton_id,?,owner_uid,max_lease_ns,?,boot_session_id,created_at FROM settings_v1`, schemaV2Version, maxWaitNS); err != nil {
		return err
	}
	return tx.ExecScript("DROP TABLE settings_v1;\nPRAGMA user_version=2;")
}

func migrateV2ToV3(tx *sqlite.Tx) error {
	if err := tx.ExecScript(
		"DROP INDEX requests_tag_queue_sequence;\n" +
			"DROP INDEX leases_one_active_resource;\n" +
			"ALTER TABLE audit RENAME TO audit_v2;\n" +
			"ALTER TABLE leases RENAME TO leases_v2;\n" +
			"ALTER TABLE requests RENAME TO requests_v2;\n" +
			"ALTER TABLE resources RENAME TO resources_v2;\n" +
			"ALTER TABLE pools RENAME TO pools_v2;\n" +
			"ALTER TABLE settings RENAME TO settings_v2;\n"); err != nil {
		return err
	}
	if err := tx.ExecScript(schemaSQL); err != nil {
		return err
	}
	if _, err := tx.Exec("INSERT INTO settings(singleton_id,schema_version,owner_uid,max_lease_ns,max_wait_ns,boot_session_id,created_at) SELECT singleton_id,3,owner_uid,max_lease_ns,max_wait_ns,boot_session_id,created_at FROM settings_v2"); err != nil {
		return err
	}
	settings, err := tx.Query("SELECT created_at FROM settings_v2 WHERE singleton_id=1")
	if err != nil || len(settings) != 1 {
		return errIncompatible
	}
	createdAt := rowString(settings[0], "created_at")
	for _, class := range []struct {
		name       string
		priority   int64
		maxLeaseNS int64
	}{{"go-test", 1, maxLeaseNS}, {"jvm-build", 2, int64(30 * time.Minute)}} {
		if _, err := tx.Exec("INSERT INTO lease_classes(class,priority,max_active,max_lease_ns,next_queue_sequence,created_at) VALUES(?,?,1,?,1,?)", class.name, class.priority, class.maxLeaseNS, createdAt); err != nil {
			return err
		}
	}
	if _, err := tx.Exec("INSERT INTO pools(tag,class,mode,next_queue_sequence,created_at) SELECT tag,CASE WHEN mode='manual' AND tag='mac.go-test-manual' THEN 'go-test' WHEN mode='manual' AND tag='mac.jvm-build' THEN 'jvm-build' ELSE NULL END,mode,next_queue_sequence,created_at FROM pools_v2"); err != nil {
		return err
	}
	if _, err := tx.Exec("INSERT INTO resources(id,tag,state,fencing_token,created_at,updated_at) SELECT id,tag,state,fencing_token,created_at,updated_at FROM resources_v2"); err != nil {
		return err
	}
	if _, err := tx.Exec("INSERT INTO requests(request_id,requester_id,legacy_unattributed,tag,class,duration_ns,wait_mode,wait_timeout_ns,queue_sequence,class_queue_sequence,queued_at,wait_deadline_at,queue_boot_session_id,queue_started_monotonic_ns,wait_deadline_monotonic_ns,state,outcome_code,created_at,updated_at) SELECT r.request_id,NULL,1,r.tag,p.class,r.duration_ns,r.wait_mode,r.wait_timeout_ns,r.queue_sequence,NULL,r.queued_at,r.wait_deadline_at,r.queue_boot_session_id,r.queue_started_monotonic_ns,r.wait_deadline_monotonic_ns,r.state,r.outcome_code,r.created_at,r.updated_at FROM requests_v2 r LEFT JOIN pools p ON p.tag=r.tag"); err != nil {
		return err
	}
	classes, err := tx.Query("SELECT class FROM lease_classes ORDER BY class COLLATE BINARY")
	if err != nil {
		return err
	}
	for _, classRow := range classes {
		class := rowString(classRow, "class")
		queued, err := tx.Query("SELECT request_id FROM requests WHERE class=? AND state='queued' ORDER BY queued_at COLLATE BINARY,tag COLLATE BINARY,queue_sequence,request_id COLLATE BINARY", class)
		if err != nil {
			return err
		}
		for i, request := range queued {
			if _, err := tx.Exec("UPDATE requests SET class_queue_sequence=? WHERE request_id=?", int64(i+1), rowString(request, "request_id")); err != nil {
				return err
			}
		}
		poolRows, err := tx.Query("SELECT COALESCE(SUM(next_queue_sequence-1),0) AS used FROM pools_v2 WHERE mode='manual' AND ((tag='mac.go-test-manual' AND ?='go-test') OR (tag='mac.jvm-build' AND ?='jvm-build'))", class, class)
		if err != nil || len(poolRows) != 1 {
			return errIncompatible
		}
		nextSequence := rowInt(poolRows[0], "used") + 1
		if nextSequence < int64(len(queued))+1 {
			nextSequence = int64(len(queued)) + 1
		}
		if _, err := tx.Exec("UPDATE lease_classes SET next_queue_sequence=? WHERE class=?", nextSequence, class); err != nil {
			return err
		}
	}
	if _, err := tx.Exec("INSERT INTO leases(id,request_id,holder_id,legacy_unattributed,resource_id,state,duration_ns,granted_at,hard_end_at,boot_session_id,granted_monotonic_ns,hard_end_monotonic_ns,fencing_token,ended_at,ended_resource_state,ended_fencing_token) SELECT id,request_id,NULL,1,resource_id,state,duration_ns,granted_at,hard_end_at,boot_session_id,granted_monotonic_ns,hard_end_monotonic_ns,fencing_token,ended_at,ended_resource_state,ended_fencing_token FROM leases_v2"); err != nil {
		return err
	}
	if _, err := tx.Exec("INSERT INTO audit(sequence,at,operation,actor_kind,actor_component,cause,cause_ref_kind,cause_ref_id,origin,request_id,requester_id,holder_id,legacy_unattributed,target_id,result_code,from_state,to_state) SELECT sequence,at,operation,'legacy',NULL,NULL,NULL,NULL,'pre_v3',request_id,NULL,NULL,1,target_id,result_code,from_state,to_state FROM audit_v2"); err != nil {
		return err
	}
	return tx.ExecScript(
		"DROP TABLE audit_v2;\n" +
			"DROP TABLE leases_v2;\n" +
			"DROP TABLE requests_v2;\n" +
			"DROP TABLE resources_v2;\n" +
			"DROP TABLE pools_v2;\n" +
			"DROP TABLE settings_v2;\n")
}

func (s *Store) migrateAndRecover() error {
	if s.beforeClockTransactionForTest != nil {
		s.beforeClockTransactionForTest()
	}
	var events []TerminalEvent
	err := s.db.Transaction(func(tx *sqlite.Tx) error {
		if err := migrateDatabase(tx, s.ownerUID); err != nil {
			return err
		}
		sample, err := readClockSample(s.clock)
		if err != nil {
			return err
		}
		return s.recoverBootTx(tx, sample, &events)
	})
	if err == nil {
		s.publishEvents(events)
	}
	return err
}

func validateDatabase(db databaseQueryer, ownerUID int64) error {
	rows, err := db.Query("PRAGMA user_version")
	if err != nil || len(rows) != 1 {
		return errIncompatible
	}
	return validateDatabaseVersion(db, ownerUID, rowInt(rows[0], "user_version"))
}

func validateDatabaseVersion(db databaseQueryer, ownerUID, version int64) error {
	rows, err := db.Query("PRAGMA integrity_check")
	if err != nil || len(rows) != 1 || rowString(rows[0], "integrity_check") != "ok" {
		return errCorrupt
	}
	rows, err = db.Query("PRAGMA user_version")
	if err != nil || len(rows) != 1 || rowInt(rows[0], "user_version") != version || (version != schemaV1Version && version != schemaV2Version && version != schemaVersion) {
		return errIncompatible
	}
	expected := map[string][]string{
		"settings":  {"singleton_id", "schema_version", "owner_uid", "max_lease_ns", "max_wait_ns", "boot_session_id", "created_at"},
		"pools":     {"tag", "mode", "next_queue_sequence", "created_at"},
		"resources": {"id", "tag", "state", "fencing_token", "created_at", "updated_at"},
		"requests":  {"request_id", "tag", "duration_ns", "wait_mode", "wait_timeout_ns", "queue_sequence", "queued_at", "wait_deadline_at", "queue_boot_session_id", "queue_started_monotonic_ns", "wait_deadline_monotonic_ns", "state", "outcome_code", "created_at", "updated_at"},
		"leases":    {"id", "request_id", "resource_id", "state", "duration_ns", "granted_at", "hard_end_at", "boot_session_id", "granted_monotonic_ns", "hard_end_monotonic_ns", "fencing_token", "ended_at", "ended_resource_state", "ended_fencing_token"},
		"audit":     {"sequence", "at", "operation", "request_id", "target_id", "result_code", "from_state", "to_state"},
	}
	wantObjects := map[string]bool{
		"table:audit": true, "table:leases": true, "table:pools": true,
		"table:requests": true, "table:resources": true, "table:settings": true,
		"index:leases_one_active_resource": true, "index:requests_tag_queue_sequence": true,
	}
	if version == schemaVersion {
		expected = map[string][]string{
			"settings":      {"singleton_id", "schema_version", "owner_uid", "max_lease_ns", "max_wait_ns", "boot_session_id", "created_at"},
			"lease_classes": {"class", "priority", "max_active", "max_lease_ns", "next_queue_sequence", "created_at"},
			"pools":         {"tag", "class", "mode", "next_queue_sequence", "created_at"},
			"resources":     {"id", "tag", "state", "fencing_token", "created_at", "updated_at"},
			"requests":      {"request_id", "requester_id", "legacy_unattributed", "tag", "class", "duration_ns", "wait_mode", "wait_timeout_ns", "queue_sequence", "class_queue_sequence", "queued_at", "wait_deadline_at", "queue_boot_session_id", "queue_started_monotonic_ns", "wait_deadline_monotonic_ns", "state", "outcome_code", "created_at", "updated_at"},
			"leases":        {"id", "request_id", "holder_id", "legacy_unattributed", "resource_id", "state", "duration_ns", "granted_at", "hard_end_at", "boot_session_id", "granted_monotonic_ns", "hard_end_monotonic_ns", "fencing_token", "ended_at", "ended_resource_state", "ended_fencing_token"},
			"audit":         {"sequence", "at", "operation", "actor_kind", "actor_component", "cause", "cause_ref_kind", "cause_ref_id", "origin", "request_id", "requester_id", "holder_id", "legacy_unattributed", "target_id", "result_code", "from_state", "to_state"},
		}
		wantObjects = map[string]bool{
			"table:audit": true, "table:lease_classes": true, "table:leases": true,
			"table:pools": true, "table:requests": true, "table:resources": true, "table:settings": true,
			"index:leases_one_active_resource": true, "index:requests_class_queue_sequence": true,
			"index:requests_tag_queue_sequence": true,
		}
	}
	for table, columns := range expected {
		actual, err := db.Query("PRAGMA table_info(" + table + ")")
		if err != nil || len(actual) != len(columns) {
			return errIncompatible
		}
		for i, column := range columns {
			if rowString(actual[i], "name") != column {
				return errIncompatible
			}
		}
	}
	objects, err := db.Query("SELECT type,name,sql FROM sqlite_schema WHERE name NOT LIKE 'sqlite_%' ORDER BY type,name")
	if err != nil {
		return errIncompatible
	}
	wantDefinitions := expectedSchemaDefinitionsFor(version)
	if len(objects) != len(wantObjects) || len(wantDefinitions) != len(wantObjects) {
		return errIncompatible
	}
	for _, object := range objects {
		key := rowString(object, "type") + ":" + rowString(object, "name")
		if !wantObjects[key] || normalizeSchemaSQL(rowString(object, "sql")) != wantDefinitions[key] {
			return errIncompatible
		}
	}
	settings, err := db.Query("SELECT schema_version,owner_uid,max_lease_ns,max_wait_ns,boot_session_id FROM settings WHERE singleton_id=1")
	if err != nil || len(settings) != 1 {
		return errIncompatible
	}
	wantMaxWaitNS := maxWaitNS
	if version == 1 {
		wantMaxWaitNS = schemaV1MaxWaitNS
	}
	if rowInt(settings[0], "schema_version") != version || rowInt(settings[0], "owner_uid") != ownerUID || rowInt(settings[0], "max_lease_ns") != maxLeaseNS || rowInt(settings[0], "max_wait_ns") != wantMaxWaitNS {
		return errUnsafePath
	}
	bootSessionID := rowString(settings[0], "boot_session_id")
	if !validUUID(bootSessionID) {
		return errCorrupt
	}
	if err := validateLiveEpochs(db, bootSessionID); err != nil {
		return err
	}
	fks, err := db.Query("PRAGMA foreign_key_check")
	if err != nil || len(fks) != 0 {
		return errCorrupt
	}
	return nil
}

func validateLiveEpochs(db databaseQueryer, settingsBootID string) error {
	leases, err := db.Query("SELECT boot_session_id FROM leases WHERE state='active'")
	if err != nil {
		return errCorrupt
	}
	for _, row := range leases {
		bootID := rowString(row, "boot_session_id")
		if !validUUID(bootID) || bootID != settingsBootID {
			return errCorrupt
		}
	}
	requests, err := db.Query("SELECT queue_boot_session_id FROM requests WHERE state='queued'")
	if err != nil {
		return errCorrupt
	}
	for _, row := range requests {
		bootID := rowString(row, "queue_boot_session_id")
		if !validUUID(bootID) || bootID != settingsBootID {
			return errCorrupt
		}
	}
	return nil
}

func expectedSchemaDefinitions() map[string]string {
	return expectedSchemaDefinitionsFrom(schemaSQL)
}

func expectedSchemaDefinitionsFrom(schema string) map[string]string {
	definitions := make(map[string]string)
	for _, statement := range strings.Split(schema, ";") {
		fields := strings.Fields(strings.TrimSpace(statement))
		if len(fields) < 3 || !strings.EqualFold(fields[0], "CREATE") {
			continue
		}
		var key string
		switch {
		case strings.EqualFold(fields[1], "TABLE"):
			key = "table:" + fields[2]
		case strings.EqualFold(fields[1], "INDEX"):
			key = "index:" + fields[2]
		case len(fields) >= 4 && strings.EqualFold(fields[1], "UNIQUE") && strings.EqualFold(fields[2], "INDEX"):
			key = "index:" + fields[3]
		default:
			continue
		}
		definitions[key] = normalizeSchemaSQL(statement)
	}
	return definitions
}

func expectedSchemaDefinitionsFor(version int64) map[string]string {
	schema := schemaSQL
	if version == schemaV1Version || version == schemaV2Version {
		schema = schemaV2SQL
	}
	definitions := expectedSchemaDefinitionsFrom(schema)
	if version == schemaV1Version {
		definitions["table:settings"] = normalizeSchemaSQL(strings.TrimSuffix(settingsV1SQL, ";"))
	}
	return definitions
}

func normalizeSchemaSQL(statement string) string {
	return strings.Join(strings.Fields(statement), " ")
}

func rejectFinalSidecars(stateDir string) error {
	for _, suffix := range []string{"-journal", "-wal", "-shm"} {
		if _, err := os.Lstat(filepath.Join(stateDir, "leases.sqlite"+suffix)); err == nil {
			return errCorrupt
		} else if !errors.Is(err, os.ErrNotExist) {
			return errUnsafePath
		}
	}
	return nil
}

func cleanupBootstrapFiles(stateDir string, ownerUID uint32) error {
	entries, err := os.ReadDir(stateDir)
	if err != nil {
		return errUnsafePath
	}
	const prefix = ".leases.sqlite.init-"
	for _, entry := range entries {
		name := entry.Name()
		if !strings.HasPrefix(name, prefix) {
			continue
		}
		base := strings.TrimSuffix(name, "-journal")
		nonce := strings.TrimPrefix(base, prefix)
		decoded, decodeErr := hex.DecodeString(nonce)
		if decodeErr != nil || len(decoded) != 16 || strings.ToLower(nonce) != nonce {
			return errCorrupt
		}
		basePath := filepath.Join(stateDir, base)
		if info, statErr := os.Lstat(basePath); statErr == nil {
			if err := checkOwnedFile(basePath, info, ownerUID, 0600); err != nil {
				return err
			}
		} else if !errors.Is(statErr, os.ErrNotExist) {
			return errUnsafePath
		}
		for _, suffix := range []string{"-journal", "-wal", "-shm"} {
			path := basePath + suffix
			info, statErr := os.Lstat(path)
			if errors.Is(statErr, os.ErrNotExist) {
				continue
			}
			if statErr != nil {
				return errUnsafePath
			}
			if suffix != "-journal" {
				return errCorrupt
			}
			if err := checkOwnedFile(path, info, ownerUID, 0600); err != nil {
				return err
			}
			if err := os.Remove(path); err != nil {
				return errStorage
			}
		}
		if _, statErr := os.Lstat(basePath); statErr == nil {
			if err := os.Remove(basePath); err != nil {
				return errStorage
			}
		}
	}
	return nil
}

func checkSQLiteSidecars(stateDir string, ownerUID uint32) error {
	journalPath := filepath.Join(stateDir, "leases.sqlite-journal")
	if _, err := os.Lstat(journalPath); err == nil {
		return errCorrupt
	} else if !errors.Is(err, os.ErrNotExist) {
		return errUnsafePath
	}
	for _, suffix := range []string{"-wal", "-shm"} {
		path := filepath.Join(stateDir, "leases.sqlite"+suffix)
		info, err := os.Lstat(path)
		if errors.Is(err, os.ErrNotExist) {
			continue
		}
		if err != nil {
			return errUnsafePath
		}
		if err := checkOwnedFile(path, info, ownerUID, 0600); err != nil {
			return err
		}
	}
	return nil
}

func checkOwnedFile(path string, info os.FileInfo, ownerUID uint32, mode os.FileMode) error {
	if info.Mode()&os.ModeSymlink != 0 || !info.Mode().IsRegular() {
		return errUnsafePath
	}
	stat, ok := info.Sys().(*syscall.Stat_t)
	if !ok || stat.Uid != ownerUID {
		return errUnsafePath
	}
	if info.Mode().Perm() != mode {
		return errBadMode
	}
	return nil
}

func createOwnedFile(path string, ownerUID uint32, mode os.FileMode) (*os.File, error) {
	fd, err := syscall.Open(path, syscall.O_CREAT|syscall.O_EXCL|syscall.O_RDWR|syscall.O_NOFOLLOW, uint32(mode.Perm()))
	if err != nil {
		return nil, errUnsafePath
	}
	file := os.NewFile(uintptr(fd), path)
	if err := file.Chmod(mode); err != nil {
		_ = file.Close()
		return nil, errBadMode
	}
	info, err := file.Stat()
	if err != nil {
		_ = file.Close()
		return nil, errUnsafePath
	}
	if err := checkOwnedFile(path, info, ownerUID, mode); err != nil {
		_ = file.Close()
		return nil, err
	}
	return file, nil
}

func syncDirectory(path string) error {
	dir, err := os.Open(path)
	if err != nil {
		return errStorage
	}
	defer dir.Close()
	if err := dir.Sync(); err != nil {
		return errStorage
	}
	return nil
}

func rowString(row sqlite.Row, key string) string {
	value, _ := row[key].(string)
	return value
}

func rowInt(row sqlite.Row, key string) int64 {
	value, _ := row[key].(int64)
	return value
}

func nullableString(row sqlite.Row, key string) *string {
	value, ok := row[key].(string)
	if !ok {
		return nil
	}
	return &value
}

func nullableInt(row sqlite.Row, key string) *int64 {
	value, ok := row[key].(int64)
	if !ok {
		return nil
	}
	return &value
}
