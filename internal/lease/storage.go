package lease

import (
	"crypto/rand"
	"encoding/hex"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"syscall"

	"github.com/relux-works/swarma-lease/internal/sqlite"
)

var (
	errUnsafePath        = errors.New("unsafe_state_path")
	errSocketPathTooLong = errors.New("socket_path_too_long")
	errBadMode           = errors.New("permissions_invalid")
	errCorrupt           = errors.New("state_corrupt")
	errIncompatible      = errors.New("state_incompatible")
	errStorage           = errors.New("storage_unavailable")
)

type Store struct {
	db       *sqlite.DB
	ownerUID int64
	notifier *changeNotifier
}

func openStore(stateDir string, ownerUID int64, sample Sample) (*Store, error) {
	dbPath := filepath.Join(stateDir, "leases.sqlite")
	info, err := os.Lstat(dbPath)
	if errors.Is(err, os.ErrNotExist) {
		if err := rejectFinalSidecars(stateDir); err != nil {
			return nil, err
		}
		if err := cleanupBootstrapFiles(stateDir, uint32(ownerUID)); err != nil {
			return nil, err
		}
		if err := bootstrapDatabase(stateDir, dbPath, ownerUID, sample); err != nil {
			return nil, err
		}
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
	if err := db.ExecScript("PRAGMA foreign_keys=ON; PRAGMA journal_mode=WAL; PRAGMA synchronous=FULL;"); err != nil {
		_ = db.Close()
		return nil, errStorage
	}
	store := &Store{db: db, ownerUID: ownerUID, notifier: newChangeNotifier()}
	if err := validateDatabase(db, ownerUID); err != nil {
		_ = db.Close()
		return nil, err
	}
	if err := store.recoverBoot(sample); err != nil {
		_ = db.Close()
		return nil, errStorage
	}
	if err := checkSQLiteSidecars(stateDir, uint32(ownerUID)); err != nil {
		_ = db.Close()
		return nil, err
	}
	return store, nil
}

func (s *Store) Close() error { return s.db.Close() }

func bootstrapDatabase(stateDir, finalPath string, ownerUID int64, sample Sample) error {
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
		return errStorage
	}
	if err := db.ExecScript("PRAGMA journal_mode=DELETE; PRAGMA synchronous=FULL; PRAGMA foreign_keys=ON;"); err != nil {
		_ = db.Close()
		return errStorage
	}
	err = db.Transaction(func(tx *sqlite.Tx) error {
		if err := tx.ExecScript(schemaSQL); err != nil {
			return err
		}
		_, err := tx.Exec("INSERT INTO settings(singleton_id,schema_version,owner_uid,max_lease_ns,max_wait_ns,boot_session_id,created_at) VALUES(1,1,?,?,?,?,?)", ownerUID, maxLeaseNS, maxWaitNS, sample.BootSessionID, timestamp(sample.Wall))
		if err != nil {
			return err
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
		return errStorage
	}
	file, err = os.OpenFile(tempPath, os.O_RDWR, 0)
	if err != nil {
		return errStorage
	}
	if err := file.Sync(); err != nil {
		_ = file.Close()
		return errStorage
	}
	if err := file.Close(); err != nil {
		return errStorage
	}
	if err := os.Rename(tempPath, finalPath); err != nil {
		return errStorage
	}
	return syncDirectory(stateDir)
}

func validateDatabase(db *sqlite.DB, ownerUID int64) error {
	rows, err := db.Query("PRAGMA integrity_check")
	if err != nil || len(rows) != 1 || rowString(rows[0], "integrity_check") != "ok" {
		return errCorrupt
	}
	rows, err = db.Query("PRAGMA user_version")
	if err != nil || len(rows) != 1 || rowInt(rows[0], "user_version") != 1 {
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
	wantObjects := map[string]bool{
		"table:audit": true, "table:leases": true, "table:pools": true,
		"table:requests": true, "table:resources": true, "table:settings": true,
		"index:leases_one_active_resource": true, "index:requests_tag_queue_sequence": true,
	}
	wantDefinitions := expectedSchemaDefinitions()
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
	if rowInt(settings[0], "schema_version") != 1 || rowInt(settings[0], "owner_uid") != ownerUID || rowInt(settings[0], "max_lease_ns") != maxLeaseNS || rowInt(settings[0], "max_wait_ns") != maxWaitNS || !validUUID(rowString(settings[0], "boot_session_id")) {
		return errUnsafePath
	}
	fks, err := db.Query("PRAGMA foreign_key_check")
	if err != nil || len(fks) != 0 {
		return errCorrupt
	}
	return nil
}

func expectedSchemaDefinitions() map[string]string {
	definitions := make(map[string]string)
	for _, statement := range strings.Split(schemaSQL, ";") {
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
