package lease

import (
	"net"
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"github.com/relux-works/swarma-lease/internal/sqlite"
)

// TestLateSocketParentRefusalPreservesRecoveryState proves a denied write to a configured socket parent refuses before overdue recovery changes the database; it covers a deterministic mode-0500 refusal, not concurrent path changes or every possible later bind error.
func TestLateSocketParentRefusalPreservesRecoveryState(t *testing.T) {
	root := shortTestDir(t)
	stateDir := filepath.Join(root, "state")
	dbPath := writeCanonicalV1Fixture(t, stateDir, int64(os.Geteuid()))
	socketParent := filepath.Join(root, "socket-parent")
	if err := os.Mkdir(socketParent, 0500); err != nil {
		t.Fatalf("create denied socket parent: %v", err)
	}
	if err := os.Chmod(socketParent, 0500); err != nil {
		t.Fatalf("set denied socket parent mode: %v", err)
	}
	controlPath := filepath.Join(socketParent, "create-control")
	control, err := os.OpenFile(controlPath, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	if err == nil {
		_ = control.Close()
		_ = os.Remove(controlPath)
		t.Fatal("fixture parent unexpectedly allowed file creation")
	}
	if !os.IsPermission(err) {
		t.Fatalf("fixture create-file control failed for unrelated reason: %v", err)
	}

	base := hardEndBaseSample()
	clock := newControlledLeaseClock(hardEndSampleAfter(base, 11*time.Second, 11*time.Second))
	socketPath := filepath.Join(socketParent, "service.sock")
	beforeDB, err := os.ReadFile(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	beforeTables := snapshotCanonicalTables(t, dbPath)
	server, startErr := Start(Config{StateDir: stateDir, SocketPath: socketPath, Clock: clock})
	if server != nil {
		_ = server.Close()
		t.Fatal("startup unexpectedly succeeded with a non-writable socket parent")
	}
	if code := ErrorCode(startErr); code != "unsafe_state_path" {
		t.Fatalf("startup refusal = %q, want unsafe_state_path", code)
	}
	afterDB, err := os.ReadFile(dbPath)
	if err != nil || !reflect.DeepEqual(afterDB, beforeDB) {
		t.Fatalf("refused startup changed database bytes: read error=%v equal=%v", err, reflect.DeepEqual(afterDB, beforeDB))
	}
	if afterTables := snapshotCanonicalTables(t, dbPath); !reflect.DeepEqual(afterTables, beforeTables) {
		t.Fatalf("refused startup changed canonical tables:\nbefore=%#v\nafter=%#v", beforeTables, afterTables)
	}
	if _, err := os.Lstat(socketPath); !os.IsNotExist(err) {
		t.Fatalf("refused startup left socket path state: lstat error=%v", err)
	}

	if err := os.Chmod(socketParent, 0700); err != nil {
		t.Fatalf("make socket parent writable for valid control: %v", err)
	}
	recovered, err := Start(Config{StateDir: stateDir, SocketPath: socketPath, Clock: clock})
	if err != nil {
		t.Fatalf("startup with writable socket parent: %v", err)
	}
	defer recovered.Close()
	state, result, code := recovered.store.Status("fixture-queued", "")
	request, requestOK := result["request"].(*RequestRecord)
	lease, leaseOK := result["lease"].(LeaseRecord)
	if code != "" || state != "leased" || !requestOK || request.State != "leased" || !leaseOK || lease.State != "active" {
		t.Fatalf("valid recovery control = %s/%s/%#v, want one active grant", state, code, result)
	}
	if audits := hardEndAuditCount(t, recovered); audits != 10 {
		t.Fatalf("valid recovery control audit rows = %d, want 10", audits)
	}
}

// TestInvalidDatabasePreservesProvenStaleSocket proves database verification failure leaves a proven-stale socket inode and all six canonical tables untouched; restoring the schema version permits a live mode-0600 socket.
func TestInvalidDatabasePreservesProvenStaleSocket(t *testing.T) {
	stateDir := filepath.Join(shortTestDir(t), "state")
	dbPath := writeCanonicalV1Fixture(t, stateDir, int64(os.Geteuid()))
	socketPath := filepath.Join(stateDir, "service.sock")
	listener, err := net.ListenUnix("unix", &net.UnixAddr{Name: socketPath, Net: "unix"})
	if err != nil {
		t.Fatalf("create stale-socket fixture: %v", err)
	}
	listener.SetUnlinkOnClose(false)
	if err := os.Chmod(socketPath, 0600); err != nil {
		_ = listener.Close()
		t.Fatalf("set stale-socket mode: %v", err)
	}
	staleInfo, err := os.Lstat(socketPath)
	if err != nil {
		_ = listener.Close()
		t.Fatalf("stat stale-socket fixture: %v", err)
	}
	if err := listener.Close(); err != nil {
		t.Fatalf("close stale-socket listener without unlink: %v", err)
	}
	defer os.Remove(socketPath)

	db, err := sqlite.Open(dbPath)
	if err != nil {
		t.Fatalf("open fixture database: %v", err)
	}
	if err := db.ExecScript("PRAGMA user_version=3;"); err != nil {
		_ = db.Close()
		t.Fatalf("set incompatible fixture version: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("close incompatible fixture database: %v", err)
	}
	beforeDB, err := os.ReadFile(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	beforeTables := snapshotCanonicalTables(t, dbPath)
	clock := newControlledLeaseClock(hardEndBaseSample())
	server, startErr := Start(Config{StateDir: stateDir, SocketPath: socketPath, Clock: clock})
	if server != nil {
		_ = server.Close()
		t.Fatal("startup unexpectedly succeeded with an incompatible database")
	}
	if code := ErrorCode(startErr); code != "state_incompatible" {
		t.Fatalf("startup refusal = %q, want state_incompatible", code)
	}
	afterDB, err := os.ReadFile(dbPath)
	if err != nil || !reflect.DeepEqual(afterDB, beforeDB) {
		t.Fatalf("refused startup changed database bytes: read error=%v equal=%v", err, reflect.DeepEqual(afterDB, beforeDB))
	}
	if afterTables := snapshotCanonicalTables(t, dbPath); !reflect.DeepEqual(afterTables, beforeTables) {
		t.Fatalf("refused startup changed canonical tables:\nbefore=%#v\nafter=%#v", beforeTables, afterTables)
	}
	afterInfo, err := os.Lstat(socketPath)
	if err != nil || !os.SameFile(staleInfo, afterInfo) || afterInfo.Mode() != staleInfo.Mode() {
		t.Fatalf("refused startup changed stale socket: stat error=%v same inode=%v", err, err == nil && os.SameFile(staleInfo, afterInfo))
	}

	db, err = sqlite.Open(dbPath)
	if err != nil {
		t.Fatalf("reopen fixture database for valid control: %v", err)
	}
	if err := db.ExecScript("PRAGMA user_version=1;"); err != nil {
		_ = db.Close()
		t.Fatalf("restore valid fixture version: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("close restored fixture database: %v", err)
	}
	recovered, err := Start(Config{StateDir: stateDir, SocketPath: socketPath, Clock: clock})
	if err != nil {
		t.Fatalf("startup with restored schema: %v", err)
	}
	defer recovered.Close()
	currentInfo, err := os.Lstat(socketPath)
	if err != nil {
		t.Fatalf("stat valid startup socket: %v", err)
	}
	if currentInfo.Mode().Perm() != 0600 {
		t.Fatalf("valid startup socket mode = %04o; want 0600", currentInfo.Mode().Perm())
	}
	conn, err := net.DialTimeout("unix", socketPath, time.Second)
	if err != nil {
		t.Fatalf("dial valid startup socket: %v", err)
	}
	if err := conn.Close(); err != nil {
		t.Fatalf("close valid startup socket connection: %v", err)
	}
	state, result, code := recovered.store.Status("fixture-active", "")
	request, requestOK := result["request"].(*RequestRecord)
	lease, leaseOK := result["lease"].(LeaseRecord)
	if code != "" || state != "leased" || !requestOK || request.State != "leased" || !leaseOK || lease.State != "active" {
		t.Fatalf("valid database control = %s/%s/%#v, want existing active lease", state, code, result)
	}
}

func snapshotCanonicalTables(t *testing.T, dbPath string) map[string][]sqlite.Row {
	t.Helper()
	db, err := sqlite.OpenReadOnly(dbPath)
	if err != nil {
		t.Fatalf("open database read-only for independent table snapshot: %v", err)
	}
	defer db.Close()
	tables := []struct {
		name  string
		query string
	}{
		{name: "settings", query: "SELECT * FROM settings ORDER BY singleton_id"},
		{name: "pools", query: "SELECT * FROM pools ORDER BY tag"},
		{name: "resources", query: "SELECT * FROM resources ORDER BY id"},
		{name: "requests", query: "SELECT * FROM requests ORDER BY request_id"},
		{name: "leases", query: "SELECT * FROM leases ORDER BY id"},
		{name: "audit", query: "SELECT * FROM audit ORDER BY sequence"},
	}
	snapshot := make(map[string][]sqlite.Row, len(tables))
	for _, table := range tables {
		rows, err := db.Query(table.query)
		if err != nil {
			t.Fatalf("snapshot %s table: %v", table.name, err)
		}
		snapshot[table.name] = rows
	}
	return snapshot
}
