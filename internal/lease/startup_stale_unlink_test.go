//go:build darwin

package lease

import (
	"errors"
	"net"
	"os"
	"path/filepath"
	"reflect"
	"syscall"
	"testing"
	"time"

	"github.com/relux-works/swarma-lease/internal/sqlite"
)

// TestStaleSocketUnlinkRefusalPreservesRecoveryState proves a deterministic stale-socket unlink refusal leaves database and sidecar files, all canonical tables, and the original socket unchanged; clearing the task fixture obstruction allows overdue recovery. It does not cover every later filesystem failure or a concurrent path swap.
func TestStaleSocketUnlinkRefusalPreservesRecoveryState(t *testing.T) {
	root := shortTestDir(t)
	stateDir := filepath.Join(root, "state")
	dbPath := writeCanonicalV1Fixture(t, stateDir, int64(os.Geteuid()))
	db, err := sqlite.Open(dbPath)
	if err != nil {
		t.Fatalf("open fixture to select WAL mode: %v", err)
	}
	if err := db.ExecScript("PRAGMA journal_mode=WAL;"); err != nil {
		_ = db.Close()
		t.Fatalf("select fixture WAL mode: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("close WAL fixture: %v", err)
	}
	socketPath := filepath.Join(stateDir, "service.sock")
	listener, err := net.ListenUnix("unix", &net.UnixAddr{Name: socketPath, Net: "unix"})
	if err != nil {
		t.Fatal(err)
	}
	listener.SetUnlinkOnClose(false)
	if err := os.Chmod(socketPath, 0600); err != nil {
		_ = listener.Close()
		t.Fatal(err)
	}
	if err := listener.Close(); err != nil {
		t.Fatal(err)
	}
	staleInfo, err := os.Lstat(socketPath)
	if err != nil {
		t.Fatal(err)
	}
	// UF_IMMUTABLE is 0x00000002 in the installed Darwin sys/stat.h.
	if err := syscall.Chflags(socketPath, 0x00000002); err != nil {
		t.Skipf("fixture setup cannot set user immutable on an owned socket: %v", err)
	}
	t.Cleanup(func() {
		if err := syscall.Chflags(socketPath, 0); err != nil && !errors.Is(err, syscall.ENOENT) {
			t.Errorf("clear only task socket flags: %v", err)
		}
	})
	if err := os.Remove(socketPath); !errors.Is(err, syscall.EPERM) {
		t.Fatalf("independent immutable unlink control = %v, want EPERM", err)
	}
	if err := proveStaleSocket(socketPath, staleInfo, uint32(os.Geteuid())); err != nil {
		t.Fatalf("fixture is not independently proven stale: %v", err)
	}
	beforeTables := snapshotCanonicalTables(t, dbPath)
	beforeBytes, err := os.ReadFile(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	beforeFiles := snapshotDatabaseFiles(t, stateDir)
	base := hardEndBaseSample()
	clock := newControlledLeaseClock(hardEndSampleAfter(base, 11*time.Second, 11*time.Second))
	server, startErr := Start(Config{StateDir: stateDir, SocketPath: socketPath, Clock: clock})
	if server != nil {
		_ = server.Close()
		t.Fatal("startup succeeded despite independently proven unlink refusal")
	}
	if code := ErrorCode(startErr); code != "socket_stale_uncertain" {
		t.Fatalf("startup refusal = %q, want socket_stale_uncertain", code)
	}
	if afterFiles := snapshotDatabaseFiles(t, stateDir); !reflect.DeepEqual(beforeFiles, afterFiles) {
		t.Fatalf("failed stale unlink changed database or sidecar files:\nbefore=%#v\nafter=%#v", beforeFiles, afterFiles)
	}
	afterBytes, err := os.ReadFile(dbPath)
	if err != nil || !reflect.DeepEqual(beforeBytes, afterBytes) {
		t.Fatalf("failed stale unlink changed database bytes: read error=%v equal=%v", err, reflect.DeepEqual(beforeBytes, afterBytes))
	}
	if afterTables := snapshotCanonicalTables(t, dbPath); !reflect.DeepEqual(beforeTables, afterTables) {
		t.Fatalf("failed stale unlink changed canonical tables:\nbefore=%#v\nafter=%#v", beforeTables, afterTables)
	}
	currentInfo, err := os.Lstat(socketPath)
	if err != nil || !os.SameFile(staleInfo, currentInfo) || currentInfo.Mode().Perm() != 0600 {
		t.Fatalf("refusal failed to preserve original stale socket: stat error=%v", err)
	}
	if err := syscall.Chflags(socketPath, 0); err != nil {
		t.Fatal(err)
	}
	recovered, err := Start(Config{StateDir: stateDir, SocketPath: socketPath, Clock: clock})
	if err != nil {
		t.Fatalf("clearing only fixture immutable bit did not permit startup: %v", err)
	}
	defer recovered.Close()
	state, data, code := recovered.store.Status("fixture-queued", "")
	request, requestOK := data["request"].(*RequestRecord)
	lease, leaseOK := data["lease"].(LeaseRecord)
	if code != "" || state != "leased" || !requestOK || request.State != "leased" || !leaseOK || lease.State != "active" || hardEndAuditCount(t, recovered) != 9 {
		t.Fatalf("valid startup control = %s/%s/%#v, want one active waiter grant and 9 audits", state, code, data)
	}
}

type databaseFileSnapshot struct {
	exists bool
	bytes  []byte
}

func snapshotDatabaseFiles(t *testing.T, stateDir string) map[string]databaseFileSnapshot {
	t.Helper()
	result := make(map[string]databaseFileSnapshot)
	for _, suffix := range []string{"", "-journal", "-wal", "-shm"} {
		path := filepath.Join(stateDir, "leases.sqlite"+suffix)
		info, err := os.Lstat(path)
		if errors.Is(err, os.ErrNotExist) {
			result[suffix] = databaseFileSnapshot{}
			continue
		}
		if err != nil {
			t.Fatalf("stat database file %q: %v", suffix, err)
		}
		if !info.Mode().IsRegular() {
			t.Fatalf("database file %q has mode %v, want regular file", suffix, info.Mode())
		}
		contents, err := os.ReadFile(path)
		if err != nil {
			t.Fatalf("read database file %q: %v", suffix, err)
		}
		result[suffix] = databaseFileSnapshot{exists: true, bytes: contents}
	}
	return result
}
