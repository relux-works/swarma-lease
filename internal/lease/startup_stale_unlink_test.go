//go:build darwin

package lease

import (
	"errors"
	"net"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/relux-works/swarma-lease/internal/sqlite"
)

// TestStaleSocketUnlinkRefusalPreservesRecoveryState proves Darwin rejects direct unlink of an immutable stale socket and startup refuses before database mutation, preserving durable state and the original inode. It holds an idle WAL writer open so read-only table snapshots can inspect the WAL; volatile SHM bytes are outside the invariant. The injected removal-failure test covers refusal after backup creation.
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
	db, err = sqlite.Open(dbPath)
	if err != nil {
		t.Fatalf("reopen WAL fixture writer for read-only snapshots: %v", err)
	}
	if _, err := db.Query("SELECT singleton_id FROM settings"); err != nil {
		_ = db.Close()
		t.Fatalf("read WAL fixture to retain shared-memory coordination: %v", err)
	}
	defer db.Close()
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
	if afterFiles := snapshotDatabaseFiles(t, stateDir); !databaseFilesPreserved(beforeFiles, afterFiles) {
		t.Fatalf("failed stale unlink changed durable database or sidecar files:\nbefore=%#v\nafter=%#v", beforeFiles, afterFiles)
	}
	afterBytes, err := os.ReadFile(dbPath)
	if err != nil || !reflect.DeepEqual(beforeBytes, afterBytes) {
		t.Fatalf("failed stale unlink changed database bytes: read error=%v equal=%v", err, reflect.DeepEqual(beforeBytes, afterBytes))
	}
	if afterTables := snapshotCanonicalTables(t, dbPath); !reflect.DeepEqual(beforeTables, afterTables) {
		t.Fatalf("failed stale unlink changed canonical tables:\nbefore=%#v\nafter=%#v", beforeTables, afterTables)
	}
	assertNoStaleSocketBackup(t, stateDir)
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
	if code != "" || state != "leased" || !requestOK || request.State != "leased" || !leaseOK || lease.State != "active" || hardEndAuditCount(t, recovered) != 10 {
		t.Fatalf("valid startup control = %s/%s/%#v, want one active waiter grant and 9 audits", state, code, data)
	}
	assertNoStaleSocketBackup(t, stateDir)
}

// TestStaleSocketRemovalFailurePreservesRecoveryState proves a failed stale-socket removal after its recovery link is created returns before writable recovery, preserving durable database state and the original inode; a normal startup then recovers the overdue waiter.
func TestStaleSocketRemovalFailurePreservesRecoveryState(t *testing.T) {
	root := shortTestDir(t)
	stateDir := filepath.Join(root, "state")
	dbPath := writeCanonicalV1Fixture(t, stateDir, int64(os.Geteuid()))
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
	removeCalls := 0
	backupObserved := false
	server, startErr := startWithSocketRemoval(Config{StateDir: stateDir, SocketPath: socketPath, Clock: clock}, func(path string) error {
		removeCalls++
		if path != socketPath {
			return errors.New("unexpected_socket_path")
		}
		entries, err := os.ReadDir(stateDir)
		if err != nil {
			return err
		}
		for _, entry := range entries {
			if !strings.HasPrefix(entry.Name(), staleSocketBackupDirPrefix) {
				continue
			}
			backupInfo, err := os.Lstat(filepath.Join(stateDir, entry.Name(), "service.sock"))
			if err == nil && os.SameFile(staleInfo, backupInfo) {
				backupObserved = true
			}
		}
		return syscall.EPERM
	})
	if server != nil {
		_ = server.Close()
		t.Fatal("startup succeeded despite injected stale-socket removal refusal")
	}
	if removeCalls != 1 || !backupObserved {
		t.Fatalf("removal path calls=%d backup observed=%v, want one call after preserving the original inode", removeCalls, backupObserved)
	}
	if code := ErrorCode(startErr); code != "socket_stale_uncertain" {
		t.Fatalf("startup refusal = %q, want socket_stale_uncertain", code)
	}
	if afterFiles := snapshotDatabaseFiles(t, stateDir); !databaseFilesPreserved(beforeFiles, afterFiles) {
		t.Fatalf("failed stale-socket removal changed durable database or sidecar files:\nbefore=%#v\nafter=%#v", beforeFiles, afterFiles)
	}
	afterBytes, err := os.ReadFile(dbPath)
	if err != nil || !reflect.DeepEqual(beforeBytes, afterBytes) {
		t.Fatalf("failed stale-socket removal changed database bytes: read error=%v equal=%v", err, reflect.DeepEqual(beforeBytes, afterBytes))
	}
	if afterTables := snapshotCanonicalTables(t, dbPath); !reflect.DeepEqual(beforeTables, afterTables) {
		t.Fatalf("failed stale-socket removal changed canonical tables:\nbefore=%#v\nafter=%#v", beforeTables, afterTables)
	}
	currentInfo, err := os.Lstat(socketPath)
	if err != nil || !os.SameFile(staleInfo, currentInfo) || currentInfo.Mode().Perm() != 0600 {
		t.Fatalf("refusal failed to preserve original stale socket: stat error=%v", err)
	}
	if err := proveStaleSocket(socketPath, staleInfo, uint32(os.Geteuid())); err != nil {
		t.Fatalf("preserved socket is no longer proven stale: %v", err)
	}
	assertNoStaleSocketBackup(t, stateDir)

	recovered, err := Start(Config{StateDir: stateDir, SocketPath: socketPath, Clock: clock})
	if err != nil {
		t.Fatalf("valid startup control failed after removing the injected refusal: %v", err)
	}
	defer recovered.Close()
	state, data, code := recovered.store.Status("fixture-queued", "")
	request, requestOK := data["request"].(*RequestRecord)
	lease, leaseOK := data["lease"].(LeaseRecord)
	if code != "" || state != "leased" || !requestOK || request.State != "leased" || !leaseOK || lease.State != "active" || hardEndAuditCount(t, recovered) != 10 {
		t.Fatalf("valid removal control = %s/%s/%#v, want one active waiter grant and 9 audits", state, code, data)
	}
	assertNoStaleSocketBackup(t, stateDir)
}

// TestWritableRecoveryRefusalRestoresOriginalStaleSocket proves a Darwin database write refusal preserves main database, journal/WAL bytes, and canonical tables while restoring the same stale-socket inode; SQLite SHM bytes are volatile. Clearing only the database flag permits overdue recovery and grant.
func TestWritableRecoveryRefusalRestoresOriginalStaleSocket(t *testing.T) {
	root := shortTestDir(t)
	stateDir := filepath.Join(root, "state")
	dbPath := writeCanonicalV1Fixture(t, stateDir, int64(os.Geteuid()))
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
	if err := proveStaleSocket(socketPath, staleInfo, uint32(os.Geteuid())); err != nil {
		t.Fatalf("fixture is not independently proven stale: %v", err)
	}
	beforeTables := snapshotCanonicalTables(t, dbPath)
	beforeBytes, err := os.ReadFile(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	beforeFiles := snapshotDatabaseFiles(t, stateDir)
	// UF_IMMUTABLE is 0x00000002 in the installed Darwin sys/stat.h.
	if err := syscall.Chflags(dbPath, 0x00000002); err != nil {
		t.Skipf("fixture setup cannot set user immutable on an owned database: %v", err)
	}
	t.Cleanup(func() {
		if err := syscall.Chflags(dbPath, 0); err != nil && !errors.Is(err, syscall.ENOENT) {
			t.Errorf("clear only task database flags: %v", err)
		}
	})
	base := hardEndBaseSample()
	clock := newControlledLeaseClock(hardEndSampleAfter(base, 11*time.Second, 11*time.Second))
	server, startErr := Start(Config{StateDir: stateDir, SocketPath: socketPath, Clock: clock})
	if server != nil {
		_ = server.Close()
		t.Fatal("startup unexpectedly succeeded with a write-protected database")
	}
	if code := ErrorCode(startErr); code != "storage_unavailable" {
		t.Fatalf("startup refusal = %q, want storage_unavailable", code)
	}
	if afterFiles := snapshotDatabaseFiles(t, stateDir); !databaseFilesPreserved(beforeFiles, afterFiles) {
		t.Fatalf("writable recovery refusal changed durable database or sidecar files:\nbefore=%#v\nafter=%#v", beforeFiles, afterFiles)
	}
	afterBytes, err := os.ReadFile(dbPath)
	if err != nil || !reflect.DeepEqual(beforeBytes, afterBytes) {
		t.Fatalf("writable recovery refusal changed database bytes: read error=%v equal=%v", err, reflect.DeepEqual(beforeBytes, afterBytes))
	}
	if afterTables := snapshotCanonicalTables(t, dbPath); !reflect.DeepEqual(beforeTables, afterTables) {
		t.Fatalf("writable recovery refusal changed canonical tables:\nbefore=%#v\nafter=%#v", beforeTables, afterTables)
	}
	currentInfo, err := os.Lstat(socketPath)
	if err != nil || !os.SameFile(staleInfo, currentInfo) || currentInfo.Mode().Perm() != 0600 {
		t.Fatalf("writable recovery refusal failed to restore original stale socket: stat error=%v same inode=%v", err, err == nil && os.SameFile(staleInfo, currentInfo))
	}
	if err := proveStaleSocket(socketPath, staleInfo, uint32(os.Geteuid())); err != nil {
		t.Fatalf("restored socket is no longer proven stale: %v", err)
	}
	assertNoStaleSocketBackup(t, stateDir)

	if err := syscall.Chflags(dbPath, 0); err != nil {
		t.Fatal(err)
	}
	recovered, err := Start(Config{StateDir: stateDir, SocketPath: socketPath, Clock: clock})
	if err != nil {
		t.Fatalf("clearing only the database immutable bit did not permit startup: %v", err)
	}
	defer recovered.Close()
	state, data, code := recovered.store.Status("fixture-queued", "")
	request, requestOK := data["request"].(*RequestRecord)
	lease, leaseOK := data["lease"].(LeaseRecord)
	if code != "" || state != "leased" || !requestOK || request.State != "leased" || !leaseOK || lease.State != "active" || hardEndAuditCount(t, recovered) != 10 {
		t.Fatalf("valid writable-recovery control = %s/%s/%#v, want one active waiter grant and 9 audits", state, code, data)
	}
	newInfo, err := os.Lstat(socketPath)
	if err != nil || newInfo.Mode()&os.ModeSocket == 0 || newInfo.Mode()&os.ModeSymlink != 0 || os.SameFile(staleInfo, newInfo) {
		t.Fatalf("valid startup did not replace the stale path with a new service socket: stat error=%v", err)
	}
	assertNoStaleSocketBackup(t, stateDir)
}

func assertNoStaleSocketBackup(t *testing.T, stateDir string) {
	t.Helper()
	entries, err := os.ReadDir(stateDir)
	if err != nil {
		t.Fatalf("read state directory after stale-socket handling: %v", err)
	}
	for _, entry := range entries {
		if strings.HasPrefix(entry.Name(), staleSocketBackupDirPrefix) {
			t.Fatalf("stale-socket backup directory remains after startup: %q", entry.Name())
		}
	}
}

type databaseFileSnapshot struct {
	exists bool
	bytes  []byte
	mode   os.FileMode
	owner  uint32
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
		stat, ok := info.Sys().(*syscall.Stat_t)
		if !ok {
			t.Fatalf("database file %q has unexpected stat type %T", suffix, info.Sys())
		}
		if stat.Uid != uint32(os.Geteuid()) || info.Mode().Perm() != 0600 {
			t.Fatalf("database file %q owner/mode = %d/%#o, want effective UID/%#o", suffix, stat.Uid, info.Mode().Perm(), 0600)
		}
		snapshot := databaseFileSnapshot{exists: true, mode: info.Mode(), owner: stat.Uid}
		if suffix != "-shm" {
			contents, err := os.ReadFile(path)
			if err != nil {
				t.Fatalf("read database file %q: %v", suffix, err)
			}
			snapshot.bytes = contents
		}
		result[suffix] = snapshot
	}
	return result
}

func databaseFilesPreserved(before, after map[string]databaseFileSnapshot) bool {
	// SQLite's -shm file is volatile coordination; snapshotDatabaseFiles validates its owner and mode without comparing its bytes or presence.
	for _, suffix := range []string{"", "-journal", "-wal"} {
		if !reflect.DeepEqual(before[suffix], after[suffix]) {
			return false
		}
	}
	return true
}
