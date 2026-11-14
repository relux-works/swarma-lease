package lease

import (
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"
)

// TestStartupSocketRefusalsPreserveOverdueRecoveryState proves four refused final-socket checks leave an existing due lease, queued waiter, audit history, database bytes, and obstructing inode unchanged; removing only the fixture obstruction then permits recovery and grant.
func TestStartupSocketRefusalsPreserveOverdueRecoveryState(t *testing.T) {
	tests := []struct {
		name     string
		wantCode string
		prepare  func(*testing.T, string) (os.FileInfo, func())
	}{
		{
			name:     "wrong-mode-socket",
			wantCode: "permissions_invalid",
			prepare: func(t *testing.T, path string) (os.FileInfo, func()) {
				t.Helper()
				listener, err := net.ListenUnix("unix", &net.UnixAddr{Name: path, Net: "unix"})
				if err != nil {
					t.Fatalf("create wrong-mode socket: %v", err)
				}
				listener.SetUnlinkOnClose(false)
				if err := os.Chmod(path, 0660); err != nil {
					_ = listener.Close()
					t.Fatalf("set wrong socket mode: %v", err)
				}
				info, err := os.Lstat(path)
				if err != nil {
					_ = listener.Close()
					t.Fatalf("stat wrong-mode socket: %v", err)
				}
				return info, func() {
					_ = listener.Close()
					_ = os.Remove(path)
				}
			},
		},
		{
			name:     "regular-file",
			wantCode: "unsafe_state_path",
			prepare: func(t *testing.T, path string) (os.FileInfo, func()) {
				t.Helper()
				if err := os.WriteFile(path, []byte("owned socket obstruction"), 0600); err != nil {
					t.Fatalf("create regular-file obstruction: %v", err)
				}
				info, err := os.Lstat(path)
				if err != nil {
					t.Fatal(err)
				}
				return info, func() { _ = os.Remove(path) }
			},
		},
		{
			name:     "symlink",
			wantCode: "unsafe_state_path",
			prepare: func(t *testing.T, path string) (os.FileInfo, func()) {
				t.Helper()
				target := path + ".target"
				if err := os.WriteFile(target, []byte("owned target"), 0600); err != nil {
					t.Fatalf("create symlink target: %v", err)
				}
				if err := os.Symlink(target, path); err != nil {
					t.Fatalf("create symlink obstruction: %v", err)
				}
				info, err := os.Lstat(path)
				if err != nil {
					t.Fatal(err)
				}
				return info, func() {
					_ = os.Remove(path)
					_ = os.Remove(target)
				}
			},
		},
		{
			name:     "live-socket",
			wantCode: "socket_stale_uncertain",
			prepare: func(t *testing.T, path string) (os.FileInfo, func()) {
				t.Helper()
				listener, err := net.ListenUnix("unix", &net.UnixAddr{Name: path, Net: "unix"})
				if err != nil {
					t.Fatalf("create live socket obstruction: %v", err)
				}
				listener.SetUnlinkOnClose(false)
				if err := os.Chmod(path, 0600); err != nil {
					_ = listener.Close()
					t.Fatalf("set live socket mode: %v", err)
				}
				info, err := os.Lstat(path)
				if err != nil {
					_ = listener.Close()
					t.Fatalf("stat live socket: %v", err)
				}
				return info, func() {
					_ = listener.Close()
					_ = os.Remove(path)
				}
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			stateDir := filepath.Join(shortTestDir(t), "state")
			dbPath := writeCanonicalV1Fixture(t, stateDir, int64(os.Geteuid()))
			base := hardEndBaseSample()
			clock := newControlledLeaseClock(hardEndSampleAfter(base, 11*time.Second, 11*time.Second))
			socketPath := filepath.Join(stateDir, "service.sock")
			obstructionInfo, removeObstruction := tc.prepare(t, socketPath)
			t.Cleanup(removeObstruction)

			beforeDB, err := os.ReadFile(dbPath)
			if err != nil {
				t.Fatal(err)
			}
			server, startErr := Start(Config{StateDir: stateDir, SocketPath: socketPath, Clock: clock})
			if server != nil {
				_ = server.Close()
				t.Fatal("startup unexpectedly succeeded with an obstructed final socket")
			}
			if code := ErrorCode(startErr); code != tc.wantCode {
				t.Fatalf("startup refusal = %q, want %q", code, tc.wantCode)
			}
			afterDB, err := os.ReadFile(dbPath)
			if err != nil || string(afterDB) != string(beforeDB) {
				t.Fatalf("refused startup changed the database: read error=%v bytes equal=%v", err, string(afterDB) == string(beforeDB))
			}
			afterObstruction, err := os.Lstat(socketPath)
			if err != nil || !os.SameFile(obstructionInfo, afterObstruction) {
				t.Fatalf("refused startup changed the obstructing path: stat error=%v same inode=%v", err, err == nil && os.SameFile(obstructionInfo, afterObstruction))
			}
			if afterObstruction.Mode() != obstructionInfo.Mode() {
				t.Fatalf("refused startup changed obstruction mode: before=%s after=%s", obstructionInfo.Mode(), afterObstruction.Mode())
			}

			removeObstruction()
			recovered, err := Start(Config{StateDir: stateDir, SocketPath: socketPath, Clock: clock})
			if err != nil {
				t.Fatalf("startup after removing only the fixture obstruction: %v", err)
			}
			defer recovered.Close()
			state, result, code := recovered.store.Status("fixture-queued", "")
			request, requestOK := result["request"].(*RequestRecord)
			lease, leaseOK := result["lease"].(LeaseRecord)
			if code != "" || state != "leased" || !requestOK || request.State != "leased" || !leaseOK || lease.State != "active" {
				t.Fatalf("recovered queued request = %s/%s/%#v, want one active grant", state, code, result)
			}
			if audits := hardEndAuditCount(t, recovered); audits != 9 {
				t.Fatalf("successful recovery audit rows = %d, want three original rows plus three expiry and three grant rows", audits)
			}
		})
	}
}
