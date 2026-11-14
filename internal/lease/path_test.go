package lease

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// TestSocketPathCapacityIncludesTerminator proves the exact maximum accepted byte length and its adjacent rejected value for the current Unix platform.
func TestSocketPathCapacityIncludesTerminator(t *testing.T) {
	capacity := socketPathCapacity()
	if capacity < 2 {
		t.Fatalf("unsupported socket path capacity %d", capacity)
	}
	if socketPathTooLong(strings.Repeat("s", capacity-1)) {
		t.Fatalf("path of %d bytes plus terminator was rejected at capacity %d", capacity-1, capacity)
	}
	if !socketPathTooLong(strings.Repeat("s", capacity)) {
		t.Fatalf("path of %d bytes plus terminator was accepted at capacity %d", capacity, capacity)
	}
}

// TestStartRejectsOverlongSocketPathBeforeStateCreation proves an over-limit path returns its dedicated code without creating the state directory or any fixture files.
func TestStartRejectsOverlongSocketPathBeforeStateCreation(t *testing.T) {
	root := shortTestDir(t)
	stateDir := filepath.Join(root, "state")
	socketPath := filepath.Join(root, strings.Repeat("s", socketPathCapacity()))
	server, err := Start(Config{StateDir: stateDir, SocketPath: socketPath})
	if server != nil {
		if closeErr := server.Close(); closeErr != nil {
			t.Fatalf("close unexpectedly started service: %v", closeErr)
		}
	}
	if ErrorCode(err) != "socket_path_too_long" {
		t.Fatalf("overlong socket path code = %q, want socket_path_too_long", ErrorCode(err))
	}
	if _, err := os.Lstat(stateDir); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("state directory after path refusal = %v, want absent", err)
	}
	entries, err := os.ReadDir(root)
	if err != nil || len(entries) != 0 {
		t.Fatalf("fixture entries after path refusal = %v, %v; want none", entries, err)
	}
}

// TestStartRejectsSymlinkedAncestorsBeforeStateCreation proves both state-directory and custom socket-parent symlinks are refused without creating database, directory, or socket entries.
func TestStartRejectsSymlinkedAncestorsBeforeStateCreation(t *testing.T) {
	root := shortTestDir(t)
	real := filepath.Join(root, "real")
	if err := os.Mkdir(real, 0700); err != nil {
		t.Fatalf("create real test directory: %v", err)
	}
	alias := filepath.Join(root, "alias")
	if err := os.Symlink(real, alias); err != nil {
		t.Fatalf("create symlink fixture: %v", err)
	}
	for _, test := range []struct {
		name, stateDir, socketPath string
	}{
		{name: "state directory ancestor", stateDir: filepath.Join(alias, "state"), socketPath: filepath.Join(real, "service.sock")},
		{name: "socket parent ancestor", stateDir: filepath.Join(real, "state"), socketPath: filepath.Join(alias, "service.sock")},
	} {
		t.Run(test.name, func(t *testing.T) {
			server, err := Start(Config{StateDir: test.stateDir, SocketPath: test.socketPath})
			if server != nil {
				if closeErr := server.Close(); closeErr != nil {
					t.Fatalf("close unexpectedly started service: %v", closeErr)
				}
			}
			if ErrorCode(err) != "unsafe_state_path" {
				t.Fatalf("symlinked path code = %q, want unsafe_state_path", ErrorCode(err))
			}
			entries, err := os.ReadDir(real)
			if err != nil || len(entries) != 0 {
				t.Fatalf("real directory entries after refusal = %v, %v; want none", entries, err)
			}
		})
	}
}

// TestDefaultSocketPathFitsRealisticHomes proves representative macOS/Linux account homes and the current account path fit the platform socket capacity without exposing a home path.
func TestDefaultSocketPathFitsRealisticHomes(t *testing.T) {
	for _, home := range []string{"/Users/example", "/home/example"} {
		if socketPathTooLong(defaultSocketPath(home)) {
			t.Fatalf("representative home socket path exceeds %d bytes including terminator", socketPathCapacity())
		}
	}
	if os.Geteuid() == 0 {
		t.Skip("service defaults refuse root")
	}
	path, err := DefaultSocketPath()
	if err != nil {
		t.Fatalf("resolve current account socket path: %v", err)
	}
	if socketPathTooLong(path) {
		t.Fatalf("current account socket path exceeds %d bytes including terminator", socketPathCapacity())
	}
}
