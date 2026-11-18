//go:build darwin

package cli

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/relux-works/swarma-lease/internal/lease"
)

const (
	launchdTestEnabledEnv = "SWARMA_LEASE_LAUNCHD_TEST"
	launchdTestDomainEnv  = "SWARMA_LEASE_LAUNCHD_TEST_DOMAIN"
	launchdTestChildEnv   = "SWARMA_LEASE_LAUNCHD_CHILD"
	launchdTestRootEnv    = "SWARMA_LEASE_LAUNCHD_ROOT"
	launchdTestCountEnv   = "SWARMA_LEASE_LAUNCHD_START_COUNT"
)

// TestLaunchdDomainGuardAllowsOnlyOwnBackgroundDomain proves the fixture accepts its own user domain and rejects GUI, foreign-user, and root targets before any launchctl mutation.
func TestLaunchdDomainGuardAllowsOnlyOwnBackgroundDomain(t *testing.T) {
	uid := 501
	if !launchdTestDomainAllowed(fmt.Sprintf("user/%d", uid), uid) {
		t.Fatal("own background user domain was rejected")
	}
	for _, domain := range []string{
		fmt.Sprintf("gui/%d", uid),
		fmt.Sprintf("user/%d", uid+1),
		"system",
		"user/0",
	} {
		if launchdTestDomainAllowed(domain, uid) {
			t.Errorf("unsafe domain %q was accepted", domain)
		}
	}
	if launchdTestDomainAllowed("user/0", 0) {
		t.Fatal("root user domain was accepted")
	}
}

// TestLaunchAgentCrashRestartInIsolatedUserDomain proves RunAtLoad starts a private-state service in the separate user domain and KeepAlive restarts it after SIGKILL; it skips unless the leased manual lane opts in.
func TestLaunchAgentCrashRestartInIsolatedUserDomain(t *testing.T) {
	if os.Getenv(launchdTestEnabledEnv) != "1" {
		t.Skip("requires the leased isolated macOS launchd lane")
	}
	uid := os.Geteuid()
	domain := os.Getenv(launchdTestDomainEnv)
	if !launchdTestDomainAllowed(domain, uid) {
		t.Fatalf("refusing launchd target %q; require user/%s, never gui/<uid>", domain, strconv.Itoa(uid))
	}
	executable, err := os.Executable()
	if err != nil {
		t.Fatalf("resolve test helper executable: %v", err)
	}
	executable, err = filepath.Abs(executable)
	if err != nil {
		t.Fatalf("make test helper path absolute: %v", err)
	}
	root, err := os.MkdirTemp("", "sl-ld-")
	if err != nil {
		t.Fatalf("create isolated launchd state root: %v", err)
	}
	retainRoot := false
	t.Cleanup(func() {
		if retainRoot {
			t.Logf("retaining isolated launchd fixture data at %q because its service could not be confirmed stopped", root)
			return
		}
		if err := os.RemoveAll(root); err != nil {
			t.Errorf("remove isolated launchd fixture data: %v", err)
		}
	})
	root, err = filepath.EvalSymlinks(root)
	if err != nil {
		t.Fatalf("resolve isolated launchd state root: %v", err)
	}
	if err := os.Chmod(root, 0700); err != nil {
		t.Fatalf("protect isolated launchd state root: %v", err)
	}

	label := fmt.Sprintf("%s.test.%d", launchAgentLabel, time.Now().UnixNano())
	target := domain + "/" + label
	if err := exec.Command("launchctl", "print", target).Run(); err == nil {
		t.Fatalf("test label %q already exists; refusing to replace it", target)
	}
	countPath := filepath.Join(root, "starts")
	definition := launchAgentDefinition{
		Label:     label,
		Arguments: []string{executable, "-test.run=^TestLaunchdServiceHelper$"},
		Environment: map[string]string{
			launchdTestChildEnv: "1",
			launchdTestRootEnv:  root,
			launchdTestCountEnv: countPath,
		},
	}
	plist, err := renderLaunchAgent(definition)
	if err != nil {
		t.Fatalf("render isolated test plist: %v", err)
	}
	plistPath := filepath.Join(root, label+".plist")
	if err := os.WriteFile(plistPath, plist, 0600); err != nil {
		t.Fatalf("write isolated test plist: %v", err)
	}
	if output, err := exec.Command("plutil", "-lint", plistPath).CombinedOutput(); err != nil {
		t.Fatalf("plutil rejected test plist: %v: %s", err, strings.TrimSpace(string(output)))
	}
	if output, err := exec.Command("launchctl", "bootstrap", domain, plistPath).CombinedOutput(); err != nil {
		t.Fatalf("bootstrap isolated test service: %v: %s", err, strings.TrimSpace(string(output)))
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()
		if output, err := exec.CommandContext(ctx, "launchctl", "bootout", "--wait", target).CombinedOutput(); err != nil {
			retainRoot = true
			t.Errorf("boot out isolated test service; fixture data will be retained at %q: %v: %s", root, err, strings.TrimSpace(string(output)))
		}
	})

	waitForLaunchdStartCount(t, countPath, 1)
	if _, err := os.Stat(filepath.Join(root, "state", "service.sock")); err != nil {
		t.Fatalf("isolated service did not create its socket: %v", err)
	}
	if output, err := exec.Command("launchctl", "kill", "SIGKILL", target).CombinedOutput(); err != nil {
		t.Fatalf("crash isolated test service: %v: %s", err, strings.TrimSpace(string(output)))
	}
	waitForLaunchdStartCount(t, countPath, 2)
	if _, err := os.Stat(filepath.Join(root, "state", "service.sock")); err != nil {
		t.Fatalf("restarted service did not recreate its socket: %v", err)
	}
}

// TestLaunchdServiceHelper starts the actual lease server in a temporary state directory only when launchd starts it as the isolated fixture child.
func TestLaunchdServiceHelper(t *testing.T) {
	if os.Getenv(launchdTestChildEnv) != "1" {
		return
	}
	root := os.Getenv(launchdTestRootEnv)
	countPath := os.Getenv(launchdTestCountEnv)
	if root == "" || countPath == "" || !filepath.IsAbs(root) || !filepath.IsAbs(countPath) {
		t.Fatal("isolated launchd helper paths are missing or relative")
	}
	stateDir := filepath.Join(root, "state")
	socketPath := filepath.Join(stateDir, "service.sock")
	server, err := lease.Start(lease.Config{StateDir: stateDir, SocketPath: socketPath})
	if err != nil {
		t.Fatalf("start isolated lease service: %v", err)
	}
	defer server.Close()
	count, err := os.OpenFile(countPath, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0600)
	if err != nil {
		t.Fatalf("open launch count: %v", err)
	}
	if _, err := count.WriteString("started\n"); err != nil {
		_ = count.Close()
		t.Fatalf("record service start: %v", err)
	}
	if err := count.Close(); err != nil {
		t.Fatalf("close launch count: %v", err)
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if err := server.Serve(ctx); err != nil {
		t.Fatalf("serve isolated lease service: %v", err)
	}
}

func launchdTestDomainAllowed(domain string, uid int) bool {
	return uid > 0 && domain == fmt.Sprintf("user/%d", uid)
}

func waitForLaunchdStartCount(t *testing.T, path string, want int) {
	t.Helper()
	deadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) {
		data, err := os.ReadFile(path)
		if err == nil && strings.Count(string(data), "started\n") >= want {
			return
		}
		time.Sleep(100 * time.Millisecond)
	}
	data, _ := os.ReadFile(path)
	t.Fatalf("launchd starts = %d, want at least %d", strings.Count(string(data), "started\n"), want)
}
