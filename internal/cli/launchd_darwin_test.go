//go:build darwin

package cli

import (
	"context"
	"fmt"
	"io"
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

// TestLaunchdFixturePathGuardRejectsWritableFileAndParent proves private user-owned plists are accepted while writable files, unsafe parents, and non-executable programs are refused.
func TestLaunchdFixturePathGuardRejectsWritableFileAndParent(t *testing.T) {
	uid := uint32(os.Geteuid())
	root := makeSecureLaunchdTestRoot(t, uid)
	t.Cleanup(func() {
		if err := os.RemoveAll(root); err != nil {
			t.Errorf("remove launchd path guard fixture: %v", err)
		}
	})

	plistPath := filepath.Join(root, "private.plist")
	if err := os.WriteFile(plistPath, []byte("fixture"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := validateLaunchdFixtureFile(plistPath, uid, false); err != nil {
		t.Fatalf("private plist path was rejected: %v", err)
	}
	if err := validateLaunchdFixtureFile(plistPath, uid, true); err == nil || !strings.Contains(err.Error(), "no executable permission") {
		t.Fatalf("non-executable fixture program = %v, want missing executable permission refusal", err)
	}
	if err := os.Chmod(plistPath, 0660); err != nil {
		t.Fatal(err)
	}
	if err := validateLaunchdFixtureFile(plistPath, uid, false); err == nil || !strings.Contains(err.Error(), "group/world-writable file") {
		t.Fatalf("group-writable plist = %v, want a group/world-writable file refusal", err)
	}

	unsafeParent := filepath.Join(root, "writable-parent")
	if err := os.Mkdir(unsafeParent, 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(unsafeParent, 0770); err != nil {
		t.Fatal(err)
	}
	unsafePlist := filepath.Join(unsafeParent, "job.plist")
	if err := os.WriteFile(unsafePlist, []byte("fixture"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := validateLaunchdFixtureFile(unsafePlist, uid, false); err == nil || !strings.Contains(err.Error(), "group/world-writable directory") {
		t.Fatalf("writable plist parent = %v, want a group/world-writable directory refusal", err)
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
	executable, err = filepath.EvalSymlinks(executable)
	if err != nil {
		t.Fatalf("resolve test helper path: %v", err)
	}
	root := makeSecureLaunchdTestRoot(t, uint32(uid))
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
	label := fmt.Sprintf("%s.test.%d", launchAgentLabel, time.Now().UnixNano())
	target := domain + "/" + label
	fixtureExecutable := filepath.Join(root, "launchd-test-helper")
	if err := copyLaunchdExecutable(executable, fixtureExecutable); err != nil {
		t.Fatalf("copy isolated launchd helper: %v", err)
	}
	if err := validateLaunchdFixtureFile(fixtureExecutable, uint32(uid), true); err != nil {
		t.Fatalf("unsafe isolated launchd helper path: %v; %s", err, describeLaunchdPath(fixtureExecutable))
	}
	helperProbe := exec.Command(fixtureExecutable, "-test.run=^$")
	helperProbeOutput, helperProbeErr := helperProbe.CombinedOutput()
	if helperProbeErr != nil {
		t.Fatalf("isolated launchd helper is not executable: %v: %s; %s", helperProbeErr, strings.TrimSpace(string(helperProbeOutput)), describeLaunchdPath(fixtureExecutable))
	}
	t.Logf("preflight executable path=%q mode/owner/parents=%s execution=0 output=%q", fixtureExecutable, describeLaunchdPath(fixtureExecutable), strings.TrimSpace(string(helperProbeOutput)))

	countPath := filepath.Join(root, "starts")
	definition := launchAgentDefinition{
		Label:                     label,
		Arguments:                 []string{fixtureExecutable, "-test.run=^TestLaunchdServiceHelper$"},
		RequiresBackgroundSession: true,
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
	if err := os.Chmod(plistPath, 0600); err != nil {
		t.Fatalf("protect isolated test plist: %v", err)
	}
	if err := validateLaunchdFixtureFile(plistPath, uint32(uid), false); err != nil {
		t.Fatalf("unsafe isolated test plist path: %v; %s", err, describeLaunchdPath(plistPath))
	}
	t.Logf("preflight plist path=%q mode/owner/parents=%s", plistPath, describeLaunchdPath(plistPath))
	plistOutput, plistErr := exec.Command("plutil", "-lint", plistPath).CombinedOutput()
	if plistErr != nil {
		t.Fatalf("plutil rejected test plist: %v: %s", plistErr, strings.TrimSpace(string(plistOutput)))
	}
	t.Logf("preflight plutil exit=0 output=%q", strings.TrimSpace(string(plistOutput)))
	loaded, printOutput, printErr := launchdFixtureLabelLoaded(target)
	if printErr != nil {
		t.Fatalf("cannot determine whether test label %q is loaded: %v: %s", target, printErr, strings.TrimSpace(string(printOutput)))
	}
	if loaded {
		t.Fatalf("test label %q is already loaded; refusing to replace it: %s", target, strings.TrimSpace(string(printOutput)))
	}
	t.Logf("preflight label target=%q loaded=false output=%q", target, strings.TrimSpace(string(printOutput)))

	bootstrapAttempted := false
	t.Cleanup(func() {
		if !bootstrapAttempted {
			return
		}
		loaded, output, err := launchdFixtureLabelLoaded(target)
		if err != nil {
			retainRoot = true
			t.Errorf("cannot verify isolated test label state; retaining fixture data at %q: %v: %s", root, err, strings.TrimSpace(string(output)))
			return
		}
		if !loaded {
			return
		}
		ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()
		if output, err := exec.CommandContext(ctx, "launchctl", "bootout", "--wait", target).CombinedOutput(); err != nil {
			retainRoot = true
			t.Errorf("boot out isolated test service; fixture data will be retained at %q: %v: %s", root, err, strings.TrimSpace(string(output)))
			return
		}
		stillLoaded, output, err := launchdFixtureLabelLoaded(target)
		if err != nil || stillLoaded {
			retainRoot = true
			t.Errorf("isolated test label is not confirmed stopped; fixture data will be retained at %q: loaded=%t err=%v output=%s", root, stillLoaded, err, strings.TrimSpace(string(output)))
		}
	})
	bootstrapAttempted = true
	if output, err := exec.Command("launchctl", "bootstrap", domain, plistPath).CombinedOutput(); err != nil {
		t.Fatalf("bootstrap isolated test service target=%q plist=%q program=%q: %v: %s", target, plistPath, fixtureExecutable, err, strings.TrimSpace(string(output)))
	}

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

func makeSecureLaunchdTestRoot(t *testing.T, uid uint32) string {
	t.Helper()
	home, err := lease.DefaultHomeDir()
	if err != nil {
		t.Fatalf("resolve system-account home for launchd fixture: %v", err)
	}
	home, err = filepath.EvalSymlinks(home)
	if err != nil {
		t.Fatalf("resolve launchd fixture home path: %v", err)
	}
	homeInfo, err := os.Stat(home)
	if err != nil {
		t.Fatalf("inspect launchd fixture home: %v", err)
	}
	homeStat, ok := homeInfo.Sys().(*syscall.Stat_t)
	if !ok || !homeInfo.IsDir() || homeStat.Uid != uid {
		t.Fatalf("launchd fixture home is not a directory owned by uid %d: %s", uid, describeLaunchdPath(home))
	}
	if err := checkLaunchdDirectoryChain(home, uid); err != nil {
		t.Fatalf("unsafe launchd fixture home path: %v; %s", err, describeLaunchdPath(home))
	}
	root, err := os.MkdirTemp(home, ".sl-ld-")
	if err != nil {
		t.Fatalf("create isolated launchd state root under account home: %v", err)
	}
	ready := false
	defer func() {
		if !ready {
			if err := os.RemoveAll(root); err != nil {
				t.Errorf("remove incomplete launchd fixture root: %v", err)
			}
		}
	}()
	if err := os.Chmod(root, 0700); err != nil {
		t.Fatalf("protect isolated launchd state root: %v", err)
	}
	if err := checkLaunchdDirectoryChain(root, uid); err != nil {
		t.Fatalf("unsafe isolated launchd state root: %v; %s", err, describeLaunchdPath(root))
	}
	ready = true
	return root
}

func copyLaunchdExecutable(source, destination string) error {
	input, err := os.Open(source)
	if err != nil {
		return err
	}
	defer input.Close()
	output, err := os.OpenFile(destination, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0700)
	if err != nil {
		return err
	}
	if _, err := io.Copy(output, input); err != nil {
		_ = output.Close()
		return err
	}
	if err := output.Sync(); err != nil {
		_ = output.Close()
		return err
	}
	if err := output.Close(); err != nil {
		return err
	}
	return os.Chmod(destination, 0700)
}

func checkLaunchdDirectoryChain(path string, uid uint32) error {
	absolute, err := filepath.Abs(path)
	if err != nil {
		return err
	}
	for current := filepath.Clean(absolute); ; current = filepath.Dir(current) {
		info, err := os.Lstat(current)
		if err != nil {
			return fmt.Errorf("inspect parent %q: %w", current, err)
		}
		if info.Mode()&os.ModeSymlink != 0 || !info.IsDir() {
			return fmt.Errorf("launchd path parent %q is not a real directory", current)
		}
		if info.Mode().Perm()&0022 != 0 {
			return fmt.Errorf("launchd path parent %q is a group/world-writable directory: mode %04o", current, info.Mode().Perm())
		}
		stat, ok := info.Sys().(*syscall.Stat_t)
		if !ok || (stat.Uid != 0 && stat.Uid != uid) {
			return fmt.Errorf("launchd path parent %q is not owned by root or uid %d", current, uid)
		}
		parent := filepath.Dir(current)
		if parent == current {
			return nil
		}
	}
}

func validateLaunchdFixtureFile(path string, uid uint32, executable bool) error {
	info, err := os.Lstat(path)
	if err != nil {
		return fmt.Errorf("inspect launchd fixture file %q: %w", path, err)
	}
	if info.Mode()&os.ModeSymlink != 0 || !info.Mode().IsRegular() {
		return fmt.Errorf("launchd fixture file %q is not a regular file", path)
	}
	if info.Mode().Perm()&0022 != 0 {
		return fmt.Errorf("launchd fixture path %q is a group/world-writable file: mode %04o", path, info.Mode().Perm())
	}
	if executable && info.Mode().Perm()&0111 == 0 {
		return fmt.Errorf("launchd fixture program %q has no executable permission", path)
	}
	stat, ok := info.Sys().(*syscall.Stat_t)
	if !ok || stat.Uid != uid {
		return fmt.Errorf("launchd fixture file %q is not owned by uid %d", path, uid)
	}
	return checkLaunchdDirectoryChain(filepath.Dir(path), uid)
}

func describeLaunchdPath(path string) string {
	abs, err := filepath.Abs(path)
	if err != nil {
		return fmt.Sprintf("path=%q abs-error=%v", path, err)
	}
	parts := make([]string, 0, 8)
	for current := filepath.Clean(abs); ; current = filepath.Dir(current) {
		info, err := os.Lstat(current)
		if err != nil {
			parts = append(parts, fmt.Sprintf("%s(stat-error=%v)", filepath.Base(current), err))
		} else {
			owner := "unknown"
			if stat, ok := info.Sys().(*syscall.Stat_t); ok {
				owner = strconv.FormatUint(uint64(stat.Uid), 10)
			}
			parts = append(parts, fmt.Sprintf("%s(mode=%04o,uid=%s)", filepath.Base(current), info.Mode().Perm(), owner))
		}
		parent := filepath.Dir(current)
		if parent == current {
			break
		}
	}
	return strings.Join(parts, " <- ")
}

func launchdFixtureLabelLoaded(target string) (bool, []byte, error) {
	output, err := exec.Command("launchctl", "print", target).CombinedOutput()
	if err == nil {
		return true, output, nil
	}
	if strings.Contains(string(output), "Could not find service") {
		return false, output, nil
	}
	return false, output, fmt.Errorf("launchctl print failed: %w", err)
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
