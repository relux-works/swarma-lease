package cli

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/relux-works/swarma-lease/internal/lease"
)

type result struct {
	State     string         `json:"state"`
	Code      string         `json:"code"`
	RequestID string         `json:"request_id"`
	Data      map[string]any `json:"data"`
}

func startFixture(t *testing.T) (*lease.Server, string) {
	t.Helper()
	createdRoot, err := os.MkdirTemp("", "sl-")
	if err != nil {
		t.Fatalf("create short test directory: %v", err)
	}
	root, err := filepath.EvalSymlinks(createdRoot)
	if err != nil {
		_ = os.RemoveAll(createdRoot)
		t.Fatalf("resolve short test directory")
	}
	t.Cleanup(func() {
		if err := os.RemoveAll(root); err != nil {
			t.Errorf("remove test directory: %v", err)
		}
	})
	server, err := lease.Start(lease.Config{
		StateDir:   filepath.Join(root, "state"),
		SocketPath: filepath.Join(root, "service.sock"),
	})
	if err != nil {
		t.Fatalf("start local service: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(func() {
		cancel()
		if err := server.Close(); err != nil {
			t.Errorf("close local service: %v", err)
		}
	})
	go func() {
		if err := server.Serve(ctx); err != nil {
			t.Errorf("serve local socket: %v", err)
		}
	}()
	return server, filepath.Join(root, "service.sock")
}

func runJSON(t *testing.T, socket string, args ...string) (int, result) {
	t.Helper()
	var stdout, stderr bytes.Buffer
	exit := RunAtSocket(args, &stdout, &stderr, socket)
	if stderr.Len() != 0 {
		t.Fatalf("management command wrote to stderr: %q", stderr.String())
	}
	var got result
	if err := json.Unmarshal(stdout.Bytes(), &got); err != nil {
		t.Fatalf("decode one JSON result %q: %v", stdout.String(), err)
	}
	return exit, got
}

func assertServiceSnapshot(t *testing.T, got result, wantActive, wantQueued float64, resourceID, wantState string, wantToken float64) {
	t.Helper()
	if got.Code != "" {
		t.Fatalf("service status code = %s, want success", got.Code)
	}
	counts, ok := got.Data["counts"].(map[string]any)
	if !ok || counts["active_leases"] != wantActive || counts["queued_requests"] != wantQueued {
		t.Fatalf("service counts = %#v, want active=%v queued=%v", got.Data["counts"], wantActive, wantQueued)
	}
	pools, ok := got.Data["pools"].([]any)
	if !ok {
		t.Fatalf("service pools = %#v, want resource %s", got.Data["pools"], resourceID)
	}
	for _, poolValue := range pools {
		pool, ok := poolValue.(map[string]any)
		if !ok {
			continue
		}
		resources, ok := pool["resources"].([]any)
		if !ok {
			continue
		}
		for _, resourceValue := range resources {
			resource, ok := resourceValue.(map[string]any)
			if ok && resource["id"] == resourceID {
				if resource["state"] != wantState || resource["fencing_token"] != wantToken {
					t.Fatalf("resource snapshot = %#v, want state=%s fencing_token=%v", resource, wantState, wantToken)
				}
				return
			}
		}
	}
	t.Fatalf("service status has no resource %s", resourceID)
}

// TestRequestReleaseUsesRealService proves the CLI registers a resource, obtains one durable SQLite-backed lease through the Unix socket, replays the same request, and releases the same lease idempotently.
func TestRequestReleaseUsesRealService(t *testing.T) {
	_, socket := startFixture(t)

	if code, got := runJSON(t, socket, "resource", "add", "--tag", "qa.pool", "--id", "qa-01", "--mode", "manual", "--json"); code != 0 || got.State != "added" {
		t.Fatalf("resource add = (%d, %#v), want success", code, got)
	}
	args := []string{"lease", "request", "--tag", "qa.pool", "--duration", "30s", "--no-wait", "--request-id", "req-01", "--json"}
	code, first := runJSON(t, socket, args...)
	if code != 0 || first.State != "granted" || first.RequestID != "req-01" {
		t.Fatalf("first acquire = (%d, %#v), want committed grant", code, first)
	}
	leaseRecord, ok := first.Data["lease"].(map[string]any)
	if !ok || leaseRecord["resource_id"] != "qa-01" || leaseRecord["fencing_token"] != float64(1) {
		t.Fatalf("grant lease = %#v, want qa-01 with fencing token 1", first.Data["lease"])
	}

	code, repeated := runJSON(t, socket, args...)
	if code != 0 || repeated.State != "granted" || repeated.Data["lease"] == nil || repeated.Data["lease"].(map[string]any)["id"] != leaseRecord["id"] {
		t.Fatalf("identical request replay = (%d, %#v), want the same lease", code, repeated)
	}

	leaseID := leaseRecord["id"].(string)
	code, released := runJSON(t, socket, "lease", "release", "--id", leaseID, "--json")
	if code != 0 || released.State != "released" {
		t.Fatalf("release = (%d, %#v), want released", code, released)
	}
	code, repeatedRelease := runJSON(t, socket, "lease", "release", "--id", leaseID, "--json")
	if code != 0 || repeatedRelease.State != "released" || repeatedRelease.Data["lease"].(map[string]any)["id"] != leaseID {
		t.Fatalf("repeated release = (%d, %#v), want the same terminal lease", code, repeatedRelease)
	}

	if code, neighbor := runJSON(t, socket, "resource", "add", "--tag", "qa.other", "--id", "qa-02", "--mode", "manual", "--json"); code != 0 || neighbor.State != "added" {
		t.Fatalf("neighbor add = (%d, %#v), want success", code, neighbor)
	}
	neighborCode, neighbor := runJSON(t, socket, "lease", "request", "--tag", "qa.other", "--duration", "10s", "--no-wait", "--request-id", "req-02", "--json")
	if neighborCode != 0 || neighbor.State != "granted" {
		t.Fatalf("neighbor grant = (%d, %#v), want success", neighborCode, neighbor)
	}
	neighborLease, ok := neighbor.Data["lease"].(map[string]any)
	if !ok || neighborLease["resource_id"] != "qa-02" {
		t.Fatalf("neighbor lease = %#v, want resource qa-02", neighbor.Data["lease"])
	}
}

// TestAcquisitionRefusalsKeepTheirState proves malformed tag/duration input stores no request, valid policy refusals do not grant or queue, and no-wait exhaustion preserves the active lease.
func TestAcquisitionRefusalsKeepTheirState(t *testing.T) {
	_, socket := startFixture(t)
	if code, got := runJSON(t, socket, "resource", "add", "--tag", "qa.pool", "--id", "qa-01", "--mode", "manual", "--json"); code != 0 || got.State != "added" {
		t.Fatalf("resource add = (%d, %#v), want success", code, got)
	}

	if code, got := runJSON(t, socket, "lease", "request", "--tag", "Bad Tag", "--duration", "10s", "--no-wait", "--request-id", "bad-tag", "--json"); code != 2 || got.Code != "invalid_tag" {
		t.Fatalf("malformed tag = (%d, %#v), want invalid_tag", code, got)
	}
	if code, got := runJSON(t, socket, "lease", "request", "--tag", "qa.pool", "--duration", "0s", "--no-wait", "--request-id", "bad-duration", "--json"); code != 2 || got.Code != "invalid_duration" {
		t.Fatalf("nonpositive duration = (%d, %#v), want invalid_duration", code, got)
	}
	if code, got := runJSON(t, socket, "lease", "request", "--tag", "qa.pool", "--duration", "21m", "--no-wait", "--request-id", "long-duration", "--json"); code != 5 || got.Code != "duration_exceeds_max" {
		t.Fatalf("over-limit duration = (%d, %#v), want duration_exceeds_max", code, got)
	}
	if code, got := runJSON(t, socket, "lease", "request", "--tag", "qa.missing", "--duration", "10s", "--no-wait", "--request-id", "unknown-tag", "--json"); code != 3 || got.Code != "unknown_tag" {
		t.Fatalf("unknown tag = (%d, %#v), want unknown_tag", code, got)
	}
	for _, requestID := range []string{"bad-tag", "bad-duration"} {
		if code, got := runJSON(t, socket, "lease", "status", "--request-id", requestID, "--json"); code != 3 || got.Code != "unknown_request" {
			t.Fatalf("status for malformed request %s = (%d, %#v), want no persisted request", requestID, code, got)
		}
	}
	_, afterInitialRefusals := runJSON(t, socket, "service", "status", "--json")
	assertServiceSnapshot(t, afterInitialRefusals, 0, 0, "qa-01", "available", 0)

	controlCode, control := runJSON(t, socket, "lease", "request", "--tag", "qa.pool", "--duration", "10s", "--no-wait", "--request-id", "req-active", "--json")
	if controlCode != 0 || control.State != "granted" {
		t.Fatalf("control grant = (%d, %#v), want success", controlCode, control)
	}
	controlLease, ok := control.Data["lease"].(map[string]any)
	if !ok {
		t.Fatalf("control grant lease = %#v, want a lease record", control.Data["lease"])
	}
	busyCode, busy := runJSON(t, socket, "lease", "request", "--tag", "qa.pool", "--duration", "10s", "--no-wait", "--request-id", "req-active-2", "--json")
	if busyCode != 75 || busy.Code != "no_wait_busy" {
		t.Fatalf("exhausted no-wait request = (%d, %#v), want no_wait_busy", busyCode, busy)
	}
	activeCode, activeStatus := runJSON(t, socket, "lease", "status", "--request-id", "req-active", "--json")
	if activeCode != 0 || activeStatus.State != "leased" {
		t.Fatalf("active request status = (%d, %#v), want original lease", activeCode, activeStatus)
	}
	activeLease, ok := activeStatus.Data["lease"].(map[string]any)
	if !ok || activeLease["id"] != controlLease["id"] {
		t.Fatalf("active lease after no-wait refusal = %#v, want original lease %v", activeStatus.Data["lease"], controlLease["id"])
	}
	if code, got := runJSON(t, socket, "lease", "status", "--request-id", "req-active-2", "--json"); code != 0 || got.State != "refused" {
		t.Fatalf("busy request status = (%d, %#v), want stored refusal", code, got)
	}
	_, afterBusyRefusal := runJSON(t, socket, "service", "status", "--json")
	assertServiceSnapshot(t, afterBusyRefusal, 1, 0, "qa-01", "leased", 1)
}

// TestRequestIDConflictDoesNotReplaceTheGrant proves a changed canonical input with an existing key refuses and leaves the original lease intact.
func TestRequestIDConflictDoesNotReplaceTheGrant(t *testing.T) {
	_, socket := startFixture(t)
	if code, got := runJSON(t, socket, "resource", "add", "--tag", "qa.pool", "--id", "qa-01", "--mode", "manual", "--json"); code != 0 || got.State != "added" {
		t.Fatalf("resource add = (%d, %#v), want success", code, got)
	}
	if code, got := runJSON(t, socket, "lease", "request", "--tag", "qa.pool", "--duration", "10s", "--no-wait", "--request-id", "same-key", "--json"); code != 0 || got.State != "granted" {
		t.Fatalf("control grant = (%d, %#v), want success", code, got)
	}
	_, original := runJSON(t, socket, "lease", "status", "--request-id", "same-key", "--json")
	originalLease, ok := original.Data["lease"].(map[string]any)
	if !ok {
		t.Fatalf("original lease = %#v, want a lease record", original.Data["lease"])
	}
	if code, got := runJSON(t, socket, "lease", "request", "--tag", "qa.pool", "--duration", "11s", "--no-wait", "--request-id", "same-key", "--json"); code != 5 || got.Code != "request_id_conflict" {
		t.Fatalf("changed request = (%d, %#v), want request_id_conflict", code, got)
	}
	statusCode, statusByRequest := runJSON(t, socket, "lease", "status", "--request-id", "same-key", "--json")
	leaseAfterConflict, leaseOK := statusByRequest.Data["lease"].(map[string]any)
	if statusCode != 0 || statusByRequest.State != "leased" || !leaseOK || leaseAfterConflict["id"] != originalLease["id"] {
		t.Fatalf("original request after conflict = (%d, %#v), want same active lease", statusCode, statusByRequest)
	}
	_, status := runJSON(t, socket, "service", "status", "--json")
	assertServiceSnapshot(t, status, 1, 0, "qa-01", "leased", 1)
}

// TestInvalidCLIInputsDoNotReserveState proves malformed resource and request IDs are rejected before connecting, while a neighboring canonical resource addition commits normally.
func TestInvalidCLIInputsDoNotReserveState(t *testing.T) {
	_, socket := startFixture(t)
	if code, got := runJSON(t, socket, "resource", "add", "--tag", "qa.pool", "--id", "../bad", "--mode", "manual", "--json"); code != 2 || got.Code != "invalid_resource_id" {
		t.Fatalf("invalid resource ID = (%d, %#v), want invalid_resource_id", code, got)
	}
	if code, got := runJSON(t, socket, "lease", "request", "--tag", "qa.pool", "--duration", "10s", "--no-wait", "--request-id", "bad id", "--json"); code != 2 || got.Code != "invalid_request_id" || got.RequestID != "" {
		t.Fatalf("invalid request ID = (%d, %#v), want unreflected invalid_request_id", code, got)
	}
	if code, got := runJSON(t, socket, "resource", "add", "--tag", "qa.pool", "--id", "qa-01", "--mode", "manual", "--json"); code != 0 || got.State != "added" {
		t.Fatalf("canonical resource control = (%d, %#v), want added", code, got)
	}
	if code, got := runJSON(t, socket, "service", "status", "--json"); code != 0 || got.Data["counts"].(map[string]any)["audit_rows"] != float64(1) {
		t.Fatalf("status after invalid inputs = (%d, %#v), want only the valid addition audited", code, got)
	}
}

// TestDefaultCommandPoolsRemainClosed proves a fresh database has exactly the two command-mode defaults and raw acquisition cannot lease either without an adapter.
func TestDefaultCommandPoolsRemainClosed(t *testing.T) {
	_, socket := startFixture(t)
	code, before := runJSON(t, socket, "service", "status", "--json")
	if code != 0 || before.Data["max_lease_duration_ns"] != float64(20*60*1_000_000_000) || before.Data["max_wait_timeout_ns"] != float64(30*60*1_000_000_000) {
		t.Fatalf("initial service status = (%d, %#v), want fixed default limits", code, before)
	}
	pools, ok := before.Data["pools"].([]any)
	if !ok || len(pools) != 2 {
		t.Fatalf("initial pools = %#v, want two command pools", before.Data["pools"])
	}
	for _, value := range pools {
		pool, ok := value.(map[string]any)
		if !ok || pool["mode"] != "command" {
			t.Fatalf("default pool = %#v, want command mode", value)
		}
		resources, ok := pool["resources"].([]any)
		if !ok || len(resources) != 1 {
			t.Fatalf("default resources = %#v, want one resource", pool["resources"])
		}
	}
	code, refused := runJSON(t, socket, "lease", "request", "--tag", "mac.go-test", "--duration", "5s", "--no-wait", "--request-id", "raw-default", "--json")
	if code != 5 || refused.Code != "adapter_required" {
		t.Fatalf("raw default-pool request = (%d, %#v), want adapter_required", code, refused)
	}
	code, after := runJSON(t, socket, "service", "status", "--json")
	counts, countsOK := after.Data["counts"].(map[string]any)
	if code != 0 || !countsOK || counts["active_leases"] != float64(0) || counts["terminal_requests"] != float64(1) || counts["audit_rows"] != float64(1) {
		t.Fatalf("status after refused raw request = (%d, %#v), want one durable refusal and no lease", code, after)
	}
}

// TestDurationGrammarRejectsLookalikes proves duration parsing accepts only positive integer ASCII quantities with the six contract units and rejects signs, fractions, whitespace, unicode micro symbols, and overflow.
func TestDurationGrammarRejectsLookalikes(t *testing.T) {
	valid := map[string]int64{"1ns": 1, "2us": 2_000, "3ms": 3_000_000, "4s": 4_000_000_000, "5m": 300_000_000_000, "1h": 3_600_000_000_000}
	for input, want := range valid {
		got, err := ParseDuration(input)
		if err != nil || got != want {
			t.Errorf("ParseDuration(%q) = (%d, %v), want (%d, nil)", input, got, err, want)
		}
	}
	for _, input := range []string{"0s", "-1s", "+1s", "1.5s", " 1s", "1µs", "9223372036854775808ns"} {
		if _, err := ParseDuration(input); err == nil {
			t.Errorf("ParseDuration(%q) succeeded, want refusal", input)
		}
	}
}

// TestSocketPathTooLongHasStableCLIResult proves the closed startup path error maps to exit 69 without exposing the rejected path.
func TestSocketPathTooLongHasStableCLIResult(t *testing.T) {
	var stdout bytes.Buffer
	if code := writeCLIError(&stdout, "cli", "", "socket_path_too_long"); code != 69 {
		t.Fatalf("socket_path_too_long exit = %d, want 69", code)
	}
	var got result
	if err := json.Unmarshal(stdout.Bytes(), &got); err != nil {
		t.Fatalf("decode CLI error: %v", err)
	}
	if got.State != "error" || got.Code != "socket_path_too_long" || got.RequestID != "" {
		t.Fatalf("socket path error result = %#v, want stable code without request ID", got)
	}
}

// TestWaitAndCancelCLICommandsCarryOnlyTheRequestKey proves the blocking observer and cancellation commands use the existing idempotency key without accepting replacement acquisition input.
func TestWaitAndCancelCLICommandsCarryOnlyTheRequestKey(t *testing.T) {
	for _, command := range []string{"wait", "cancel"} {
		request, err := buildRequest([]string{"lease", command, "--request-id", "req-01", "--json"})
		if err != nil || request.Op != "lease."+command || request.RequestID != "req-01" || len(request.Data.(map[string]any)) != 0 {
			t.Fatalf("lease %s request = (%#v, %v), want only key req-01", command, request, err)
		}
		if _, err := buildRequest([]string{"lease", command, "--request-id", "bad key", "--json"}); err == nil || err.code != "invalid_request_id" {
			t.Fatalf("lease %s malformed key error = %v, want invalid_request_id", command, err)
		}
	}
	if got := exitStatus("client_interrupted"); got != 130 {
		t.Fatalf("client_interrupted exit = %d, want 130", got)
	}
}

// TestHistoryCLIFormsReadTheExpectedAuditWindow proves the documented default, limit, and cursor forms reach the service and return the requested audit entries.
func TestHistoryCLIFormsReadTheExpectedAuditWindow(t *testing.T) {
	_, socket := startFixture(t)
	if code, got := runJSON(t, socket, "resource", "add", "--tag", "qa.history", "--id", "history-01", "--mode", "manual", "--json"); code != 0 || got.State != "added" {
		t.Fatalf("resource-add control = (%d,%#v)", code, got)
	}
	if code, got := runJSON(t, socket, "lease", "request", "--tag", "qa.history", "--duration", "10s", "--no-wait", "--request-id", "history-lease", "--json"); code != 0 || got.State != "granted" {
		t.Fatalf("lease control = (%d,%#v)", code, got)
	}
	for _, tc := range []struct {
		name string
		args []string
		want int
	}{
		{"default", []string{"history", "--json"}, 4},
		{"limit", []string{"history", "--limit", "1", "--json"}, 1},
		{"cursor", []string{"history", "--limit", "1", "--before-sequence", "2", "--json"}, 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			code, got := runJSON(t, socket, tc.args...)
			entries, ok := got.Data["entries"].([]any)
			if code != 0 || got.State != "ok" || !ok || len(entries) != tc.want {
				t.Fatalf("history %v = (%d,%#v), want %d entries", tc.args, code, got, tc.want)
			}
			if tc.name == "limit" && entries[0].(map[string]any)["sequence"] != float64(4) {
				t.Fatalf("limited history = %#v, want newest sequence four", entries)
			}
			if tc.name == "cursor" && entries[0].(map[string]any)["sequence"] != float64(1) {
				t.Fatalf("cursor history = %#v, want sequence below two", entries)
			}
		})
	}
}

// TestAutoKeyedCLIInterruptionReturnsItsDurableKey proves the real CLI builds an auto-keyed wait request before transport and reports the generated key with unknown attempt state after the peer has acknowledged queueing.
func TestAutoKeyedCLIInterruptionReturnsItsDurableKey(t *testing.T) {
	root, err := os.MkdirTemp("", "sl-cli-interrupt-")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(root)
	path := filepath.Join(root, "interrupt.sock")
	listener, err := net.ListenUnix("unix", &net.UnixAddr{Name: path, Net: "unix"})
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	requestCh := make(chan lease.WireRequest, 1)
	queuedSent := make(chan struct{})
	peerDone := make(chan error, 1)
	go func() {
		conn, err := listener.AcceptUnix()
		if err != nil {
			peerDone <- err
			return
		}
		defer conn.Close()
		_ = conn.SetDeadline(time.Now().Add(3 * time.Second))
		line, err := bufio.NewReader(conn).ReadString('\n')
		if err != nil {
			peerDone <- err
			return
		}
		var request lease.WireRequest
		if err := json.Unmarshal([]byte(line), &request); err != nil {
			peerDone <- err
			return
		}
		requestCh <- request
		queued, err := json.Marshal(lease.Response{Version: 1, Type: "response", Op: request.Op, RequestID: request.RequestID, State: "queued", Data: map[string]any{"request": map[string]any{"request_id": request.RequestID}, "lease": nil, "resource": nil}})
		if err == nil {
			_, err = conn.Write(append(queued, '\n'))
		}
		close(queuedSent)
		if err != nil {
			peerDone <- err
			return
		}
		_ = conn.SetReadDeadline(time.Now().Add(2 * time.Second))
		if _, err := bufio.NewReader(conn).ReadString('\n'); err == nil {
			peerDone <- os.ErrInvalid
			return
		}
		peerDone <- nil
	}()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var stdout, stderr bytes.Buffer
	finished := make(chan int, 1)
	go func() {
		finished <- runAtSocketContext(ctx, []string{"lease", "request", "--tag", "qa.pool", "--duration", "10s", "--wait", "--wait-timeout", "30s", "--json"}, &stdout, &stderr, path)
	}()
	<-queuedSent
	request := <-requestCh
	cancel()
	select {
	case code := <-finished:
		var got result
		if err := json.Unmarshal(stdout.Bytes(), &got); err != nil {
			t.Fatalf("decode interrupted CLI result %q: %v", stdout.String(), err)
		}
		if code != 130 || got.Code != "client_interrupted" || got.RequestID == "" || got.RequestID != request.RequestID || len(got.RequestID) != 32 || strings.Trim(got.RequestID, "0123456789abcdef") != "" || got.Data["attempt_state"] != "unknown" || strings.Contains(stdout.String()+stderr.String(), "qa.pool") || strings.Contains(stdout.String()+stderr.String(), "30s") {
			t.Fatalf("interrupted auto-key CLI = (%d,%#v,stderr=%q), want generated key and unknown attempt", code, got, stderr.String())
		}
	case <-time.After(3 * time.Second):
		t.Fatal("CLI did not return after cancellation")
	}
	if err := <-peerDone; err != nil {
		t.Fatalf("client sent another frame after queued interruption: %v", err)
	}
}

// TestRejectedCLIInputDoesNotEchoPrivateValue proves a malformed private-looking argument is refused locally and absent from stdout and stderr.
func TestRejectedCLIInputDoesNotEchoPrivateValue(t *testing.T) {
	const marker = "private/rejected/value"
	var stdout, stderr bytes.Buffer
	exit := RunAtSocket([]string{"lease", "request", "--tag", marker, "--duration", "10s", "--no-wait", "--json"}, &stdout, &stderr, filepath.Join(t.TempDir(), "absent.sock"))
	var got result
	if err := json.Unmarshal(stdout.Bytes(), &got); err != nil {
		t.Fatal(err)
	}
	if exit != 2 || got.Code != "invalid_tag" || strings.Contains(stdout.String()+stderr.String(), marker) || stderr.Len() != 0 {
		t.Fatalf("private-looking CLI refusal = exit %d stdout %q stderr %q", exit, stdout.String(), stderr.String())
	}
}

// TestStableRefusalExitCodeTable proves every documented service and CLI refusal code maps to its specified process exit status.
func TestStableRefusalExitCodeTable(t *testing.T) {
	cases := []struct {
		code string
		want int
	}{
		{"invalid_json", 2}, {"frame_too_large", 2}, {"unsupported_version", 2}, {"unknown_operation", 2}, {"invalid_request", 2},
		{"invalid_tag", 2}, {"invalid_resource_id", 2}, {"invalid_lease_id", 2}, {"invalid_request_id", 2}, {"invalid_duration", 2},
		{"invalid_wait_mode", 2}, {"invalid_wait_timeout", 2}, {"invalid_pool_mode", 2},
		{"unknown_tag", 3}, {"unknown_resource", 3}, {"unknown_lease", 3}, {"unknown_request", 3},
		{"request_timed_out", 4}, {"request_cancelled", 4}, {"lease_released", 4}, {"lease_expired", 4},
		{"duration_exceeds_max", 5}, {"wait_exceeds_max", 5}, {"adapter_required", 5}, {"pool_disabled", 5},
		{"queue_sequence_exhausted", 5}, {"request_id_conflict", 5}, {"duplicate_resource_id", 5}, {"pool_mode_mismatch", 5},
		{"invalid_transition", 5}, {"request_not_cancellable", 5}, {"no_wait_busy", 75}, {"client_interrupted", 130},
		{"daemon_unavailable", 69}, {"clock_unavailable", 69}, {"result_unknown", 69}, {"storage_unavailable", 69},
		{"state_corrupt", 69}, {"state_incompatible", 69}, {"permissions_invalid", 69}, {"unsafe_state_path", 69},
		{"socket_path_too_long", 69}, {"instance_running", 69}, {"socket_stale_uncertain", 69},
	}
	for _, tc := range cases {
		t.Run(tc.code, func(t *testing.T) {
			if got := exitStatus(tc.code); got != tc.want {
				t.Fatalf("exit status for %s = %d, want %d", tc.code, got, tc.want)
			}
		})
	}
}
