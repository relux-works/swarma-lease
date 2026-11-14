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
	"github.com/relux-works/swarma-lease/internal/sqlite"
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

func assertStoredRequestRefusal(t *testing.T, socket string, code int, got result, wantExit int, wantCode, wantID string) {
	t.Helper()
	if code != wantExit || got.State != "error" || got.Code != wantCode || got.RequestID != wantID || len(got.Data) != 3 {
		t.Fatalf("stored refusal = (%d,%#v), want exit=%d code=%s key=%q and request-result data", code, got, wantExit, wantCode, wantID)
	}
	request, ok := got.Data["request"].(map[string]any)
	if !ok || request["request_id"] != wantID || request["state"] != "refused" || request["outcome_code"] != wantCode || got.Data["lease"] != nil || got.Data["resource"] != nil {
		t.Fatalf("stored refusal data = %#v, want refused request and null lease/resource", got.Data)
	}
	statusCode, status := runJSON(t, socket, "lease", "status", "--request-id", wantID, "--json")
	statusRequest, statusOK := status.Data["request"].(map[string]any)
	if statusCode != 0 || status.State != "refused" || status.Code != "" || status.RequestID != wantID || len(status.Data) != 3 || !statusOK || statusRequest["request_id"] != wantID || statusRequest["state"] != "refused" || statusRequest["outcome_code"] != wantCode || status.Data["lease"] != nil || status.Data["resource"] != nil {
		t.Fatalf("stored refusal status = (%d,%#v), want durable refused request %q with outcome %s", statusCode, status, wantID, wantCode)
	}
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

	if code, got := runJSON(t, socket, "lease", "request", "--tag", "Bad Tag", "--duration", "10s", "--no-wait", "--request-id", "bad-tag", "--json"); code != 2 || got.State != "error" || got.Code != "invalid_tag" || got.RequestID != "" || len(got.Data) != 0 {
		t.Fatalf("malformed tag = (%d, %#v), want invalid_tag", code, got)
	}
	if code, got := runJSON(t, socket, "lease", "request", "--tag", "qa.pool", "--duration", "0s", "--no-wait", "--request-id", "bad-duration", "--json"); code != 2 || got.State != "error" || got.Code != "invalid_duration" || got.RequestID != "" || len(got.Data) != 0 {
		t.Fatalf("nonpositive duration = (%d, %#v), want invalid_duration", code, got)
	}
	durationCode, durationRefusal := runJSON(t, socket, "lease", "request", "--tag", "qa.pool", "--duration", "21m", "--no-wait", "--request-id", "long-duration", "--json")
	assertStoredRequestRefusal(t, socket, durationCode, durationRefusal, 5, "duration_exceeds_max", "long-duration")
	unknownTagCode, unknownTagRefusal := runJSON(t, socket, "lease", "request", "--tag", "qa.missing", "--duration", "10s", "--no-wait", "--request-id", "unknown-tag", "--json")
	assertStoredRequestRefusal(t, socket, unknownTagCode, unknownTagRefusal, 3, "unknown_tag", "unknown-tag")
	for _, requestID := range []string{"bad-tag", "bad-duration"} {
		if code, got := runJSON(t, socket, "lease", "status", "--request-id", requestID, "--json"); code != 3 || got.State != "error" || got.Code != "unknown_request" || got.RequestID != requestID || len(got.Data) != 0 {
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
	assertStoredRequestRefusal(t, socket, busyCode, busy, 75, "no_wait_busy", "req-active-2")
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
	_, beforeConflict := runJSON(t, socket, "service", "status", "--json")
	beforeCounts := beforeConflict.Data["counts"].(map[string]any)
	conflictCode, conflict := runJSON(t, socket, "lease", "request", "--tag", "qa.pool", "--duration", "11s", "--no-wait", "--request-id", "same-key", "--json")
	if conflictCode != 5 || conflict.State != "error" || conflict.Code != "request_id_conflict" || conflict.RequestID != "same-key" || len(conflict.Data) != 3 {
		t.Fatalf("changed request = (%d,%#v), want request-result conflict for same-key", conflictCode, conflict)
	}
	request, requestOK := conflict.Data["request"].(map[string]any)
	leaseRecord, leaseRecordOK := conflict.Data["lease"].(map[string]any)
	resource, resourceOK := conflict.Data["resource"].(map[string]any)
	if !requestOK || request["request_id"] != "same-key" || request["state"] != "leased" || request["duration_ns"] != float64(10*time.Second) || !leaseRecordOK || leaseRecord["id"] != originalLease["id"] || !resourceOK || resource["id"] != "qa-01" || resource["state"] != "leased" {
		t.Fatalf("conflict response = %#v, want only the original committed request/lease/resource", conflict.Data)
	}
	statusCode, statusByRequest := runJSON(t, socket, "lease", "status", "--request-id", "same-key", "--json")
	leaseAfterConflict, leaseOK := statusByRequest.Data["lease"].(map[string]any)
	if statusCode != 0 || statusByRequest.State != "leased" || !leaseOK || leaseAfterConflict["id"] != originalLease["id"] {
		t.Fatalf("original request after conflict = (%d, %#v), want same active lease", statusCode, statusByRequest)
	}
	_, status := runJSON(t, socket, "service", "status", "--json")
	assertServiceSnapshot(t, status, 1, 0, "qa-01", "leased", 1)
	afterCounts := status.Data["counts"].(map[string]any)
	if beforeCounts["audit_rows"] != afterCounts["audit_rows"] {
		t.Fatalf("conflict changed audit count from %v to %v", beforeCounts["audit_rows"], afterCounts["audit_rows"])
	}
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

// TestAutoKeyedServiceInterruptionPreservesQueuedRequest proves a real service keeps an auto-keyed waiter and its original monotonic deadline after the CLI is interrupted, and that status plus a later wait recover the single committed grant.
func TestAutoKeyedServiceInterruptionPreservesQueuedRequest(t *testing.T) {
	_, socket := startFixture(t)
	stateDir := filepath.Join(filepath.Dir(socket), "state")
	if code, got := runJSON(t, socket, "resource", "add", "--tag", "qa.interrupt", "--id", "interrupt-01", "--mode", "manual", "--json"); code != 0 || got.State != "added" {
		t.Fatalf("resource add = (%d,%#v), want added", code, got)
	}
	holderCode, holder := runJSON(t, socket, "lease", "request", "--tag", "qa.interrupt", "--duration", "10m", "--no-wait", "--request-id", "interrupt-holder", "--json")
	if holderCode != 0 || holder.State != "granted" {
		t.Fatalf("holder acquisition = (%d,%#v), want grant", holderCode, holder)
	}
	holderLease, ok := holder.Data["lease"].(map[string]any)
	holderLeaseID, idOK := holderLease["id"].(string)
	if !ok || !idOK || holderLeaseID == "" {
		t.Fatalf("holder lease = %#v, want lease ID", holder.Data["lease"])
	}

	ctx, cancel := context.WithCancel(context.Background())
	finished := make(chan struct {
		code   int
		stdout string
		stderr string
	}, 1)
	go func() {
		var stdout, stderr bytes.Buffer
		code := runAtSocketContext(ctx, []string{"lease", "request", "--tag", "qa.interrupt", "--duration", "10s", "--wait", "--wait-timeout", "20m", "--json"}, &stdout, &stderr, socket)
		finished <- struct {
			code   int
			stdout string
			stderr string
		}{code: code, stdout: stdout.String(), stderr: stderr.String()}
	}()
	completed := false
	t.Cleanup(func() {
		cancel()
		if !completed {
			select {
			case <-finished:
			case <-time.After(3 * time.Second):
				t.Error("auto-keyed CLI did not stop after cleanup cancellation")
			}
		}
	})

	queued := waitForQueuedRequestRow(t, stateDir, "qa.interrupt")
	requestID, idOK := queued["request_id"].(string)
	deadline, deadlineOK := queued["wait_deadline_monotonic_ns"].(int64)
	bootID, bootOK := queued["queue_boot_session_id"].(string)
	if !idOK || len(requestID) != 32 || strings.Trim(requestID, "0123456789abcdef") != "" || !deadlineOK || !bootOK || bootID == "" {
		t.Fatalf("queued auto-key row = %#v, want generated key and persisted deadline/epoch", queued)
	}
	cancel()
	var cliResult struct {
		code   int
		stdout string
		stderr string
	}
	select {
	case cliResult = <-finished:
		completed = true
	case <-time.After(3 * time.Second):
		t.Fatal("service-backed CLI did not return after interruption")
	}
	var interrupted result
	if err := json.Unmarshal([]byte(cliResult.stdout), &interrupted); err != nil {
		t.Fatalf("decode interrupted service-backed CLI output %q: %v", cliResult.stdout, err)
	}
	if cliResult.code != 130 || interrupted.State != "error" || interrupted.Code != "client_interrupted" || interrupted.RequestID != requestID || interrupted.Data["attempt_state"] != "unknown" || cliResult.stderr != "" {
		t.Fatalf("service-backed interruption = (%d,%#v,stderr=%q), want unknown result for %s", cliResult.code, interrupted, cliResult.stderr, requestID)
	}

	statusCode, status := runJSON(t, socket, "lease", "status", "--request-id", requestID, "--json")
	if statusCode != 0 || status.State != "queued" || status.RequestID != requestID {
		t.Fatalf("status after interruption = (%d,%#v), want original queued request", statusCode, status)
	}
	if current := readRequestRow(t, stateDir, requestID); current["state"] != "queued" || current["wait_deadline_monotonic_ns"] != deadline || current["queue_boot_session_id"] != bootID {
		t.Fatalf("queued request changed after interruption: %#v; want deadline=%d epoch=%s", current, deadline, bootID)
	}

	if code, released := runJSON(t, socket, "lease", "release", "--id", holderLeaseID, "--json"); code != 0 || released.State != "released" {
		t.Fatalf("release holder = (%d,%#v), want released", code, released)
	}
	waitCode, recovered := runJSON(t, socket, "lease", "wait", "--request-id", requestID, "--json")
	if waitCode != 0 || recovered.State != "granted" || recovered.RequestID != requestID || recovered.Data["lease"] == nil || recovered.Data["resource"] == nil {
		t.Fatalf("wait recovery = (%d,%#v), want the committed grant", waitCode, recovered)
	}
	if after := readRequestRow(t, stateDir, requestID); after["state"] != "leased" || after["wait_deadline_monotonic_ns"] != deadline || after["queue_boot_session_id"] != bootID {
		t.Fatalf("request changed after grant recovery: %#v; original deadline=%d epoch=%s", after, deadline, bootID)
	}
	db, err := sqlite.Open(filepath.Join(stateDir, "leases.sqlite"))
	if err != nil {
		t.Fatalf("open service database for audit check: %v", err)
	}
	defer db.Close()
	audits, err := db.Query("SELECT COUNT(*) AS count FROM audit WHERE request_id=?", requestID)
	if err != nil || len(audits) != 1 || audits[0]["count"] != int64(4) {
		t.Fatalf("queued/granted audit rows = %#v, %v; want one queued transition and three grant transitions", audits, err)
	}
}

func waitForQueuedRequestRow(t *testing.T, stateDir, tag string) sqlite.Row {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		db, err := sqlite.Open(filepath.Join(stateDir, "leases.sqlite"))
		if err != nil {
			t.Fatalf("open service database while awaiting queued request: %v", err)
		}
		rows, queryErr := db.Query("SELECT request_id,state,wait_deadline_monotonic_ns,queue_boot_session_id FROM requests WHERE tag=? AND state='queued' ORDER BY queue_sequence DESC LIMIT 1", tag)
		closeErr := db.Close()
		if queryErr != nil || closeErr != nil {
			t.Fatalf("read queued request row: query=%v close=%v", queryErr, closeErr)
		}
		if len(rows) == 1 {
			return rows[0]
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal("auto-keyed request never reached durable queued state")
	return nil
}

func readRequestRow(t *testing.T, stateDir, requestID string) sqlite.Row {
	t.Helper()
	db, err := sqlite.Open(filepath.Join(stateDir, "leases.sqlite"))
	if err != nil {
		t.Fatalf("open service database: %v", err)
	}
	rows, queryErr := db.Query("SELECT request_id,state,wait_deadline_monotonic_ns,queue_boot_session_id FROM requests WHERE request_id=?", requestID)
	closeErr := db.Close()
	if queryErr != nil || closeErr != nil || len(rows) != 1 {
		t.Fatalf("read request row: rows=%#v query=%v close=%v", rows, queryErr, closeErr)
	}
	return rows[0]
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
