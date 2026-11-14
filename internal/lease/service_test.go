package lease

import (
	"context"
	"encoding/json"
	"net"
	"os"
	"path/filepath"
	"testing"
)

func newTestServer(t *testing.T) *Server {
	t.Helper()
	root := shortTestDir(t)
	server, err := Start(Config{
		StateDir:   filepath.Join(root, "state"),
		SocketPath: filepath.Join(root, "service.sock"),
	})
	if err != nil {
		t.Fatalf("start service: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(func() {
		cancel()
		if err := server.Close(); err != nil {
			t.Errorf("close service: %v", err)
		}
	})
	go func() {
		if err := server.Serve(ctx); err != nil {
			t.Errorf("serve socket: %v", err)
		}
	}()
	return server
}

func newDirectTestServer(t *testing.T) *Server {
	t.Helper()
	root := shortTestDir(t)
	server, err := Start(Config{
		StateDir:   filepath.Join(root, "state"),
		SocketPath: filepath.Join(root, "service.sock"),
	})
	if err != nil {
		t.Fatalf("start direct service: %v", err)
	}
	t.Cleanup(func() {
		if err := server.Close(); err != nil {
			t.Errorf("close direct service: %v", err)
		}
	})
	return server
}

func shortTestDir(t *testing.T) string {
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
	return root
}

func request(t *testing.T, server *Server, peer uint32, v any) Response {
	t.Helper()
	raw, err := json.Marshal(v)
	if err != nil {
		t.Fatal(err)
	}
	response, _ := server.handleBytes(raw, peer)
	return response
}

// TestCrossUserReleaseRefusesWithoutStateChange proves a peer outside the configured OS-user boundary cannot release another user's active lease or change its durable state.
func TestCrossUserReleaseRefusesWithoutStateChange(t *testing.T) {
	server := newTestServer(t)
	uid := uint32(server.ownerUID)
	if got := request(t, server, uid, map[string]any{
		"version": 1, "type": "request", "op": "resource.add",
		"data": map[string]any{"tag": "qa.pool", "resource_id": "qa-01", "mode": "manual"},
	}); got.State != "added" {
		t.Fatalf("add resource = %#v, want added", got)
	}
	granted := request(t, server, uid, map[string]any{
		"version": 1, "type": "request", "op": "lease.acquire", "request_id": "owner-request",
		"data": map[string]any{"tag": "qa.pool", "duration_ns": int64(10_000_000_000), "wait_mode": "no_wait"},
	})
	if granted.State != "granted" {
		t.Fatalf("acquire = %#v, want grant", granted)
	}
	leaseRecord, ok := granted.Data["lease"].(LeaseRecord)
	if !ok {
		t.Fatalf("grant lease has type %T, want LeaseRecord", granted.Data["lease"])
	}
	leaseID := leaseRecord.ID
	foreign := request(t, server, uid+1, map[string]any{
		"version": 1, "type": "request", "op": "lease.release",
		"data": map[string]any{"lease_id": leaseID},
	})
	if foreign.Code != "permissions_invalid" {
		t.Fatalf("foreign release = %#v, want permissions_invalid", foreign)
	}
	status := request(t, server, uid, map[string]any{
		"version": 1, "type": "request", "op": "lease.status",
		"data": map[string]any{"request_id": "owner-request"},
	})
	statusLease, leaseOK := status.Data["lease"].(LeaseRecord)
	statusResource, resourceOK := status.Data["resource"].(ResourceRecord)
	if status.State != "leased" || !leaseOK || statusLease.ID != leaseID || !resourceOK || statusResource.State != "leased" {
		t.Fatalf("post-refusal status = %#v, want unchanged active lease", status)
	}
}

// TestSQLiteGrantFailureRollsBackEveryTransition proves a real SQLite trigger failure during lease insertion leaves no request, lease, audit row, or resource-state change.
func TestSQLiteGrantFailureRollsBackEveryTransition(t *testing.T) {
	server := newTestServer(t)
	uid := uint32(server.ownerUID)
	added := request(t, server, uid, map[string]any{
		"version": 1, "type": "request", "op": "resource.add",
		"data": map[string]any{"tag": "qa.pool", "resource_id": "qa-01", "mode": "manual"},
	})
	if added.State != "added" {
		t.Fatalf("add resource = %#v, want added", added)
	}
	if err := server.store.db.ExecScript("CREATE TRIGGER injected_lease_insert_failure BEFORE INSERT ON leases BEGIN SELECT RAISE(ABORT, 'injected'); END;"); err != nil {
		t.Fatalf("install isolated failure trigger: %v", err)
	}
	failed := request(t, server, uid, map[string]any{
		"version": 1, "type": "request", "op": "lease.acquire", "request_id": "rollback-request",
		"data": map[string]any{"tag": "qa.pool", "duration_ns": int64(10_000_000_000), "wait_mode": "no_wait"},
	})
	if failed.Code != "storage_unavailable" {
		t.Fatalf("injected grant failure = %#v, want storage_unavailable", failed)
	}
	status := request(t, server, uid, map[string]any{
		"version": 1, "type": "request", "op": "service.status", "data": map[string]any{},
	})
	counts, countsOK := status.Data["counts"].(Counts)
	pools, poolsOK := status.Data["pools"].([]PoolStatus)
	if status.Code != "" || !countsOK || counts.ActiveLeases != 0 || counts.AuditRows != 1 || !poolsOK || len(pools) != 3 {
		t.Fatalf("post-failure status = %#v, want only the committed resource.add audit and default pools", status)
	}
	resource := findResource(pools, "qa-01")
	if resource == nil || resource.State != "available" || resource.FencingToken != 0 {
		t.Fatalf("post-failure resource = %#v, want available at token zero", resource)
	}
	unknown := request(t, server, uid, map[string]any{
		"version": 1, "type": "request", "op": "lease.status",
		"data": map[string]any{"request_id": "rollback-request"},
	})
	if unknown.Code != "unknown_request" {
		t.Fatalf("post-failure request lookup = %#v, want no persisted request", unknown)
	}
}

// TestResourceControlAndRecoveryTransitions proves audit nullable fields, disable/enable idempotency, the command-resource recovery gate, and the scheduler transition in a synchronous MAX_INT64 enable refusal without a racing background scheduler.
func TestResourceControlAndRecoveryTransitions(t *testing.T) {
	server := newDirectTestServer(t)
	uid := uint32(server.ownerUID)
	add := request(t, server, uid, map[string]any{
		"version": 1, "type": "request", "op": "resource.add",
		"data": map[string]any{"tag": "qa.manual", "resource_id": "manual-01", "mode": "manual"},
	})
	if add.State != "added" {
		t.Fatalf("add manual resource = %#v, want added", add)
	}
	auditRows, err := server.store.db.Query("SELECT request_id,target_id,from_state,to_state FROM audit WHERE operation='resource.add' AND target_id=?", "manual-01")
	if err != nil || len(auditRows) != 1 {
		t.Fatalf("resource-add audit = %v, %v; want one row", auditRows, err)
	}
	if auditRows[0]["request_id"] != nil || auditRows[0]["from_state"] != nil || auditRows[0]["target_id"] != "manual-01" || auditRows[0]["to_state"] != "available" {
		t.Fatalf("resource-add audit fields = %#v, want nullable fields NULL and target/state values", auditRows[0])
	}
	for _, step := range []struct {
		op, wantState, wantResourceState string
	}{
		{"resource.disable", "disabled", "disabled"},
		{"resource.disable", "disabled", "disabled"},
		{"resource.enable", "enabled", "available"},
		{"resource.enable", "enabled", "available"},
	} {
		got := request(t, server, uid, map[string]any{
			"version": 1, "type": "request", "op": step.op,
			"data": map[string]any{"resource_id": "manual-01"},
		})
		resource, ok := got.Data["resource"].(ResourceRecord)
		if got.State != step.wantState || !ok || resource.State != step.wantResourceState {
			t.Fatalf("%s = %#v, want %s/%s", step.op, got, step.wantState, step.wantResourceState)
		}
	}

	command := request(t, server, uid, map[string]any{
		"version": 1, "type": "request", "op": "resource.add",
		"data": map[string]any{"tag": "qa.command", "resource_id": "command-01", "mode": "command"},
	})
	if command.State != "added" {
		t.Fatalf("add command resource = %#v, want added", command)
	}
	if _, err := server.store.db.Exec("UPDATE resources SET state='quarantined' WHERE id=?", "command-01"); err != nil {
		t.Fatalf("seed isolated quarantined fixture: %v", err)
	}
	before := request(t, server, uid, map[string]any{
		"version": 1, "type": "request", "op": "service.status", "data": map[string]any{},
	})
	beforeCounts, countsOK := before.Data["counts"].(Counts)
	if before.Code != "" || !countsOK {
		t.Fatalf("pre-refusal status = %#v, want healthy status", before)
	}
	refused := request(t, server, uid, map[string]any{
		"version": 1, "type": "request", "op": "resource.recover",
		"data": map[string]any{"resource_id": "manual-01", "confirm_stopped": true},
	})
	if refused.Code != "invalid_transition" {
		t.Fatalf("recover manual resource = %#v, want invalid_transition", refused)
	}
	afterRefusal := request(t, server, uid, map[string]any{
		"version": 1, "type": "request", "op": "service.status", "data": map[string]any{},
	})
	afterCounts, countsOK := afterRefusal.Data["counts"].(Counts)
	resources, poolsOK := afterRefusal.Data["pools"].([]PoolStatus)
	if afterRefusal.Code != "" || !countsOK || afterCounts.AuditRows != beforeCounts.AuditRows || !poolsOK {
		t.Fatalf("post-refusal status = %#v, want no audit or state effect", afterRefusal)
	}
	manual := findResource(resources, "manual-01")
	commandFixture := findResource(resources, "command-01")
	if manual == nil || manual.State != "available" || commandFixture == nil || commandFixture.State != "quarantined" {
		t.Fatalf("resources after refused recovery = manual %#v, command %#v", manual, commandFixture)
	}
	recovered := request(t, server, uid, map[string]any{
		"version": 1, "type": "request", "op": "resource.recover",
		"data": map[string]any{"resource_id": "command-01", "confirm_stopped": true},
	})
	resource, ok := recovered.Data["resource"].(ResourceRecord)
	if recovered.State != "recovered" || !ok || resource.State != "available" {
		t.Fatalf("recover quarantined command resource = %#v, want available", recovered)
	}
	if _, err := server.store.db.Exec("UPDATE resources SET fencing_token=? WHERE id=?", int64(^uint64(0)>>1), "manual-01"); err != nil {
		t.Fatalf("seed exhausted fencing token: %v", err)
	}
	beforeMax := request(t, server, uid, map[string]any{
		"version": 1, "type": "request", "op": "service.status", "data": map[string]any{},
	})
	beforeMaxCounts, countsOK := beforeMax.Data["counts"].(Counts)
	beforeMaxPools, poolsOK := beforeMax.Data["pools"].([]PoolStatus)
	manualBeforeMax := findResource(beforeMaxPools, "manual-01")
	if beforeMax.Code != "" || !countsOK || !poolsOK || manualBeforeMax == nil || manualBeforeMax.State != "available" || manualBeforeMax.FencingToken != int64(^uint64(0)>>1) {
		t.Fatalf("status before MAX_INT64 refusal = %#v, want available resource at MAX_INT64", beforeMax)
	}
	maxRefusal := request(t, server, uid, map[string]any{
		"version": 1, "type": "request", "op": "resource.enable",
		"data": map[string]any{"resource_id": "manual-01"},
	})
	if maxRefusal.Code != "invalid_transition" {
		t.Fatalf("enable MAX_INT64 resource = %#v, want invalid_transition", maxRefusal)
	}
	finalStatus := request(t, server, uid, map[string]any{
		"version": 1, "type": "request", "op": "service.status", "data": map[string]any{},
	})
	finalPools, poolsOK := finalStatus.Data["pools"].([]PoolStatus)
	finalCounts, countsOK := finalStatus.Data["counts"].(Counts)
	manual = findResource(finalPools, "manual-01")
	if finalStatus.Code != "" || !poolsOK || !countsOK || finalCounts.AuditRows != beforeMaxCounts.AuditRows+1 || manual == nil || manual.State != "disabled" || manual.FencingToken != int64(^uint64(0)>>1) {
		t.Fatalf("resource after MAX_INT64 enable refusal = %#v, status=%#v", manual, finalStatus)
	}
	maxAudit, err := server.store.db.Query("SELECT operation,result_code,from_state,to_state FROM audit WHERE target_id=? ORDER BY sequence DESC LIMIT 1", "manual-01")
	if err != nil || len(maxAudit) != 1 || rowString(maxAudit[0], "operation") != "scheduler" || rowString(maxAudit[0], "result_code") != "ok" || rowString(maxAudit[0], "from_state") != "available" || rowString(maxAudit[0], "to_state") != "disabled" {
		t.Fatalf("MAX_INT64 pre-refusal audit = %#v, want only the scheduler transition from available to disabled", maxAudit)
	}
}

// TestRestartRecoversProvenStaleSocketAndPreservesLiveSocket proves wrong-mode stale sockets are refused without removal, same-inode mode-correct stale sockets recover, and connectable sockets remain untouched.
func TestRestartRecoversProvenStaleSocketAndPreservesLiveSocket(t *testing.T) {
	root := shortTestDir(t)
	config := Config{StateDir: filepath.Join(root, "state"), SocketPath: filepath.Join(root, "service.sock")}
	first, err := Start(config)
	if err != nil {
		t.Fatalf("start initial service: %v", err)
	}
	if state, _, code := first.store.AddResource("qa.persist", "persist-01", "manual"); code != "" || state != "added" {
		t.Fatalf("persist resource before restart = (%s, %s), want added", state, code)
	}
	if err := first.Close(); err != nil {
		t.Fatalf("close initial service: %v", err)
	}

	stale, err := net.ListenUnix("unix", &net.UnixAddr{Name: config.SocketPath, Net: "unix"})
	if err != nil {
		t.Fatalf("create stale socket fixture: %v", err)
	}
	stale.SetUnlinkOnClose(false)
	if err := stale.Close(); err != nil {
		t.Fatalf("close stale socket fixture: %v", err)
	}
	if err := os.Chmod(config.SocketPath, 0644); err != nil {
		t.Fatalf("set invalid stale socket mode: %v", err)
	}
	invalidModeInfo, err := os.Lstat(config.SocketPath)
	if err != nil {
		t.Fatalf("stat invalid-mode stale socket: %v", err)
	}
	if _, err := Start(config); ErrorCode(err) != "permissions_invalid" {
		t.Fatalf("start with wrong-mode stale socket = %v, want permissions_invalid", err)
	}
	invalidCurrentInfo, err := os.Lstat(config.SocketPath)
	if err != nil || !os.SameFile(invalidModeInfo, invalidCurrentInfo) {
		t.Fatalf("wrong-mode stale socket changed after refusal: info=%v err=%v", invalidCurrentInfo, err)
	}
	if err := os.Chmod(config.SocketPath, 0600); err != nil {
		t.Fatalf("set valid stale socket mode: %v", err)
	}
	recovered, err := Start(config)
	if err != nil {
		t.Fatalf("restart with proven stale socket: %v", err)
	}
	status, code := recovered.store.ServiceStatus()
	if code != "" {
		t.Fatalf("status after restart = %s, want success", code)
	}
	if poolContains(status["pools"].([]PoolStatus), "qa.persist", "persist-01") == false {
		t.Fatal("durable resource registration was lost during same-boot restart")
	}
	if err := recovered.Close(); err != nil {
		t.Fatalf("close recovered service: %v", err)
	}
	if _, err := os.Lstat(config.SocketPath); !os.IsNotExist(err) {
		t.Fatalf("recovered service socket remains after owned shutdown: %v", err)
	}
	live, err := net.ListenUnix("unix", &net.UnixAddr{Name: config.SocketPath, Net: "unix"})
	if err != nil {
		t.Fatalf("create live socket fixture: %v", err)
	}
	live.SetUnlinkOnClose(false)
	if err := os.Chmod(config.SocketPath, 0600); err != nil {
		t.Fatalf("set live socket fixture mode: %v", err)
	}
	t.Cleanup(func() {
		_ = live.Close()
		_ = os.Remove(config.SocketPath)
	})
	ownedInfo, err := os.Lstat(config.SocketPath)
	if err != nil {
		t.Fatalf("stat live socket fixture: %v", err)
	}
	if _, err := Start(config); ErrorCode(err) != "socket_stale_uncertain" {
		t.Fatalf("start with connectable socket = %v, want socket_stale_uncertain", err)
	}
	currentInfo, err := os.Lstat(config.SocketPath)
	if err != nil || !os.SameFile(ownedInfo, currentInfo) {
		t.Fatalf("live socket changed after refusal: info=%v err=%v", currentInfo, err)
	}
}

func poolContains(pools []PoolStatus, tag, resourceID string) bool {
	for _, pool := range pools {
		if pool.Tag != tag {
			continue
		}
		for _, resource := range pool.Resources {
			if resource.ID == resourceID {
				return true
			}
		}
	}
	return false
}

func findResource(pools []PoolStatus, id string) *ResourceRecord {
	for _, pool := range pools {
		for i := range pool.Resources {
			if pool.Resources[i].ID == id {
				return &pool.Resources[i]
			}
		}
	}
	return nil
}
