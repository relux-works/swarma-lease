package lease

import (
	"math"
	"os"
	"path/filepath"
	"testing"
	"time"
)

// TestSchedulerPromotionUsesSystemActorAndInheritedRequest proves the caller's queued request is a caller row while the later scheduler promotion is a system row with the stored caller and request IDs; labeling the grant as caller is rejected.
func TestSchedulerPromotionUsesSystemActorAndInheritedRequest(t *testing.T) {
	server := newDirectTestServer(t)
	addManualResource(t, server, "qa.audit", "audit-resource")
	holderResponse := directRequest(t, server, "lease.acquire", "audit-holder", map[string]any{
		"requester_id": "holder-caller", "tag": "qa.audit", "duration_ns": int64(time.Minute), "wait_mode": "no_wait",
	})
	holder, ok := holderResponse.Data["lease"].(LeaseRecord)
	if holderResponse.State != "granted" || !ok {
		t.Fatalf("holder grant = %#v, want a lease", holderResponse)
	}
	waitNS := int64(time.Minute)
	waiterResponse := directRequest(t, server, "lease.acquire", "audit-waiter", map[string]any{
		"requester_id": "waiter-caller", "tag": "qa.audit", "duration_ns": int64(time.Minute), "wait_mode": "wait", "wait_timeout_ns": waitNS,
	})
	if waiterResponse.State != "queued" {
		t.Fatalf("waiter acquire = %#v, want queued", waiterResponse)
	}
	released := directRequest(t, server, "lease.release", "", map[string]any{"lease_id": holder.ID})
	if released.State != "released" {
		t.Fatalf("holder release = %#v, want released", released)
	}

	rows, err := server.store.db.Query(`SELECT operation,target_id,actor_kind,actor_component,cause,cause_ref_kind,cause_ref_id,request_id,requester_id,holder_id,legacy_unattributed,origin
FROM audit WHERE request_id='audit-waiter' ORDER BY sequence`)
	if err != nil || len(rows) != 4 {
		t.Fatalf("waiter audit rows = %#v, %v; want one caller request and three system grant rows", rows, err)
	}
	callerRows, systemRows := 0, 0
	for _, row := range rows {
		switch rowString(row, "actor_kind") {
		case "caller":
			callerRows++
			if rowString(row, "operation") != "lease.acquire" || rowString(row, "target_id") != "audit-waiter" || rowString(row, "requester_id") != "waiter-caller" || nullableString(row, "cause") != nil || nullableString(row, "actor_component") != nil {
				t.Errorf("caller request row = %#v, want only the direct acquire action", row)
			}
		case "system":
			systemRows++
			if rowString(row, "operation") != "scheduler" || rowString(row, "actor_component") != "scheduler" || rowString(row, "cause") != "promotion" || rowString(row, "cause_ref_kind") != "request" || rowString(row, "cause_ref_id") != "audit-waiter" || rowString(row, "request_id") != "audit-waiter" || rowString(row, "requester_id") != "waiter-caller" || rowInt(row, "legacy_unattributed") != 0 || nullableString(row, "origin") != nil {
				t.Errorf("scheduler promotion row = %#v, want system attribution with inherited v3 request identity", row)
			}
		default:
			t.Errorf("waiter audit row has unexpected actor_kind: %#v", row)
		}
	}
	if callerRows != 1 || systemRows != 3 {
		t.Fatalf("waiter audit actors = caller:%d system:%d, want caller:1 system:3", callerRows, systemRows)
	}

	history, code := server.store.History(50, nil)
	entries, entriesOK := history["entries"].([]AuditEntry)
	if code != "" || !entriesOK {
		t.Fatalf("audit history = %#v/%s, want typed entries", history, code)
	}
	seenCaller, seenSystem := false, false
	for _, entry := range entries {
		if stringValue(entry.RequestID) != "audit-waiter" {
			continue
		}
		seenCaller = seenCaller || entry.ActorKind == "caller"
		seenSystem = seenSystem || entry.ActorKind == "system" && stringValue(entry.Cause) == "promotion"
	}
	if !seenCaller || !seenSystem {
		t.Fatalf("audit query actor projection caller=%v system=%v, want both distinguishable", seenCaller, seenSystem)
	}

	before := hardEndAuditCount(t, server)
	_, err = server.store.db.Exec(`INSERT INTO audit(at,operation,actor_kind,actor_component,cause,cause_ref_kind,cause_ref_id,origin,request_id,requester_id,holder_id,legacy_unattributed,target_id,result_code,from_state,to_state)
VALUES(?, 'scheduler', 'caller', 'scheduler', 'promotion', 'request', 'audit-waiter', NULL, 'audit-waiter', 'waiter-caller', NULL, 0, 'bad-grant', 'ok', 'queued', 'leased')`, timestamp(time.Now().UTC()))
	if err == nil || hardEndAuditCount(t, server) != before {
		t.Fatalf("caller-labeled scheduler grant insert err=%v audit count %d -> %d, want constraint rejection with no row", err, before, hardEndAuditCount(t, server))
	}
}

// TestSchedulerExhaustionCanBeRecordedWithoutCaller proves the scheduler can disable a terminal-fence resource without inventing a caller, while a normal resource operation remains a separately queryable caller row.
func TestSchedulerExhaustionCanBeRecordedWithoutCaller(t *testing.T) {
	server := newDirectTestServer(t)
	addManualResource(t, server, "qa.exhaustion", "exhausted-resource")
	if _, err := server.store.db.Exec("UPDATE resources SET fencing_token=? WHERE id=?", int64(math.MaxInt64), "exhausted-resource"); err != nil {
		t.Fatalf("seed exhausted resource fixture: %v", err)
	}
	changed, code := server.store.RunScheduler()
	if code != "" || !changed {
		t.Fatalf("scheduler exhaustion = changed:%v code:%s, want committed transition", changed, code)
	}
	stateRows, err := server.store.db.Query("SELECT state FROM resources WHERE id=?", "exhausted-resource")
	if err != nil || len(stateRows) != 1 || rowString(stateRows[0], "state") != "disabled" {
		t.Fatalf("exhausted resource = %#v, %v; want disabled", stateRows, err)
	}
	rows, err := server.store.db.Query(`SELECT actor_kind,actor_component,cause,cause_ref_kind,cause_ref_id,request_id,requester_id,legacy_unattributed,origin
FROM audit WHERE target_id='exhausted-resource' ORDER BY sequence`)
	if err != nil || len(rows) != 2 {
		t.Fatalf("resource audit rows = %#v, %v; want caller add plus scheduler exhaustion", rows, err)
	}
	caller, system := rows[0], rows[1]
	if rowString(caller, "actor_kind") != "caller" || rowString(caller, "requester_id") != "test-caller" || nullableString(caller, "request_id") != nil {
		t.Fatalf("resource add actor = %#v, want caller with no parent", caller)
	}
	if rowString(system, "actor_kind") != "system" || rowString(system, "actor_component") != "scheduler" || rowString(system, "cause") != "exhaustion" || rowString(system, "cause_ref_kind") != "resource" || rowString(system, "cause_ref_id") != "exhausted-resource" || nullableString(system, "request_id") != nil || nullableString(system, "requester_id") != nil || rowInt(system, "legacy_unattributed") != 0 || nullableString(system, "origin") != nil {
		t.Fatalf("scheduler exhaustion actor = %#v, want a no-caller system row", system)
	}
}

// TestPreV3LeaseExpiryWritesMarkedSystemRows proves startup expires an overdue migrated lease without inventing a caller and marks each new transition row as scheduler-originated pre-v3 data.
func TestPreV3LeaseExpiryWritesMarkedSystemRows(t *testing.T) {
	base := hardEndBaseSample()
	clock := newControlledLeaseClock(hardEndSampleAfter(base, 10*time.Second, 10*time.Second))
	stateDir := filepath.Join(shortTestDir(t), "state")
	writeCanonicalV2IdentityFixture(t, stateDir, int64(os.Geteuid()))
	server, err := Start(Config{StateDir: stateDir, SocketPath: filepath.Join(stateDir, "service.sock"), Clock: clock})
	if err != nil {
		t.Fatalf("start and recover migrated v2 fixture: %v", err)
	}
	t.Cleanup(func() {
		if err := server.Close(); err != nil {
			t.Errorf("close migrated fixture service: %v", err)
		}
	})

	rows, err := server.store.db.Query(`SELECT operation,target_id,actor_kind,actor_component,cause,cause_ref_kind,cause_ref_id,request_id,requester_id,holder_id,legacy_unattributed,origin
FROM audit WHERE request_id='go-active' AND cause='expiry' ORDER BY sequence`)
	if err != nil || len(rows) != 3 {
		t.Fatalf("legacy lease expiry rows = %#v, %v; want request, lease and resource transitions", rows, err)
	}
	for _, row := range rows {
		if rowString(row, "actor_kind") != "system" || rowString(row, "actor_component") != "scheduler" || rowString(row, "cause_ref_kind") != "lease" || rowString(row, "cause_ref_id") != "00000000000000000000000000000007" || rowString(row, "request_id") != "go-active" || nullableString(row, "requester_id") != nil || nullableString(row, "holder_id") != nil || rowInt(row, "legacy_unattributed") != 1 || rowString(row, "origin") != "pre_v3" {
			t.Errorf("pre-v3 expiry row = %#v, want system expiry with legacy marker and no caller", row)
		}
	}
	state, data, code := server.store.Status("go-active", "")
	lease, ok := data["lease"].(LeaseRecord)
	if code != "" || state != "expired" || !ok || lease.State != "expired" {
		t.Fatalf("migrated lease after expiry = %s/%s/%#v, want expired", state, code, data)
	}
}

// TestPreV3SystemAuditRequiresOriginMarker proves a pre-v3-derived system row needs origin=pre_v3 and rejects the unmarked form without inserting it.
func TestPreV3SystemAuditRequiresOriginMarker(t *testing.T) {
	server := newDirectTestServer(t)
	at := "2026-11-18T12:00:00.000000000Z"
	insertWithoutOrigin := `INSERT INTO audit(at,operation,actor_kind,actor_component,cause,cause_ref_kind,cause_ref_id,origin,request_id,requester_id,holder_id,legacy_unattributed,target_id,result_code,from_state,to_state)
VALUES(?, 'scheduler', 'system', 'scheduler', 'expiry', 'lease', 'legacy-lease', NULL, 'legacy-request', NULL, NULL, 1, 'legacy-lease', 'lease_expired', 'active', 'expired')`
	insertWithOrigin := `INSERT INTO audit(at,operation,actor_kind,actor_component,cause,cause_ref_kind,cause_ref_id,origin,request_id,requester_id,holder_id,legacy_unattributed,target_id,result_code,from_state,to_state)
VALUES(?, 'scheduler', 'system', 'scheduler', 'expiry', 'lease', 'legacy-lease', 'pre_v3', 'legacy-request', NULL, NULL, 1, 'legacy-lease', 'lease_expired', 'active', 'expired')`
	before := hardEndAuditCount(t, server)
	if _, err := server.store.db.Exec(insertWithoutOrigin, at); err == nil {
		t.Fatal("pre-v3 system audit without origin marker was accepted")
	}
	if got := hardEndAuditCount(t, server); got != before {
		t.Fatalf("unmarked pre-v3 row changed audit count: before=%d after=%d", before, got)
	}
	if _, err := server.store.db.Exec(insertWithOrigin, at); err != nil {
		t.Fatalf("pre-v3-derived system audit with origin marker was rejected: %v", err)
	}
	rows, err := server.store.db.Query("SELECT actor_kind,actor_component,cause,request_id,requester_id,holder_id,legacy_unattributed,origin FROM audit")
	if err != nil || len(rows) != 1 || rowString(rows[0], "actor_kind") != "system" || rowString(rows[0], "actor_component") != "scheduler" || rowString(rows[0], "cause") != "expiry" || rowString(rows[0], "request_id") != "legacy-request" || rows[0]["requester_id"] != nil || rows[0]["holder_id"] != nil || rowInt(rows[0], "legacy_unattributed") != 1 || rowString(rows[0], "origin") != "pre_v3" {
		t.Fatalf("marked pre-v3 system row = %#v, %v; want legacy-marked system transition without caller", rows, err)
	}
}
