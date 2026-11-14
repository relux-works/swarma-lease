package lease

import (
	"errors"
	"math"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

const (
	hardEndBootA = "00000000-0000-4000-8000-000000000001"
	hardEndBootB = "00000000-0000-4000-8000-000000000002"
)

type controlledLeaseClock struct {
	mu     sync.Mutex
	sample Sample
	err    error
}

func newControlledLeaseClock(sample Sample) *controlledLeaseClock {
	return &controlledLeaseClock{sample: sample}
}

func (c *controlledLeaseClock) Read() (Sample, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.err != nil {
		return Sample{}, c.err
	}
	return c.sample, nil
}

func (c *controlledLeaseClock) set(sample Sample) {
	c.mu.Lock()
	c.sample = sample
	c.err = nil
	c.mu.Unlock()
}

func (c *controlledLeaseClock) fail() {
	c.mu.Lock()
	c.err = errors.New("injected clock failure")
	c.mu.Unlock()
}

func hardEndBaseSample() Sample {
	return Sample{
		Wall:          time.Date(2026, time.November, 14, 0, 0, 0, 0, time.UTC),
		MonotonicNS:   int64(time.Second),
		BootSessionID: hardEndBootA,
	}
}

func hardEndSampleAfter(base Sample, monotonic, wall time.Duration) Sample {
	return Sample{
		Wall:          base.Wall.Add(wall),
		MonotonicNS:   base.MonotonicNS + int64(monotonic),
		BootSessionID: base.BootSessionID,
	}
}

func hardEndConfig(t *testing.T, clock Clock) Config {
	t.Helper()
	root := shortTestDir(t)
	return Config{
		StateDir:   filepath.Join(root, "state"),
		SocketPath: filepath.Join(root, "service.sock"),
		Clock:      clock,
	}
}

func startHardEndServer(t *testing.T, config Config) *Server {
	t.Helper()
	server, err := Start(config)
	if err != nil {
		t.Fatalf("start controlled-clock service: %v", err)
	}
	t.Cleanup(func() {
		if err := server.Close(); err != nil {
			t.Errorf("close controlled-clock service: %v", err)
		}
	})
	return server
}

func acquireHardEndLease(t *testing.T, server *Server, requestID, tag string, duration time.Duration) LeaseRecord {
	t.Helper()
	result := directRequest(t, server, "lease.acquire", requestID, map[string]any{
		"tag": tag, "duration_ns": int64(duration), "wait_mode": "no_wait",
	})
	lease, ok := result.Data["lease"].(LeaseRecord)
	if result.State != "granted" || !ok {
		t.Fatalf("acquire %s = %#v, want a grant", requestID, result)
	}
	return lease
}

func hardEndStatus(t *testing.T, server *Server, requestID string) (string, map[string]any) {
	t.Helper()
	state, data, code := server.store.Status(requestID, "")
	if code != "" {
		t.Fatalf("status %s = %s, want success", requestID, code)
	}
	return state, data
}

func hardEndLeaseClock(t *testing.T, server *Server, requestID string) (int64, int64, string) {
	t.Helper()
	rows, err := server.store.db.Query("SELECT granted_monotonic_ns,hard_end_monotonic_ns,boot_session_id FROM leases WHERE request_id=?", requestID)
	if err != nil || len(rows) != 1 {
		t.Fatalf("lease clock fields for %s = %#v, %v", requestID, rows, err)
	}
	return rowInt(rows[0], "granted_monotonic_ns"), rowInt(rows[0], "hard_end_monotonic_ns"), rowString(rows[0], "boot_session_id")
}

func hardEndQueueClock(t *testing.T, server *Server, requestID string) (int64, int64, int64, string) {
	t.Helper()
	rows, err := server.store.db.Query("SELECT queue_sequence,queue_started_monotonic_ns,wait_deadline_monotonic_ns,queue_boot_session_id FROM requests WHERE request_id=?", requestID)
	if err != nil || len(rows) != 1 {
		t.Fatalf("queue clock fields for %s = %#v, %v", requestID, rows, err)
	}
	return rowInt(rows[0], "queue_sequence"), rowInt(rows[0], "queue_started_monotonic_ns"), rowInt(rows[0], "wait_deadline_monotonic_ns"), rowString(rows[0], "queue_boot_session_id")
}

func hardEndAuditCount(t *testing.T, server *Server) int64 {
	t.Helper()
	rows, err := server.store.db.Query("SELECT COUNT(*) AS count FROM audit")
	if err != nil || len(rows) != 1 {
		t.Fatalf("audit count = %#v, %v", rows, err)
	}
	return rowInt(rows[0], "count")
}

func hardEndAuditMax(t *testing.T, server *Server, requestID, resultCode string) int64 {
	t.Helper()
	rows, err := server.store.db.Query("SELECT COALESCE(MAX(sequence),0) AS sequence FROM audit WHERE request_id=? AND result_code=?", requestID, resultCode)
	if err != nil || len(rows) != 1 {
		t.Fatalf("audit maximum for %s/%s = %#v, %v", requestID, resultCode, rows, err)
	}
	return rowInt(rows[0], "sequence")
}

func hardEndAuditGrantSequence(t *testing.T, server *Server, requestID string) int64 {
	t.Helper()
	rows, err := server.store.db.Query("SELECT COALESCE(MIN(sequence),0) AS sequence FROM audit WHERE request_id=? AND to_state='active'", requestID)
	if err != nil || len(rows) != 1 {
		t.Fatalf("grant audit sequence for %s = %#v, %v", requestID, rows, err)
	}
	return rowInt(rows[0], "sequence")
}

func runHardEndScheduler(t *testing.T, server *Server, sample Sample) {
	t.Helper()
	clock, ok := server.clock.(*controlledLeaseClock)
	if !ok {
		t.Fatal("test server clock is not controlled")
	}
	clock.set(sample)
	if _, code := server.store.RunScheduler(); code != "" {
		t.Fatalf("run scheduler at monotonic %d = %s, want success", sample.MonotonicNS, code)
	}
}

// TestWallClockStepsDoNotChangeLeaseOrWaitDeadlines proves forward and backward wall-clock steps leave lease and waiter outcomes governed by their monotonic deadlines.
func TestWallClockStepsDoNotChangeLeaseOrWaitDeadlines(t *testing.T) {
	base := hardEndBaseSample()
	clock := newControlledLeaseClock(base)
	server := startHardEndServer(t, hardEndConfig(t, clock))
	addManualResource(t, server, "qa.lease", "lease-01")
	addManualResource(t, server, "qa.wait", "wait-01")
	lease := acquireHardEndLease(t, server, "lease-owner", "qa.lease", 10*time.Second)
	if holder := acquireHardEndLease(t, server, "wait-holder", "qa.wait", 30*time.Second); holder.State != "active" {
		t.Fatalf("wait-pool holder = %#v, want active", holder)
	}
	queued := directRequest(t, server, "lease.acquire", "waiter", waitData("qa.wait", 5*time.Second))
	if queued.State != "queued" {
		t.Fatalf("queue waiter = %#v, want queued", queued)
	}

	forward := hardEndSampleAfter(base, time.Second, 24*time.Hour)
	clock.set(forward)
	runHardEndScheduler(t, server, forward)
	leaseState, leaseData := hardEndStatus(t, server, "lease-owner")
	waiterState, _ := hardEndStatus(t, server, "waiter")
	activeLease, leaseOK := leaseData["lease"].(LeaseRecord)
	if leaseState != "leased" || !leaseOK || activeLease.State != "active" || waiterState != "queued" {
		t.Fatalf("after forward wall step: lease=%s/%#v waiter=%s, want active/queued", leaseState, leaseData["lease"], waiterState)
	}

	backward := hardEndSampleAfter(base, 5*time.Second, -24*time.Hour)
	clock.set(backward)
	runHardEndScheduler(t, server, backward)
	waiterState, waiterData := hardEndStatus(t, server, "waiter")
	waiterRecord, waiterOK := waiterData["request"].(*RequestRecord)
	leaseState, leaseData = hardEndStatus(t, server, "lease-owner")
	activeLease, leaseOK = leaseData["lease"].(LeaseRecord)
	if waiterState != "timed_out" || !waiterOK || waiterRecord.OutcomeCode == nil || *waiterRecord.OutcomeCode != "request_timed_out" || leaseState != "leased" || !leaseOK || activeLease.State != "active" {
		t.Fatalf("at waiter monotonic deadline after backward wall step: waiter=%s/%#v lease=%s/%#v", waiterState, waiterData["request"], leaseState, leaseData["lease"])
	}

	backwardAtLeaseEnd := hardEndSampleAfter(base, 10*time.Second, -48*time.Hour)
	clock.set(backwardAtLeaseEnd)
	runHardEndScheduler(t, server, backwardAtLeaseEnd)
	leaseState, leaseData = hardEndStatus(t, server, "lease-owner")
	expiredLease, leaseOK := leaseData["lease"].(LeaseRecord)
	resource, resourceOK := leaseData["resource"].(ResourceRecord)
	if leaseState != "expired" || !leaseOK || expiredLease.State != "expired" || expiredLease.ID != lease.ID || expiredLease.EndedResourceState == nil || *expiredLease.EndedResourceState != "available" || !resourceOK || resource.State != "available" {
		t.Fatalf("at lease monotonic deadline after backward wall step: state=%s data=%#v", leaseState, leaseData)
	}
}

// TestStableWallClockUsesTheSameMonotonicBoundaries proves the no-step control expires a waiter and lease at their exact monotonic deadlines.
func TestStableWallClockUsesTheSameMonotonicBoundaries(t *testing.T) {
	base := hardEndBaseSample()
	clock := newControlledLeaseClock(base)
	server := startHardEndServer(t, hardEndConfig(t, clock))
	addManualResource(t, server, "qa.stable", "stable-01")
	acquireHardEndLease(t, server, "stable-holder", "qa.stable", 10*time.Second)
	queued := directRequest(t, server, "lease.acquire", "stable-waiter", waitData("qa.stable", 5*time.Second))
	if queued.State != "queued" {
		t.Fatalf("queue stable-clock waiter = %#v, want queued", queued)
	}

	beforeTimeout := hardEndSampleAfter(base, 4*time.Second, 4*time.Second)
	clock.set(beforeTimeout)
	runHardEndScheduler(t, server, beforeTimeout)
	leaseState, _ := hardEndStatus(t, server, "stable-holder")
	waiterState, _ := hardEndStatus(t, server, "stable-waiter")
	if leaseState != "leased" || waiterState != "queued" {
		t.Fatalf("stable wall before wait deadline: lease=%s waiter=%s, want leased/queued", leaseState, waiterState)
	}

	atWaitDeadline := hardEndSampleAfter(base, 5*time.Second, 5*time.Second)
	clock.set(atWaitDeadline)
	runHardEndScheduler(t, server, atWaitDeadline)
	leaseState, _ = hardEndStatus(t, server, "stable-holder")
	waiterState, waiterData := hardEndStatus(t, server, "stable-waiter")
	waiter, waiterOK := waiterData["request"].(*RequestRecord)
	if leaseState != "leased" || waiterState != "timed_out" || !waiterOK || waiter.OutcomeCode == nil || *waiter.OutcomeCode != "request_timed_out" {
		t.Fatalf("stable wall at wait deadline: lease=%s waiter=%s/%#v, want leased/timed_out", leaseState, waiterState, waiterData["request"])
	}

	atHardEnd := hardEndSampleAfter(base, 10*time.Second, 10*time.Second)
	clock.set(atHardEnd)
	runHardEndScheduler(t, server, atHardEnd)
	leaseState, leaseData := hardEndStatus(t, server, "stable-holder")
	lease, leaseOK := leaseData["lease"].(LeaseRecord)
	if leaseState != "expired" || !leaseOK || lease.State != "expired" || leaseData["resource"].(ResourceRecord).State != "available" {
		t.Fatalf("stable wall at lease deadline: state=%s data=%#v, want expired/available", leaseState, leaseData)
	}
}

// TestSameBootRestartPreservesDeadlines proves an unexpired lease and waiter retain their original monotonic deadlines across restart, then the overdue lease expires before its FIFO waiter is granted.
func TestSameBootRestartPreservesDeadlines(t *testing.T) {
	base := hardEndBaseSample()
	clock := newControlledLeaseClock(base)
	config := hardEndConfig(t, clock)
	first := startHardEndServer(t, config)
	addManualResource(t, first, "qa.restart", "restart-01")
	acquireHardEndLease(t, first, "restart-holder", "qa.restart", 10*time.Second)
	queued := directRequest(t, first, "lease.acquire", "restart-waiter", waitData("qa.restart", 20*time.Second))
	if queued.State != "queued" {
		t.Fatalf("queue restart waiter = %#v, want queued", queued)
	}
	grantedAt, hardEndAt, leaseBoot := hardEndLeaseClock(t, first, "restart-holder")
	queueSequence, queuedAt, waitDeadline, queueBoot := hardEndQueueClock(t, first, "restart-waiter")
	if err := first.Close(); err != nil {
		t.Fatalf("close first service: %v", err)
	}

	beforeEnd := hardEndSampleAfter(base, 5*time.Second, 24*time.Hour)
	clock.set(beforeEnd)
	second := startHardEndServer(t, config)
	leaseState, leaseData := hardEndStatus(t, second, "restart-holder")
	waiterState, _ := hardEndStatus(t, second, "restart-waiter")
	if leaseState != "leased" || waiterState != "queued" {
		t.Fatalf("same-boot restart before deadline: lease=%s waiter=%s, want leased/queued", leaseState, waiterState)
	}
	gotGrantedAt, gotHardEndAt, gotLeaseBoot := hardEndLeaseClock(t, second, "restart-holder")
	gotQueueSequence, gotQueuedAt, gotWaitDeadline, gotQueueBoot := hardEndQueueClock(t, second, "restart-waiter")
	if grantedAt != gotGrantedAt || hardEndAt != gotHardEndAt || leaseBoot != gotLeaseBoot || queueSequence != gotQueueSequence || queuedAt != gotQueuedAt || waitDeadline != gotWaitDeadline || queueBoot != gotQueueBoot || leaseData["lease"].(LeaseRecord).State != "active" {
		t.Fatalf("same-boot restart changed persisted deadlines: lease (%d,%d,%s)/(%d,%d,%s), queue (%d,%d,%d,%s)/(%d,%d,%d,%s)", grantedAt, hardEndAt, leaseBoot, gotGrantedAt, gotHardEndAt, gotLeaseBoot, queueSequence, queuedAt, waitDeadline, queueBoot, gotQueueSequence, gotQueuedAt, gotWaitDeadline, gotQueueBoot)
	}
	if err := second.Close(); err != nil {
		t.Fatalf("close second service: %v", err)
	}

	atHardEnd := hardEndSampleAfter(base, 10*time.Second, -24*time.Hour)
	clock.set(atHardEnd)
	third := startHardEndServer(t, config)
	holderState, holderData := hardEndStatus(t, third, "restart-holder")
	waiterState, waiterData := hardEndStatus(t, third, "restart-waiter")
	waiterLease, waiterLeaseOK := waiterData["lease"].(LeaseRecord)
	if holderState != "expired" || holderData["lease"].(LeaseRecord).State != "expired" || holderData["resource"].(ResourceRecord).State != "available" || waiterState != "leased" || !waiterLeaseOK || waiterLease.State != "active" {
		t.Fatalf("same-boot overdue recovery: holder=%s/%#v waiter=%s/%#v", holderState, holderData, waiterState, waiterData)
	}
	endSequence := hardEndAuditMax(t, third, "restart-holder", "lease_expired")
	grantSequence := hardEndAuditGrantSequence(t, third, "restart-waiter")
	if endSequence == 0 || grantSequence == 0 || endSequence >= grantSequence {
		t.Fatalf("restart recovery audit order: expiry=%d waiter grant=%d, want expiry first", endSequence, grantSequence)
	}
}

// TestChangedBootExpiresOldEpochBeforeGrant proves reboot expires old-epoch leases and waiters before a valid new request can acquire the manual resource.
func TestChangedBootExpiresOldEpochBeforeGrant(t *testing.T) {
	base := hardEndBaseSample()
	clock := newControlledLeaseClock(base)
	config := hardEndConfig(t, clock)
	first := startHardEndServer(t, config)
	addManualResource(t, first, "qa.reboot", "reboot-01")
	acquireHardEndLease(t, first, "old-holder", "qa.reboot", 60*time.Second)
	queued := directRequest(t, first, "lease.acquire", "old-waiter", waitData("qa.reboot", 90*time.Second))
	if queued.State != "queued" {
		t.Fatalf("queue old-epoch waiter = %#v, want queued", queued)
	}
	if err := first.Close(); err != nil {
		t.Fatalf("close pre-reboot service: %v", err)
	}

	newBoot := hardEndSampleAfter(base, time.Nanosecond, -24*time.Hour)
	newBoot.BootSessionID = hardEndBootB
	clock.set(newBoot)
	second := startHardEndServer(t, config)
	oldLeaseState, oldLeaseData := hardEndStatus(t, second, "old-holder")
	oldWaiterState, oldWaiterData := hardEndStatus(t, second, "old-waiter")
	if oldLeaseState != "expired" || oldLeaseData["lease"].(LeaseRecord).State != "expired" || oldWaiterState != "timed_out" || oldWaiterData["request"].(*RequestRecord).OutcomeCode == nil || *oldWaiterData["request"].(*RequestRecord).OutcomeCode != "request_timed_out" || leaseCount(t, second, "old-waiter") != 0 {
		t.Fatalf("old boot state after recovery: lease=%s/%#v waiter=%s/%#v", oldLeaseState, oldLeaseData, oldWaiterState, oldWaiterData)
	}
	newLease := acquireHardEndLease(t, second, "new-holder", "qa.reboot", 60*time.Second)
	if newLease.FencingToken != 2 {
		t.Fatalf("post-reboot grant token = %d, want 2 after the expired old-epoch grant", newLease.FencingToken)
	}
	leaseEndSequence := hardEndAuditMax(t, second, "old-holder", "lease_expired")
	waiterEndSequence := hardEndAuditMax(t, second, "old-waiter", "request_timed_out")
	newGrantSequence := hardEndAuditGrantSequence(t, second, "new-holder")
	if leaseEndSequence == 0 || waiterEndSequence == 0 || newGrantSequence == 0 || leaseEndSequence >= newGrantSequence || waiterEndSequence >= newGrantSequence {
		t.Fatalf("boot recovery audit order: lease expiry=%d waiter timeout=%d new grant=%d", leaseEndSequence, waiterEndSequence, newGrantSequence)
	}
}

// TestClockUnavailableLeavesLeaseStateUntouched proves a failed clock sample returns clock_unavailable without releasing or expiring a due lease, then a valid sample permits expiry.
func TestClockUnavailableLeavesLeaseStateUntouched(t *testing.T) {
	base := hardEndBaseSample()
	clock := newControlledLeaseClock(base)
	server := startHardEndServer(t, hardEndConfig(t, clock))
	addManualResource(t, server, "qa.clock", "clock-01")
	lease := acquireHardEndLease(t, server, "clock-holder", "qa.clock", 10*time.Second)
	beforeAudit := hardEndAuditCount(t, server)
	clock.set(hardEndSampleAfter(base, 10*time.Second, 24*time.Hour))
	clock.fail()

	refused := directRequest(t, server, "lease.release", "", map[string]any{"lease_id": lease.ID})
	if refused.State != "error" || refused.Code != "clock_unavailable" {
		t.Fatalf("release with unavailable clock = %#v, want clock_unavailable", refused)
	}
	state, data := hardEndStatus(t, server, "clock-holder")
	request, requestOK := data["request"].(*RequestRecord)
	leaseAfterFailure, leaseOK := data["lease"].(LeaseRecord)
	resourceAfterFailure, resourceOK := data["resource"].(ResourceRecord)
	if state != "leased" || !requestOK || request.State != "leased" || request.OutcomeCode != nil || !leaseOK || leaseAfterFailure.ID != lease.ID || leaseAfterFailure.State != "active" || !resourceOK || resourceAfterFailure.State != "leased" || hardEndAuditCount(t, server) != beforeAudit {
		t.Fatalf("clock failure changed lease state: status=%#v audit=%d before=%d", data, hardEndAuditCount(t, server), beforeAudit)
	}

	atHardEnd := hardEndSampleAfter(base, 10*time.Second, 24*time.Hour)
	clock.set(atHardEnd)
	runHardEndScheduler(t, server, atHardEnd)
	state, data = hardEndStatus(t, server, "clock-holder")
	if state != "expired" || data["lease"].(LeaseRecord).State != "expired" {
		t.Fatalf("valid clock recovery at hard end = %s/%#v, want expired", state, data)
	}
}

// TestExpiryStorageFailureRollsBackWholeTransition proves an expiry write failure preserves the active lease, request, resource, and audit before a successful retry commits the complete transition.
func TestExpiryStorageFailureRollsBackWholeTransition(t *testing.T) {
	base := hardEndBaseSample()
	clock := newControlledLeaseClock(base)
	server := startHardEndServer(t, hardEndConfig(t, clock))
	addManualResource(t, server, "qa.atomic-expiry", "atomic-01")
	acquireHardEndLease(t, server, "atomic-holder", "qa.atomic-expiry", 10*time.Second)
	beforeAudit := hardEndAuditCount(t, server)
	if err := server.store.db.ExecScript("CREATE TRIGGER injected_expiry_resource_update_failure BEFORE UPDATE OF state ON resources WHEN OLD.state='leased' AND NEW.state IN ('available','disabled') BEGIN SELECT RAISE(ABORT, 'injected'); END;"); err != nil {
		t.Fatalf("install isolated expiry failure trigger: %v", err)
	}
	atHardEnd := hardEndSampleAfter(base, 10*time.Second, 0)
	clock.set(atHardEnd)
	if _, code := server.store.RunScheduler(); code != "storage_unavailable" {
		t.Fatalf("scheduler with injected expiry failure = %s, want storage_unavailable", code)
	}
	state, data := hardEndStatus(t, server, "atomic-holder")
	if state != "leased" || data["lease"].(LeaseRecord).State != "active" || data["request"].(*RequestRecord).State != "leased" || data["resource"].(ResourceRecord).State != "leased" || hardEndAuditCount(t, server) != beforeAudit {
		t.Fatalf("failed expiry was partially committed: status=%#v audit=%d before=%d", data, hardEndAuditCount(t, server), beforeAudit)
	}
	if err := server.store.db.ExecScript("DROP TRIGGER injected_expiry_resource_update_failure;"); err != nil {
		t.Fatalf("remove isolated expiry failure trigger: %v", err)
	}
	if _, code := server.store.RunScheduler(); code != "" {
		t.Fatalf("retry scheduler at hard end = %s, want success", code)
	}
	state, data = hardEndStatus(t, server, "atomic-holder")
	if state != "expired" || data["lease"].(LeaseRecord).State != "expired" || data["resource"].(ResourceRecord).State != "available" {
		t.Fatalf("expiry retry = %s/%#v, want complete expired transition", state, data)
	}
}

// TestFencingTokenIncrementsAndExhaustionDisablesResource proves every grant increments its resource token and the MAX_INT64 grant is terminal while a below-limit control still works.
func TestFencingTokenIncrementsAndExhaustionDisablesResource(t *testing.T) {
	base := hardEndBaseSample()
	clock := newControlledLeaseClock(base)
	server := startHardEndServer(t, hardEndConfig(t, clock))
	addManualResource(t, server, "qa.tokens", "token-01")
	first := acquireHardEndLease(t, server, "token-first", "qa.tokens", time.Minute)
	if first.FencingToken != 1 {
		t.Fatalf("first fencing token = %d, want 1", first.FencingToken)
	}
	if result := directRequest(t, server, "lease.release", "", map[string]any{"lease_id": first.ID}); result.State != "released" {
		t.Fatalf("release first token = %#v, want released", result)
	}
	second := acquireHardEndLease(t, server, "token-second", "qa.tokens", time.Minute)
	if second.FencingToken != 2 {
		t.Fatalf("second fencing token = %d, want 2", second.FencingToken)
	}

	addManualResource(t, server, "qa.boundary", "boundary-01")
	if _, err := server.store.db.Exec("UPDATE resources SET fencing_token=? WHERE id=?", int64(math.MaxInt64-2), "boundary-01"); err != nil {
		t.Fatalf("seed isolated below-limit token: %v", err)
	}
	belowLimit := acquireHardEndLease(t, server, "boundary-below-max", "qa.boundary", time.Minute)
	if belowLimit.FencingToken != math.MaxInt64-1 {
		t.Fatalf("below-limit token = %d, want %d", belowLimit.FencingToken, int64(math.MaxInt64-1))
	}
	if result := directRequest(t, server, "lease.release", "", map[string]any{"lease_id": belowLimit.ID}); result.State != "released" {
		t.Fatalf("release below-limit token = %#v, want released", result)
	}
	atMaximum := acquireHardEndLease(t, server, "boundary-at-max", "qa.boundary", time.Minute)
	if atMaximum.FencingToken != math.MaxInt64 {
		t.Fatalf("boundary token = %d, want MAX_INT64", atMaximum.FencingToken)
	}
	if result := directRequest(t, server, "lease.release", "", map[string]any{"lease_id": atMaximum.ID}); result.State != "released" {
		t.Fatalf("release MAX_INT64 token = %#v, want terminal release", result)
	}
	refused := directRequest(t, server, "lease.acquire", "boundary-after-max", map[string]any{"tag": "qa.boundary", "duration_ns": int64(time.Minute), "wait_mode": "no_wait"})
	status, data := hardEndStatus(t, server, "boundary-after-max")
	if refused.State != "error" || refused.Code != "pool_disabled" || status != "refused" || leaseCount(t, server, "boundary-after-max") != 0 {
		t.Fatalf("post-MAX_INT64 grant = %#v status=%s/%#v, want pool_disabled without a lease", refused, status, data)
	}
	resources, code := server.store.ServiceStatus()
	if code != "" || findResource(resources["pools"].([]PoolStatus), "boundary-01") == nil || findResource(resources["pools"].([]PoolStatus), "boundary-01").State != "disabled" {
		t.Fatalf("MAX_INT64 resource state = %#v, want disabled", resources["pools"])
	}
}

// TestCommandPoolRemainsAdapterRequired proves the first slice still refuses command execution while a manual-pool acquisition succeeds as a positive control.
func TestCommandPoolRemainsAdapterRequired(t *testing.T) {
	base := hardEndBaseSample()
	clock := newControlledLeaseClock(base)
	server := startHardEndServer(t, hardEndConfig(t, clock))
	command := directRequest(t, server, "lease.acquire", "command-refused", map[string]any{"tag": "mac.go-test", "duration_ns": int64(time.Minute), "wait_mode": "no_wait"})
	commandStatus, commandData := hardEndStatus(t, server, "command-refused")
	commandRequest, requestOK := commandData["request"].(*RequestRecord)
	if command.State != "error" || command.Code != "adapter_required" || command.RequestID != "command-refused" || leaseCount(t, server, "command-refused") != 0 || commandStatus != "refused" || !requestOK || commandRequest.State != "refused" || commandRequest.OutcomeCode == nil || *commandRequest.OutcomeCode != "adapter_required" || commandRequest.QueueSequence != nil {
		t.Fatalf("command-mode acquire = %#v, want adapter_required without a lease", command)
	}
	addManualResource(t, server, "qa.manual-control", "manual-control-01")
	manual := acquireHardEndLease(t, server, "manual-control", "qa.manual-control", time.Minute)
	if manual.State != "active" || manual.FencingToken != 1 {
		t.Fatalf("manual control grant = %#v, want active token one", manual)
	}
	status, code := server.store.ServiceStatus()
	commandResource := findResource(status["pools"].([]PoolStatus), "go-test-01")
	if code != "" || commandResource == nil || commandResource.State != "available" || commandResource.FencingToken != 0 {
		t.Fatalf("command resource after refusal = %#v, want unchanged available token zero", commandResource)
	}
}
