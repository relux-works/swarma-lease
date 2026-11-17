package lease

import (
	"bytes"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/relux-works/swarma-lease/internal/sqlite"
)

func startQueueStatusFixture(t *testing.T, clock Clock) (*Server, string) {
	t.Helper()
	stateDir := filepath.Join(shortTestDir(t), "state")
	server, err := Start(Config{StateDir: stateDir, SocketPath: filepath.Join(stateDir, "service.sock"), Clock: clock})
	if err != nil {
		t.Fatalf("start queue-status fixture: %v", err)
	}
	t.Cleanup(func() {
		if err := server.Close(); err != nil {
			t.Errorf("close queue-status fixture: %v", err)
		}
	})
	return server, filepath.Join(stateDir, "leases.sqlite")
}

func addQueueStatusResource(t *testing.T, server *Server, tag, id string) {
	t.Helper()
	state, _, code := server.store.AddResource(tag, id, "manual")
	if code != "" || state != "added" {
		t.Fatalf("add queue-status resource = %s/%s, want added", state, code)
	}
}

func acquireQueueStatusHolder(t *testing.T, server *Server, tag, requestID string) LeaseRecord {
	t.Helper()
	state, data, code := server.store.Acquire(AcquireInput{RequestID: requestID, Tag: tag, DurationNS: maxLeaseNS, WaitMode: "no_wait"})
	lease, ok := data["lease"].(LeaseRecord)
	if code != "" || state != "granted" || !ok {
		t.Fatalf("acquire holder = %s/%s/%#v, want granted lease", state, code, data)
	}
	return lease
}

func acquireQueueStatusWaiter(t *testing.T, server *Server, tag, requestID string, timeout int64) {
	t.Helper()
	state, _, code := server.store.Acquire(AcquireInput{RequestID: requestID, Tag: tag, DurationNS: maxLeaseNS, WaitMode: "wait", WaitTimeoutNS: &timeout})
	if code != "" || state != "queued" {
		t.Fatalf("acquire waiter %s = %s/%s, want queued", requestID, state, code)
	}
}

func queueStatusKeys(data map[string]any) []string {
	keys := make([]string, 0, len(data))
	for key := range data {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
}

func queueStatusCount(t *testing.T, data map[string]any, tag string, count int64, oldest any) {
	t.Helper()
	wantKeys := []string{"oldest_queued_at", "queued_count", "tag"}
	if !reflect.DeepEqual(queueStatusKeys(data), wantKeys) || data["tag"] != tag || data["queued_count"] != count || data["oldest_queued_at"] != oldest {
		t.Fatalf("queue status = %#v, want exact fields tag=%q count=%d oldest=%#v", data, tag, count, oldest)
	}
}

func requestQueueStatus(t *testing.T, server *Server, data map[string]any) Response {
	t.Helper()
	return request(t, server, uint32(server.ownerUID), map[string]any{"version": 1, "type": "request", "op": "queue.status", "data": data})
}

func queueRow(t *testing.T, server *Server, requestID string) (state string, sequence, started, deadline *int64) {
	t.Helper()
	rows, err := server.store.db.Query("SELECT state,queue_sequence,queue_started_monotonic_ns,wait_deadline_monotonic_ns FROM requests WHERE request_id=?", requestID)
	if err != nil || len(rows) != 1 {
		t.Fatalf("read queued request %s: rows=%#v error=%v", requestID, rows, err)
	}
	return rowString(rows[0], "state"), nullableInt(rows[0], "queue_sequence"), nullableInt(rows[0], "queue_started_monotonic_ns"), nullableInt(rows[0], "wait_deadline_monotonic_ns")
}

func auditRowCount(t *testing.T, server *Server) int64 {
	t.Helper()
	rows, err := server.store.db.Query("SELECT COUNT(*) AS n FROM audit")
	if err != nil || len(rows) != 1 {
		t.Fatalf("read audit count: rows=%#v error=%v", rows, err)
	}
	return rowInt(rows[0], "n")
}

// TestQueueStatusCountsCurrentRowsAndUsesTheOldestTimestamp proves exact-tag counts and oldest timestamps across pools, including the update after cancelling the oldest waiter.
func TestQueueStatusCountsCurrentRowsAndUsesTheOldestTimestamp(t *testing.T) {
	base := hardEndBaseSample()
	clock := newControlledLeaseClock(base)
	server, _ := startQueueStatusFixture(t, clock)
	addQueueStatusResource(t, server, "qa.queue", "queue-01")
	acquireQueueStatusHolder(t, server, "qa.queue", "holder-01")
	addQueueStatusResource(t, server, "qa.other", "other-01")
	acquireQueueStatusHolder(t, server, "qa.other", "other-holder")

	clock.set(hardEndSampleAfter(base, time.Second, time.Second))
	acquireQueueStatusWaiter(t, server, "qa.queue", "waiter-01", int64(time.Hour))
	clock.set(hardEndSampleAfter(base, 2*time.Second, 2*time.Second))
	acquireQueueStatusWaiter(t, server, "qa.queue", "waiter-02", int64(time.Hour))
	clock.set(hardEndSampleAfter(base, 3*time.Second, 3*time.Second))
	acquireQueueStatusWaiter(t, server, "qa.queue", "waiter-03", int64(time.Hour))
	clock.set(hardEndSampleAfter(base, 4*time.Second, 4*time.Second))
	acquireQueueStatusWaiter(t, server, "qa.other", "other-waiter", int64(time.Hour))

	data, code := server.store.QueueStatus("qa.queue")
	if code != "" {
		t.Fatalf("queue status code = %s, want success", code)
	}
	queueStatusCount(t, data, "qa.queue", 3, timestamp(base.Wall.Add(time.Second)))
	data, code = server.store.QueueStatus("qa.other")
	if code != "" {
		t.Fatalf("other-tag queue status = %s, want success", code)
	}
	queueStatusCount(t, data, "qa.other", 1, timestamp(base.Wall.Add(4*time.Second)))
	if state, _, code := server.store.Cancel("waiter-01"); code != "" || state != "cancelled" {
		t.Fatalf("cancel oldest waiter = %s/%s, want cancelled", state, code)
	}
	data, code = server.store.QueueStatus("qa.queue")
	if code != "" {
		t.Fatalf("queue status after cancelling oldest = %s, want success", code)
	}
	queueStatusCount(t, data, "qa.queue", 2, timestamp(base.Wall.Add(2*time.Second)))

	for _, tag := range []string{"qa.not-registered", "mac.go-test"} {
		data, code = server.store.QueueStatus(tag)
		if code != "" {
			t.Fatalf("empty queue status for %s = %s, want success", tag, code)
		}
		queueStatusCount(t, data, tag, 0, nil)
	}
	addQueueStatusResource(t, server, "qa.disabled", "a-held")
	addQueueStatusResource(t, server, "qa.disabled", "z-disabled")
	acquireQueueStatusHolder(t, server, "qa.disabled", "disabled-holder")
	if state, _, code := server.store.SetResource("resource.disable", "z-disabled", false); code != "" || state != "disabled" {
		t.Fatalf("disable unused pool capacity = %s/%s, want disabled", state, code)
	}
	acquireQueueStatusWaiter(t, server, "qa.disabled", "disabled-waiter", int64(time.Hour))
	data, code = server.store.QueueStatus("qa.disabled")
	if code != "" {
		t.Fatalf("disabled-pool queue status = %s, want success", code)
	}
	queueStatusCount(t, data, "qa.disabled", 1, timestamp(base.Wall.Add(4*time.Second)))
	state, _, code := server.store.Acquire(AcquireInput{RequestID: "command-check", Tag: "mac.go-test", DurationNS: maxLeaseNS, WaitMode: "no_wait"})
	if state != "error" || code != "adapter_required" {
		t.Fatalf("command-pool acquisition = %s/%s, want adapter_required", state, code)
	}
}

// TestQueueStatusDoesNotExpireRows proves the pure read shifts the oldest timestamp past an elapsed waiter without changing bytes or state, while a later real scheduler operation times it out and keeps the next waiter queued.
func TestQueueStatusDoesNotExpireRows(t *testing.T) {
	base := hardEndBaseSample()
	clock := newControlledLeaseClock(base)
	server, dbPath := startQueueStatusFixture(t, clock)
	addQueueStatusResource(t, server, "qa.expiry", "expiry-01")
	holder := acquireQueueStatusHolder(t, server, "qa.expiry", "expiry-holder")
	acquireQueueStatusWaiter(t, server, "qa.expiry", "expiry-waiter", int64(10*time.Second))
	clock.set(hardEndSampleAfter(base, time.Second, time.Second))
	acquireQueueStatusWaiter(t, server, "qa.expiry", "expiry-waiter-next", int64(time.Hour))
	_, beforeSequence, beforeStarted, beforeDeadline := queueRow(t, server, "expiry-waiter")
	beforeAudit := auditRowCount(t, server)
	clock.set(hardEndSampleAfter(base, 9*time.Second, 9*time.Second))
	data, code := server.store.QueueStatus("qa.expiry")
	if code != "" {
		t.Fatalf("pre-deadline queue status = %s, want success", code)
	}
	queueStatusCount(t, data, "qa.expiry", 2, timestamp(base.Wall))
	clock.set(hardEndSampleAfter(base, 10*time.Second, 10*time.Second))
	beforeBytes, err := os.ReadFile(dbPath)
	if err != nil {
		t.Fatal(err)
	}

	data, code = server.store.QueueStatus("qa.expiry")
	if code != "" {
		t.Fatalf("elapsed queue status = %s, want success", code)
	}
	queueStatusCount(t, data, "qa.expiry", 1, timestamp(base.Wall.Add(time.Second)))
	state, afterSequence, afterStarted, afterDeadline := queueRow(t, server, "expiry-waiter")
	afterBytes, err := os.ReadFile(dbPath)
	if err != nil || !bytes.Equal(afterBytes, beforeBytes) {
		t.Fatalf("pure read changed database bytes: error=%v equal=%v", err, bytes.Equal(afterBytes, beforeBytes))
	}
	if state != "queued" || !reflect.DeepEqual(afterSequence, beforeSequence) || !reflect.DeepEqual(afterStarted, beforeStarted) || !reflect.DeepEqual(afterDeadline, beforeDeadline) || auditRowCount(t, server) != beforeAudit {
		t.Fatalf("pure read changed queued state: state=%q seq=%v start=%v deadline=%v audits=%d", state, afterSequence, afterStarted, afterDeadline, auditRowCount(t, server))
	}
	requestState, _, code := server.store.Status("expiry-waiter", "")
	if code != "" || requestState != "queued" {
		t.Fatalf("request status after pure read = %s/%s, want queued", requestState, code)
	}

	state, _, code = server.store.Acquire(AcquireInput{RequestID: "expiry-scheduler", Tag: "qa.expiry", DurationNS: maxLeaseNS, WaitMode: "no_wait"})
	if state != "error" || code != "no_wait_busy" {
		t.Fatalf("real scheduler operation = %s/%s, want no_wait_busy while holder remains active", state, code)
	}
	requestState, requestData, code := server.store.Status("expiry-waiter", "")
	requestRecord, ok := requestData["request"].(*RequestRecord)
	if code != "" || requestState != "timed_out" || !ok || requestRecord.QueueSequence == nil || *requestRecord.QueueSequence != *beforeSequence {
		t.Fatalf("scheduler timeout result = %s/%s/%#v, want timed_out at original sequence", requestState, code, requestData)
	}
	nextState, _, code := server.store.Status("expiry-waiter-next", "")
	if code != "" || nextState != "queued" {
		t.Fatalf("next waiter after real timeout = %s/%s, want queued", nextState, code)
	}
	data, code = server.store.QueueStatus("qa.expiry")
	if code != "" {
		t.Fatalf("queue status after real timeout = %s, want success", code)
	}
	queueStatusCount(t, data, "qa.expiry", 1, timestamp(base.Wall.Add(time.Second)))
	originalWait := int64(10 * time.Second)
	state, _, code = server.store.Acquire(AcquireInput{RequestID: "expiry-waiter", Tag: "qa.expiry", DurationNS: maxLeaseNS, WaitMode: "wait", WaitTimeoutNS: &originalWait})
	if state != "error" || code != "request_timed_out" {
		t.Fatalf("replay after timeout = %s/%s, want the stored terminal outcome", state, code)
	}

	if _, _, code := server.store.Release(holder.ID); code != "" {
		t.Fatalf("release holder after timeout = %s, want success", code)
	}
	nextState, _, code = server.store.Status("expiry-waiter-next", "")
	if code != "" || nextState != "leased" {
		t.Fatalf("next waiter after holder release = %s/%s, want leased", nextState, code)
	}
}

// TestQueueStatusInvalidInputsPreserveState proves malformed tags, types, missing fields, and extra fields refuse before storage without weakening the valid empty-read control.
func TestQueueStatusInvalidInputsPreserveState(t *testing.T) {
	clock := newControlledLeaseClock(hardEndBaseSample())
	server, dbPath := startQueueStatusFixture(t, clock)
	beforeBytes, err := os.ReadFile(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	beforeAudit := auditRowCount(t, server)
	for _, data := range []map[string]any{
		{},
		{"tag": 17},
		{"tag": "bad/tag"},
		{"tag": "qa.valid", "extra": "ignored"},
	} {
		got := requestQueueStatus(t, server, data)
		if got.State != "error" || got.Code != "invalid_request" || len(got.Data) != 0 {
			t.Fatalf("queue.status with %#v = %#v, want empty invalid_request", data, got)
		}
	}
	good := requestQueueStatus(t, server, map[string]any{"tag": "qa.valid"})
	if good.State != "ok" || good.Code != "" {
		t.Fatalf("valid empty queue status = %#v, want success", good)
	}
	queueStatusCount(t, good.Data, "qa.valid", 0, nil)
	clock.fail()
	failed, code := server.store.QueueStatus("qa.valid")
	if code != "clock_unavailable" || len(failed) != 0 {
		t.Fatalf("clock failure queue status = %#v/%s, want typed refusal", failed, code)
	}
	afterBytes, err := os.ReadFile(dbPath)
	if err != nil || !bytes.Equal(afterBytes, beforeBytes) || auditRowCount(t, server) != beforeAudit {
		t.Fatalf("queue.status validation changed state: read error=%v bytesEqual=%v audits=%d", err, bytes.Equal(afterBytes, beforeBytes), auditRowCount(t, server))
	}
}

// TestFourHourWaitCapAndFIFOAfterTwoLeaseHeads proves 30-minute and exact four-hour waits remain valid, over-cap refusal creates no queue or lease, and a waiter keeps its original FIFO position through two 20-minute holders.
func TestFourHourWaitCapAndFIFOAfterTwoLeaseHeads(t *testing.T) {
	base := hardEndBaseSample()
	clock := newControlledLeaseClock(base)
	server, _ := startQueueStatusFixture(t, clock)
	addQueueStatusResource(t, server, "qa.fifo", "fifo-01")
	firstHolder := acquireQueueStatusHolder(t, server, "qa.fifo", "fifo-holder-01")
	acquireQueueStatusWaiter(t, server, "qa.fifo", "fifo-waiter-01", int64(30*time.Minute))
	acquireQueueStatusWaiter(t, server, "qa.fifo", "fifo-waiter-02", maxWaitNS)

	over := maxWaitNS + 1
	state, refusal, code := server.store.Acquire(AcquireInput{RequestID: "fifo-over-cap", Tag: "qa.fifo", DurationNS: maxLeaseNS, WaitMode: "wait", WaitTimeoutNS: &over})
	if state != "error" || code != "wait_exceeds_max" {
		t.Fatalf("over-cap wait = %s/%s, want wait_exceeds_max", state, code)
	}
	refused, ok := refusal["request"].(*RequestRecord)
	if !ok || refused.State != "refused" || stringValue(refused.OutcomeCode) != "wait_exceeds_max" || refusal["lease"] != nil {
		t.Fatalf("over-cap result = %#v, want a terminal refusal without lease", refusal)
	}
	rows, err := server.store.db.Query("SELECT COUNT(*) AS n FROM requests WHERE request_id=? AND state='queued'", "fifo-over-cap")
	if err != nil || len(rows) != 1 || rowInt(rows[0], "n") != 0 {
		t.Fatalf("over-cap queue rows = %#v, %v; want none", rows, err)
	}
	rows, err = server.store.db.Query("SELECT COUNT(*) AS n FROM leases WHERE request_id=?", "fifo-over-cap")
	if err != nil || len(rows) != 1 || rowInt(rows[0], "n") != 0 {
		t.Fatalf("over-cap leases = %#v, %v; want none", rows, err)
	}
	data, code := server.store.QueueStatus("qa.fifo")
	if code != "" {
		t.Fatalf("initial queue status = %s", code)
	}
	queueStatusCount(t, data, "qa.fifo", 2, timestamp(base.Wall))
	_, firstSequence, _, _ := queueRow(t, server, "fifo-waiter-01")
	_, targetSequence, targetStarted, targetDeadline := queueRow(t, server, "fifo-waiter-02")
	if firstSequence == nil || targetSequence == nil || *firstSequence >= *targetSequence || targetDeadline == nil || targetStarted == nil || *targetDeadline-*targetStarted != maxWaitNS {
		t.Fatalf("initial FIFO rows: first=%v target=(%v,%v,%v)", firstSequence, targetSequence, targetStarted, targetDeadline)
	}

	clock.set(hardEndSampleAfter(base, 20*time.Minute, 20*time.Minute))
	if state, _, code := server.store.Release(firstHolder.ID); code != "" || state != "expired" {
		t.Fatalf("first maximum holder end = %s/%s, want expired", state, code)
	}
	firstState, firstData, code := server.store.Status("fifo-waiter-01", "")
	firstRequest, firstRequestOK := firstData["request"].(*RequestRecord)
	firstLease, firstLeaseOK := firstData["lease"].(LeaseRecord)
	if code != "" || firstState != "leased" || !firstRequestOK || !firstLeaseOK || firstRequest.QueueSequence == nil || *firstRequest.QueueSequence != *firstSequence {
		t.Fatalf("first FIFO grant = %s/%s/%#v, want first waiter at sequence %d", firstState, code, firstData, *firstSequence)
	}
	targetState, _, code := server.store.Status("fifo-waiter-02", "")
	if code != "" || targetState != "queued" {
		t.Fatalf("second waiter after first 20-minute hold = %s/%s, want queued", targetState, code)
	}
	clock.set(hardEndSampleAfter(base, 25*time.Minute, 25*time.Minute))
	fourHourWait := maxWaitNS
	state, _, code = server.store.Acquire(AcquireInput{RequestID: "fifo-waiter-02", Tag: "qa.fifo", DurationNS: maxLeaseNS, WaitMode: "wait", WaitTimeoutNS: &fourHourWait})
	if code != "" || state != "queued" {
		t.Fatalf("same-key retry at 25 minutes = %s/%s, want existing queued request", state, code)
	}
	_, replaySequence, replayStarted, replayDeadline := queueRow(t, server, "fifo-waiter-02")
	if !reflect.DeepEqual(replaySequence, targetSequence) || !reflect.DeepEqual(replayStarted, targetStarted) || !reflect.DeepEqual(replayDeadline, targetDeadline) {
		t.Fatalf("same-key retry changed waiter position/deadline: sequence=%v start=%v deadline=%v", replaySequence, replayStarted, replayDeadline)
	}

	clock.set(hardEndSampleAfter(base, 40*time.Minute, 40*time.Minute))
	if state, _, code := server.store.Release(firstLease.ID); code != "" || state != "expired" {
		t.Fatalf("second maximum holder end = %s/%s, want expired", state, code)
	}
	targetState, targetData, code := server.store.Status("fifo-waiter-02", "")
	targetRequest, targetRequestOK := targetData["request"].(*RequestRecord)
	if code != "" || targetState != "leased" || !targetRequestOK || targetRequest.QueueSequence == nil || *targetRequest.QueueSequence != *targetSequence {
		t.Fatalf("target FIFO grant after 40 minutes = %s/%s/%#v, want original sequence %d", targetState, code, targetData, *targetSequence)
	}
}

type queueStatusSnapshotClock struct {
	sample Sample
	once   sync.Once
	write  func() error
	err    error
}

func (c *queueStatusSnapshotClock) Read() (Sample, error) {
	c.once.Do(func() { c.err = c.write() })
	if c.err != nil {
		return Sample{}, c.err
	}
	return c.sample, nil
}

// TestQueueStatusUsesOneReadSnapshot proves a queue row committed after the clock sample's snapshot begins is visible only to the next status call.
func TestQueueStatusUsesOneReadSnapshot(t *testing.T) {
	base := hardEndBaseSample()
	server, dbPath := startQueueStatusFixture(t, newControlledLeaseClock(base))
	addQueueStatusResource(t, server, "qa.snapshot", "snapshot-01")
	acquireQueueStatusHolder(t, server, "qa.snapshot", "snapshot-holder")
	clock := &queueStatusSnapshotClock{sample: base}
	clock.write = func() error {
		db, err := sqlite.Open(dbPath)
		if err != nil {
			return err
		}
		defer db.Close()
		return db.Transaction(func(tx *sqlite.Tx) error {
			if _, err := tx.Exec("UPDATE pools SET next_queue_sequence=2 WHERE tag=?", "qa.snapshot"); err != nil {
				return err
			}
			at := timestamp(base.Wall)
			deadlineAt := timestamp(base.Wall.Add(time.Minute))
			_, err := tx.Exec(`INSERT INTO requests(request_id,tag,duration_ns,wait_mode,wait_timeout_ns,queue_sequence,queued_at,wait_deadline_at,queue_boot_session_id,queue_started_monotonic_ns,wait_deadline_monotonic_ns,state,outcome_code,created_at,updated_at)
VALUES(?,?,?,?,?,?,?,?,?,?,?,'queued',NULL,?,?)`, "snapshot-waiter", "qa.snapshot", int64(10*time.Second), "wait", int64(time.Minute), int64(1), at, deadlineAt, base.BootSessionID, base.MonotonicNS, base.MonotonicNS+int64(time.Minute), at, at)
			return err
		})
	}
	server.store.clock = clock

	data, code := server.store.QueueStatus("qa.snapshot")
	if code != "" {
		t.Fatalf("first snapshot queue status = %s", code)
	}
	queueStatusCount(t, data, "qa.snapshot", 0, nil)
	data, code = server.store.QueueStatus("qa.snapshot")
	if code != "" {
		t.Fatalf("second snapshot queue status = %s", code)
	}
	queueStatusCount(t, data, "qa.snapshot", 1, timestamp(base.Wall))
}

// TestQueueStatusRejectsIncompatibleDeadlineState proves malformed same-boot deadline or boot metadata returns storage_unavailable without expiring or rewriting the queued request.
func TestQueueStatusRejectsIncompatibleDeadlineState(t *testing.T) {
	for _, tc := range []struct {
		name  string
		query string
		value any
	}{
		{"boot-mismatch", "UPDATE requests SET queue_boot_session_id=? WHERE request_id=?", hardEndBootB},
		{"deadline-mismatch", "UPDATE requests SET wait_deadline_monotonic_ns=queue_started_monotonic_ns+wait_timeout_ns-1 WHERE request_id=?", nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			base := hardEndBaseSample()
			server, dbPath := startQueueStatusFixture(t, newControlledLeaseClock(base))
			addQueueStatusResource(t, server, "qa.incompatible", "incompatible-01")
			acquireQueueStatusHolder(t, server, "qa.incompatible", "incompatible-holder")
			acquireQueueStatusWaiter(t, server, "qa.incompatible", "incompatible-waiter", int64(time.Minute))
			if tc.value != nil {
				if _, err := server.store.db.Exec(tc.query, tc.value, "incompatible-waiter"); err != nil {
					t.Fatalf("prepare incompatible boot row: %v", err)
				}
			} else if _, err := server.store.db.Exec(tc.query, "incompatible-waiter"); err != nil {
				t.Fatalf("prepare incompatible deadline row: %v", err)
			}
			beforeBytes, err := os.ReadFile(dbPath)
			if err != nil {
				t.Fatal(err)
			}
			beforeAudit := auditRowCount(t, server)
			data, code := server.store.QueueStatus("qa.incompatible")
			if code != "storage_unavailable" || len(data) != 0 {
				t.Fatalf("incompatible queue status = %#v/%s, want storage_unavailable", data, code)
			}
			afterBytes, err := os.ReadFile(dbPath)
			state, _, _, _ := queueRow(t, server, "incompatible-waiter")
			if err != nil || !bytes.Equal(afterBytes, beforeBytes) || state != "queued" || auditRowCount(t, server) != beforeAudit {
				t.Fatalf("incompatible read changed state: error=%v bytesEqual=%v state=%q audits=%d", err, bytes.Equal(afterBytes, beforeBytes), state, auditRowCount(t, server))
			}
		})
	}
}
