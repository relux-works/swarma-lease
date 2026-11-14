package lease

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"net"
	"path/filepath"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/relux-works/swarma-lease/internal/sqlite"
)

type waitObserver struct {
	conn   *net.UnixConn
	reader *bufio.Reader
}

type retryTestClock struct {
	mu      sync.Mutex
	started time.Time
	offset  time.Duration
	fail    bool
	failed  chan struct{}
}

func newRetryTestClock() *retryTestClock {
	return &retryTestClock{started: time.Now(), failed: make(chan struct{}, 8)}
}

func (c *retryTestClock) Read() (Sample, error) {
	c.mu.Lock()
	fail, offset := c.fail, c.offset
	c.mu.Unlock()
	if fail {
		select {
		case c.failed <- struct{}{}:
		default:
		}
		return Sample{}, errors.New("clock_unavailable")
	}
	elapsed := time.Since(c.started) + offset
	return Sample{Wall: c.started.Add(elapsed), MonotonicNS: elapsed.Nanoseconds(), BootSessionID: "00000000-0000-4000-8000-000000000001"}, nil
}

func (c *retryTestClock) failReads() {
	c.mu.Lock()
	c.fail = true
	c.mu.Unlock()
}

func (c *retryTestClock) recoverAndAdvance(delta time.Duration) {
	c.mu.Lock()
	c.offset += delta
	c.fail = false
	c.mu.Unlock()
}

func openQueuedObserver(t *testing.T, server *Server, op, requestID string, data map[string]any) (*waitObserver, Response) {
	t.Helper()
	conn, err := net.DialUnix("unix", nil, &net.UnixAddr{Name: server.socketPath, Net: "unix"})
	if err != nil {
		t.Fatalf("dial service for %s: %v", op, err)
	}
	observer := &waitObserver{conn: conn, reader: bufio.NewReaderSize(conn, maxFrameBytes+2)}
	t.Cleanup(func() { _ = conn.Close() })
	encoded, err := json.Marshal(WireRequest{Version: 1, Type: "request", Op: op, RequestID: requestID, Data: data})
	if err != nil {
		t.Fatalf("encode %s: %v", op, err)
	}
	if _, err := conn.Write(append(encoded, '\n')); err != nil {
		t.Fatalf("write %s: %v", op, err)
	}
	_ = conn.SetReadDeadline(time.Now().Add(3 * time.Second))
	line, err := observer.reader.ReadSlice('\n')
	if err != nil {
		t.Fatalf("read queued %s response: %v", op, err)
	}
	var response Response
	if err := json.Unmarshal(line[:len(line)-1], &response); err != nil {
		t.Fatalf("decode queued %s response: %v", op, err)
	}
	if response.State != "queued" || response.RequestID != requestID {
		t.Fatalf("queued %s response = %#v, want queued request %q", op, response, requestID)
	}
	return observer, response
}

func (o *waitObserver) readEvent(t *testing.T) TerminalEvent {
	t.Helper()
	_ = o.conn.SetReadDeadline(time.Now().Add(3 * time.Second))
	line, err := o.reader.ReadSlice('\n')
	if err != nil {
		t.Fatalf("read terminal event: %v", err)
	}
	var event TerminalEvent
	if err := json.Unmarshal(line[:len(line)-1], &event); err != nil {
		t.Fatalf("decode terminal event: %v", err)
	}
	return event
}

func directRequest(t *testing.T, server *Server, op, requestID string, data map[string]any) Response {
	t.Helper()
	message := map[string]any{"version": 1, "type": "request", "op": op, "data": data}
	if requestID != "" {
		message["request_id"] = requestID
	}
	return request(t, server, uint32(server.ownerUID), message)
}

func addManualResource(t *testing.T, server *Server, tag, id string) {
	t.Helper()
	result := directRequest(t, server, "resource.add", "", map[string]any{"tag": tag, "resource_id": id, "mode": "manual"})
	if result.State != "added" {
		t.Fatalf("add %s: %#v", id, result)
	}
}

func acquireNoWait(t *testing.T, server *Server, id, tag string) LeaseRecord {
	t.Helper()
	result := directRequest(t, server, "lease.acquire", id, map[string]any{"tag": tag, "duration_ns": int64(60 * time.Second), "wait_mode": "no_wait"})
	if result.State != "granted" {
		t.Fatalf("acquire %s: %#v", id, result)
	}
	return result.Data["lease"].(LeaseRecord)
}

func releaseLease(t *testing.T, server *Server, id string) Response {
	t.Helper()
	return directRequest(t, server, "lease.release", "", map[string]any{"lease_id": id})
}

// TestUnknownWaitAndCancelPreserveState proves missing request keys create no queue, lease, or audit effect while an existing valid lease remains active.
func TestUnknownWaitAndCancelPreserveState(t *testing.T) {
	server := newTestServer(t)
	addManualResource(t, server, "qa.pool", "qa-01")
	holder := acquireNoWait(t, server, "holder", "qa.pool")
	before := directRequest(t, server, "service.status", "", map[string]any{})
	beforeCounts := before.Data["counts"].(Counts)
	for _, operation := range []string{"lease.wait", "lease.cancel"} {
		requestID := "missing-" + operation
		result := directRequest(t, server, operation, requestID, map[string]any{})
		if result.State != "error" || result.Code != "unknown_request" || result.RequestID != requestID || result.Data["request"] != nil || result.Data["lease"] != nil || result.Data["resource"] != nil || leaseCount(t, server, requestID) != 0 {
			t.Fatalf("%s missing-key result = %#v, want unknown_request with no records", operation, result)
		}
	}
	after := directRequest(t, server, "service.status", "", map[string]any{})
	afterCounts := after.Data["counts"].(Counts)
	status := directRequest(t, server, "lease.status", "", map[string]any{"request_id": "holder"})
	lease, ok := status.Data["lease"].(LeaseRecord)
	if before.Code != "" || after.Code != "" || beforeCounts != afterCounts || afterCounts.QueuedRequests != 0 || afterCounts.ActiveLeases != 1 || status.State != "leased" || !ok || lease.ID != holder.ID || lease.ResourceID != "qa-01" {
		t.Fatalf("post-refusal state = counts %#v/%#v holder %#v, want unchanged active control", beforeCounts, afterCounts, status)
	}
}

func waitData(tag string, timeout time.Duration) map[string]any {
	return map[string]any{"tag": tag, "duration_ns": int64(60 * time.Second), "wait_mode": "wait", "wait_timeout_ns": int64(timeout)}
}

func requestSequence(t *testing.T, result map[string]any) int64 {
	t.Helper()
	switch request := result["request"].(type) {
	case map[string]any:
		if request["queue_sequence"] != nil {
			return int64(request["queue_sequence"].(float64))
		}
	case RequestRecord:
		if request.QueueSequence != nil {
			return *request.QueueSequence
		}
	case *RequestRecord:
		if request != nil && request.QueueSequence != nil {
			return *request.QueueSequence
		}
	}
	t.Fatalf("request result has no queue sequence: %#v", result)
	return 0
}

func leaseCount(t *testing.T, server *Server, requestID string) int64 {
	t.Helper()
	var count int64
	if err := server.store.db.View(func(tx *sqlite.Tx) error {
		rows, err := tx.Query("SELECT COUNT(*) AS count FROM leases WHERE request_id=?", requestID)
		if err == nil && len(rows) == 1 {
			count = rowInt(rows[0], "count")
		}
		return err
	}); err != nil {
		t.Fatalf("count leases for %s: %v", requestID, err)
	}
	return count
}

// TestExchangeWaitsForServiceGrant proves the real CLI exchange blocks after its queued acknowledgement and returns only the committed grant event without polling.
func TestExchangeWaitsForServiceGrant(t *testing.T) {
	server := newTestServer(t)
	addManualResource(t, server, "qa.pool", "qa-01")
	holder := acquireNoWait(t, server, "holder", "qa.pool")
	changed := server.store.changes()
	request := WireRequest{Version: 1, Type: "request", Op: "lease.acquire", RequestID: "exchange-wait", Data: waitData("qa.pool", time.Minute)}
	resultCh := make(chan struct {
		response Response
		code     string
	}, 1)
	go func() {
		result, code := Exchange(server.socketPath, request)
		resultCh <- struct {
			response Response
			code     string
		}{result, code}
	}()
	select {
	case <-changed:
	case <-time.After(3 * time.Second):
		t.Fatal("blocking request did not persist its queue entry")
	}
	status := directRequest(t, server, "lease.status", "", map[string]any{"request_id": request.RequestID})
	if status.State != "queued" {
		t.Fatalf("request before release = %#v, want queued", status)
	}
	if got := releaseLease(t, server, holder.ID); got.State != "released" {
		t.Fatalf("release holder = %#v", got)
	}
	select {
	case result := <-resultCh:
		if result.code != "" || result.response.State != "granted" || result.response.RequestID != request.RequestID || requestSequence(t, result.response.Data) != 1 {
			t.Fatalf("blocking exchange result = (%#v, %q), want committed grant", result.response, result.code)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("blocking exchange did not consume its terminal event")
	}
}

// TestFIFOWaitsGrantOldestAndKeepTagsIndependent proves same-tag FIFO ordering and confirms that capacity on another tag remains available.
func TestFIFOWaitsGrantOldestAndKeepTagsIndependent(t *testing.T) {
	server := newTestServer(t)
	addManualResource(t, server, "qa.pool", "qa-01")
	addManualResource(t, server, "qa.other", "other-01")
	holder := acquireNoWait(t, server, "holder", "qa.pool")
	first, firstAck := openQueuedObserver(t, server, "lease.acquire", "wait-first", waitData("qa.pool", time.Minute))
	second, secondAck := openQueuedObserver(t, server, "lease.acquire", "wait-second", waitData("qa.pool", time.Minute))
	if requestSequence(t, firstAck.Data) != 1 || requestSequence(t, secondAck.Data) != 2 {
		t.Fatalf("queue sequences = %d, %d; want 1, 2", requestSequence(t, firstAck.Data), requestSequence(t, secondAck.Data))
	}
	other := acquireNoWait(t, server, "other-tag", "qa.other")
	if other.ResourceID != "other-01" {
		t.Fatalf("independent tag used %q, want other-01", other.ResourceID)
	}
	if got := releaseLease(t, server, holder.ID); got.State != "released" {
		t.Fatalf("release holder = %#v", got)
	}
	firstEvent := first.readEvent(t)
	if firstEvent.Name != "lease.granted" || firstEvent.RequestID != "wait-first" || requestSequence(t, firstEvent.Data) != 1 {
		t.Fatalf("first terminal event = %#v", firstEvent)
	}
	secondStatus := directRequest(t, server, "lease.status", "", map[string]any{"request_id": "wait-second"})
	if secondStatus.State != "queued" || requestSequence(t, secondStatus.Data) != 2 || leaseCount(t, server, "wait-second") != 0 {
		t.Fatalf("later waiter bypassed FIFO: %#v", secondStatus)
	}
	firstLease := firstEvent.Data["lease"].(map[string]any)
	if got := releaseLease(t, server, firstLease["id"].(string)); got.State != "released" {
		t.Fatalf("release first waiter = %#v", got)
	}
	secondEvent := second.readEvent(t)
	secondLease := secondEvent.Data["lease"].(map[string]any)
	if secondEvent.Name != "lease.granted" || secondEvent.RequestID != "wait-second" || secondLease["resource_id"] != "qa-01" {
		t.Fatalf("second terminal event = %#v", secondEvent)
	}
}

// TestDisconnectPreservesRequestAndOtherObserver proves a disconnected client does not cancel a durable waiter or change the snapshot delivered to another observer.
func TestDisconnectPreservesRequestAndOtherObserver(t *testing.T) {
	server := newTestServer(t)
	addManualResource(t, server, "qa.pool", "qa-01")
	holder := acquireNoWait(t, server, "holder", "qa.pool")
	first, firstAck := openQueuedObserver(t, server, "lease.acquire", "shared-wait", waitData("qa.pool", time.Minute))
	second, secondAck := openQueuedObserver(t, server, "lease.wait", "shared-wait", map[string]any{})
	firstSequence := requestSequence(t, firstAck.Data)
	if requestSequence(t, secondAck.Data) != firstSequence {
		t.Fatalf("second observer changed queue sequence: %#v", secondAck.Data)
	}
	_ = first.conn.Close()
	status := directRequest(t, server, "lease.status", "", map[string]any{"request_id": "shared-wait"})
	if status.State != "queued" || requestSequence(t, status.Data) != firstSequence || leaseCount(t, server, "shared-wait") != 0 {
		t.Fatalf("disconnect changed queued request: %#v", status)
	}
	third, thirdAck := openQueuedObserver(t, server, "lease.wait", "shared-wait", map[string]any{})
	if requestSequence(t, thirdAck.Data) != firstSequence {
		t.Fatalf("reconnect changed queue sequence: %#v", thirdAck.Data)
	}
	if got := releaseLease(t, server, holder.ID); got.State != "released" {
		t.Fatalf("release holder = %#v", got)
	}
	event, thirdEvent := second.readEvent(t), third.readEvent(t)
	if event.Name != "lease.granted" || event.RequestID != "shared-wait" || requestSequence(t, event.Data) != firstSequence || !reflect.DeepEqual(event.Data, thirdEvent.Data) {
		t.Fatalf("observer results differ: %#v vs %#v", event, thirdEvent)
	}
	if leaseCount(t, server, "shared-wait") != 1 {
		t.Fatalf("request received %d leases, want exactly one", leaseCount(t, server, "shared-wait"))
	}
	if !reflect.DeepEqual(firstAck.Data["request"], secondAck.Data["request"]) {
		t.Fatalf("reconnect observed a different request: %#v vs %#v", firstAck.Data, secondAck.Data)
	}
}

// TestLateObserverDoesNotDuplicateTerminalEvent proves a late terminal-event subscription receives the cached snapshot without replaying it to an already served observer.
func TestLateObserverDoesNotDuplicateTerminalEvent(t *testing.T) {
	server := newTestServer(t)
	addManualResource(t, server, "qa.pool", "qa-01")
	holder := acquireNoWait(t, server, "holder", "qa.pool")
	queued := directRequest(t, server, "lease.acquire", "late-observer", waitData("qa.pool", time.Minute))
	if queued.State != "queued" {
		t.Fatalf("queue request = %#v, want queued", queued)
	}
	first, err := server.store.subscribeRequest("late-observer")
	if err != nil {
		t.Fatalf("subscribe first observer: %v", err)
	}
	defer first.cancel()
	if got := releaseLease(t, server, holder.ID); got.State != "released" {
		t.Fatalf("release holder = %#v", got)
	}
	var firstEvent TerminalEvent
	select {
	case firstEvent = <-first.ch:
	case <-time.After(3 * time.Second):
		t.Fatal("first observer did not receive the committed grant")
	}
	second, err := server.store.subscribeRequest("late-observer")
	if err != nil {
		t.Fatalf("subscribe late observer: %v", err)
	}
	defer second.cancel()
	var secondEvent TerminalEvent
	select {
	case secondEvent = <-second.ch:
	case <-time.After(3 * time.Second):
		t.Fatal("late observer did not receive the committed grant")
	}
	if firstEvent.Name != "lease.granted" || secondEvent.Name != firstEvent.Name || secondEvent.Sequence != firstEvent.Sequence || !reflect.DeepEqual(secondEvent.Data, firstEvent.Data) {
		t.Fatalf("observer event snapshots differ: %#v vs %#v", firstEvent, secondEvent)
	}
	select {
	case duplicate := <-first.ch:
		t.Fatalf("already served observer received duplicate event: %#v", duplicate)
	default:
	}
}

// TestWaitTimeoutAndLimitPreserveLeaseState proves the exact 30-minute boundary is accepted, an over-limit wait is refused before queueing, and timeout creates no lease.
func TestWaitTimeoutAndLimitPreserveLeaseState(t *testing.T) {
	server := newTestServer(t)
	addManualResource(t, server, "qa.pool", "qa-01")
	holder := acquireNoWait(t, server, "holder", "qa.pool")
	unknown := directRequest(t, server, "lease.acquire", "unknown-wait-tag", waitData("qa.missing", time.Minute))
	unknownRequest := unknown.Data["request"].(*RequestRecord)
	if unknown.Code != "unknown_tag" || unknownRequest.State != "refused" || unknownRequest.QueueSequence != nil || unknown.Data["lease"] != nil || unknown.Data["resource"] != nil {
		t.Fatalf("unknown-tag waiter = %#v, want immediate durable refusal without queue or lease", unknown)
	}
	tooLong := directRequest(t, server, "lease.acquire", "too-long", waitData("qa.pool", time.Duration(maxWaitNS)+time.Nanosecond))
	if tooLong.Code != "wait_exceeds_max" || tooLong.Data["lease"] != nil || tooLong.Data["resource"] != nil {
		t.Fatalf("over-limit waiter = %#v", tooLong)
	}
	boundary := directRequest(t, server, "lease.acquire", "boundary", waitData("qa.pool", time.Duration(maxWaitNS)))
	if boundary.State != "queued" {
		t.Fatalf("30-minute waiter = %#v, want queued", boundary)
	}
	cancelled := directRequest(t, server, "lease.cancel", "boundary", map[string]any{})
	if cancelled.State != "cancelled" {
		t.Fatalf("cancel boundary waiter = %#v", cancelled)
	}
	timeoutRequest := WireRequest{Version: 1, Type: "request", Op: "lease.acquire", RequestID: "short-wait", Data: waitData("qa.pool", 50*time.Millisecond)}
	timedOut, code := Exchange(server.socketPath, timeoutRequest)
	if code != "" || timedOut.State != "error" || timedOut.Code != "request_timed_out" || timedOut.RequestID != "short-wait" || timedOut.Data["lease"] != nil || timedOut.Data["resource"] != nil {
		t.Fatalf("timeout result = (%#v, %q)", timedOut, code)
	}
	status := directRequest(t, server, "lease.status", "", map[string]any{"request_id": "short-wait"})
	if status.State != "timed_out" || leaseCount(t, server, "short-wait") != 0 || leaseCount(t, server, "holder") != 1 {
		t.Fatalf("timeout state = %#v; holder lease count=%d", status, leaseCount(t, server, "holder"))
	}
	if got := releaseLease(t, server, holder.ID); got.State != "released" {
		t.Fatalf("release holder = %#v", got)
	}
}

// TestCancelBroadcastsAndCannotGrant proves cancellation commits before a later release, wakes every observer once, and never creates a lease for the cancelled key.
func TestCancelBroadcastsAndCannotGrant(t *testing.T) {
	server := newTestServer(t)
	addManualResource(t, server, "qa.pool", "qa-01")
	holder := acquireNoWait(t, server, "holder", "qa.pool")
	first, _ := openQueuedObserver(t, server, "lease.acquire", "cancel-me", waitData("qa.pool", time.Minute))
	second, _ := openQueuedObserver(t, server, "lease.wait", "cancel-me", map[string]any{})
	cancelled := directRequest(t, server, "lease.cancel", "cancel-me", map[string]any{})
	if cancelled.State != "cancelled" || cancelled.Code != "" {
		t.Fatalf("cancel response = %#v", cancelled)
	}
	firstEvent, secondEvent := first.readEvent(t), second.readEvent(t)
	if firstEvent.Name != "request.cancelled" || secondEvent.Name != "request.cancelled" || !reflect.DeepEqual(firstEvent.Data, secondEvent.Data) {
		t.Fatalf("cancel observer events differ: %#v vs %#v", firstEvent, secondEvent)
	}
	if got := releaseLease(t, server, holder.ID); got.State != "released" {
		t.Fatalf("release holder = %#v", got)
	}
	status := directRequest(t, server, "lease.status", "", map[string]any{"request_id": "cancel-me"})
	if status.State != "cancelled" || leaseCount(t, server, "cancel-me") != 0 {
		t.Fatalf("cancelled waiter changed after release: %#v", status)
	}
}

// TestSchedulerRetriesAfterTemporaryClockFailure proves a transient failed clock read does not strand a queued request past its original timeout.
func TestSchedulerRetriesAfterTemporaryClockFailure(t *testing.T) {
	clock := newRetryTestClock()
	root := shortTestDir(t)
	server, err := Start(Config{StateDir: filepath.Join(root, "state"), SocketPath: filepath.Join(root, "service.sock"), Clock: clock})
	if err != nil {
		t.Fatalf("start service with retry clock: %v", err)
	}
	t.Cleanup(func() {
		if err := server.Close(); err != nil {
			t.Errorf("close service: %v", err)
		}
	})
	addManualResource(t, server, "qa.pool", "qa-01")
	_ = acquireNoWait(t, server, "holder", "qa.pool")
	queued := directRequest(t, server, "lease.acquire", "clock-retry", waitData("qa.pool", 5*time.Second))
	if queued.State != "queued" {
		t.Fatalf("queue request before scheduler start = %#v, want queued", queued)
	}
	subscription, err := server.store.subscribeRequest("clock-retry")
	if err != nil {
		t.Fatalf("subscribe queued request: %v", err)
	}
	defer subscription.cancel()
	clock.failReads()
	ctx, cancel := context.WithCancel(context.Background())
	serveDone := make(chan struct{})
	go func() {
		defer close(serveDone)
		if err := server.Serve(ctx); err != nil {
			t.Errorf("serve socket: %v", err)
		}
	}()
	t.Cleanup(func() {
		cancel()
		<-serveDone
	})
	for i := 0; i < 2; i++ {
		select {
		case <-clock.failed:
		case <-time.After(time.Second):
			t.Fatal("scheduler did not observe the injected clock failure")
		}
	}
	clock.recoverAndAdvance(10 * time.Second)
	var event TerminalEvent
	select {
	case event = <-subscription.ch:
	case <-time.After(2 * time.Second):
		t.Fatal("scheduler did not retry and time out the queued request")
	}
	request, ok := event.Data["request"].(*RequestRecord)
	if event.Name != "request.timed_out" || !ok || request == nil || request.RequestID != "clock-retry" || request.State != "timed_out" || event.Data["lease"] != nil || event.Data["resource"] != nil || leaseCount(t, server, "clock-retry") != 0 {
		t.Fatalf("retry terminal event = %#v, want timeout without lease", event)
	}
}

// TestServeWaitSubscriptionFailureReturnsRecoverableUnknown proves a post-commit observation failure is not reported as rollback and the queued request remains recoverable; it injects the failure by closing only the isolated test database.
func TestServeWaitSubscriptionFailureReturnsRecoverableUnknown(t *testing.T) {
	server := newDirectTestServer(t)
	addManualResource(t, server, "qa.pool", "qa-01")
	_ = acquireNoWait(t, server, "holder", "qa.pool")
	queued := directRequest(t, server, "lease.acquire", "post-commit-subscribe", waitData("qa.pool", time.Minute))
	if queued.State != "queued" {
		t.Fatalf("queue before injected observation failure = %#v, want queued", queued)
	}
	if err := server.store.Close(); err != nil {
		t.Fatalf("close isolated store to force observation failure: %v", err)
	}

	serverConn, clientConn := net.Pipe()
	defer clientConn.Close()
	serveDone := make(chan error, 1)
	go func() {
		defer serverConn.Close()
		serveDone <- server.serveWait(serverConn, queued)
	}()
	line, err := bufio.NewReaderSize(clientConn, maxFrameBytes+2).ReadSlice('\n')
	if err != nil {
		t.Fatalf("read post-commit observation response: %v", err)
	}
	var result Response
	if err := json.Unmarshal(line[:len(line)-1], &result); err != nil {
		t.Fatalf("decode post-commit observation response: %v", err)
	}
	if result.State != "error" || result.Code != "result_unknown" || result.RequestID != "post-commit-subscribe" || len(result.Data) != 0 {
		t.Fatalf("post-commit observation response = %#v, want result_unknown with request ID and empty data", result)
	}
	if err := <-serveDone; err != nil {
		t.Fatalf("serve post-commit observation failure: %v", err)
	}
	if err := server.Close(); err != nil {
		t.Fatalf("close isolated service before recovery: %v", err)
	}

	recovered, err := Start(Config{StateDir: server.stateDir, SocketPath: server.socketPath})
	if err != nil {
		t.Fatalf("restart service after observation failure: %v", err)
	}
	t.Cleanup(func() {
		if err := recovered.Close(); err != nil {
			t.Errorf("close recovered service: %v", err)
		}
	})
	state, data, code := recovered.store.Status("post-commit-subscribe", "")
	request, ok := data["request"].(*RequestRecord)
	if code != "" || state != "queued" || !ok || request == nil || request.State != "queued" || data["lease"] != nil || data["resource"] != nil || leaseCount(t, recovered, "post-commit-subscribe") != 0 {
		t.Fatalf("recovered post-commit request = (%s, %#v, %q), want queued with no lease", state, data, code)
	}
}
