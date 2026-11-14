package lease

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"math"
	"net"
	"os"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/relux-works/swarma-lease/internal/sqlite"
)

// Proves shutdown removes its own socket but preserves a different file at the socket pathname.
func TestReviewShutdownSocketIdentity(t *testing.T) {
	for _, replace := range []bool{false, true} {
		name := "own-socket"
		if replace {
			name = "replacement-file"
		}
		t.Run(name, func(t *testing.T) {
			s := newDirectTestServer(t)
			if replace {
				if err := os.Rename(s.socketPath, s.socketPath+".old"); err != nil {
					t.Fatal(err)
				}
				if err := os.WriteFile(s.socketPath, []byte("preserve"), 0600); err != nil {
					t.Fatal(err)
				}
			}
			if err := s.Close(); err != nil {
				t.Fatal(err)
			}
			content, err := os.ReadFile(s.socketPath)
			if replace {
				if err != nil || string(content) != "preserve" {
					t.Fatalf("replacement file was removed or changed: exists=%v", err == nil)
				}
			} else if !os.IsNotExist(err) {
				t.Fatalf("own socket remains: %v", err)
			}
		})
	}
}

// Proves startup distinguishes an OS reboot from corrupted live rows whose epoch differs from settings.
func TestReviewStartupRejectsMismatchedLiveEpoch(t *testing.T) {
	for _, object := range []string{"lease", "queue"} {
		for _, epoch := range []string{hardEndBootB, "gggggggg-gggg-gggg-gggg-gggggggggggg"} {
			t.Run(object+"/"+epoch, func(t *testing.T) {
				clock := newControlledLeaseClock(hardEndBaseSample())
				config := hardEndConfig(t, clock)
				s := startHardEndServer(t, config)
				addManualResource(t, s, "qa.epoch", "epoch-01")
				acquireHardEndLease(t, s, "epoch-holder", "qa.epoch", 10*time.Second)
				query := "UPDATE leases SET boot_session_id=? WHERE state='active'"
				if object == "queue" {
					got := directRequest(t, s, "lease.acquire", "epoch-waiter", waitData("qa.epoch", 5*time.Second))
					if got.State != "queued" {
						t.Fatal(got)
					}
					query = "UPDATE requests SET queue_boot_session_id=? WHERE state='queued'"
				}
				if _, err := s.store.db.Exec(query, epoch); err != nil {
					t.Fatal(err)
				}
				if err := s.Close(); err != nil {
					t.Fatal(err)
				}
				restarted, err := Start(config)
				if restarted != nil {
					defer restarted.Close()
				}
				if err == nil || err.Error() != "state_corrupt" {
					t.Fatalf("startup accepted inconsistent live %s epoch: err=%v", object, err)
				}
			})
		}
	}
}

// Proves rejected operation text cannot escape the fixed protocol operation allowlist.
func TestReviewUnknownOperationPrivacy(t *testing.T) {
	s := newDirectTestServer(t)
	const rejected = "private-rejected-value"
	result, _ := s.handleBytes([]byte(`{"version":1,"type":"request","op":"`+rejected+`","request_id":"req-01","data":{}}`), uint32(s.ownerUID))
	encoded, err := json.Marshal(result)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(encoded), rejected) || result.Op != "protocol" || result.Code != "unknown_operation" {
		t.Fatalf("unknown-operation refusal leaks rejected op or violates precedence: op=%q code=%q leak=%v", result.Op, result.Code, strings.Contains(string(encoded), rejected))
	}
}

// Proves protocol version takes precedence over an unknown operation and invalid acquisition key.
func TestReviewVersionRefusalPrecedence(t *testing.T) {
	s := newDirectTestServer(t)
	for _, raw := range []string{
		`{"version":2,"type":"request","op":"unknown","data":{}}`,
		`{"version":2,"type":"request","op":"lease.acquire","request_id":"!","data":{}}`,
	} {
		got, _ := s.handleBytes([]byte(raw), uint32(s.ownerUID))
		if got.Code != "unsupported_version" {
			t.Errorf("version refusal = %s, want unsupported_version", got.Code)
		}
	}
}

// Proves exact signed 64-bit counters survive response and event decoding, while out-of-range counters refuse as result_unknown.
func TestReviewCounterPrecision(t *testing.T) {
	root := shortTestDir(t)
	path := root + "/counter.sock"
	done := startLiteralPeer(t, path,
		`{"version":1,"type":"response","op":"service.status","state":"ok","data":{"above_float_precision":9007199254740993,"signed_max":9223372036854775807}}`+"\n")
	got, code := Exchange(path, WireRequest{Version: 1, Type: "request", Op: "service.status", Data: map[string]any{}})
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if code != "" {
		t.Fatal(code)
	}
	if got.Data["above_float_precision"] != json.Number("9007199254740993") || got.Data["signed_max"] != json.Number("9223372036854775807") {
		t.Fatalf("wire integer values lost precision or range: %#v", got.Data)
	}
	encoded, err := json.Marshal(got)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(encoded), `"above_float_precision":9007199254740993`) || !strings.Contains(string(encoded), `"signed_max":9223372036854775807`) {
		t.Fatalf("wire counter lost precision: %s", encoded)
	}

	badResponsePath := root + "/counter-overflow.sock"
	badDone := startLiteralPeer(t, badResponsePath,
		`{"version":1,"type":"response","op":"service.status","state":"ok","data":{"counter":9223372036854775808}}`+"\n")
	if _, code := Exchange(badResponsePath, WireRequest{Version: 1, Type: "request", Op: "service.status", Data: map[string]any{}}); code != "result_unknown" {
		t.Fatalf("out-of-range response counter code=%s, want result_unknown", code)
	}
	if err := <-badDone; err != nil {
		t.Fatal(err)
	}

	eventPath := root + "/event-counter.sock"
	eventDone := startLiteralPeer(t, eventPath,
		`{"version":1,"type":"response","op":"lease.acquire","request_id":"counter-01","state":"queued","data":{"request":{},"lease":null,"resource":null}}`+"\n",
		`{"version":1,"type":"event","name":"lease.granted","request_id":"counter-01","sequence":9223372036854775807,"data":{"request":{"request_id":"counter-01","state":"leased"},"lease":{"request_id":"counter-01","state":"active","fencing_token":9007199254740993},"resource":{}}}`+"\n")
	got, code = Exchange(eventPath, WireRequest{Version: 1, Type: "request", Op: "lease.acquire", RequestID: "counter-01", Data: map[string]any{}})
	if err := <-eventDone; err != nil {
		t.Fatal(err)
	}
	lease, ok := got.Data["lease"].(map[string]any)
	if code != "" || got.State != "granted" || !ok || lease["fencing_token"] != json.Number("9007199254740993") {
		t.Fatalf("event counter was not preserved exactly: result=%#v code=%s", got, code)
	}

	badEventPath := root + "/event-counter-overflow.sock"
	badEventDone := startLiteralPeer(t, badEventPath,
		`{"version":1,"type":"response","op":"lease.acquire","request_id":"counter-02","state":"queued","data":{"request":{},"lease":null,"resource":null}}`+"\n",
		`{"version":1,"type":"event","name":"lease.granted","request_id":"counter-02","sequence":1,"data":{"request":{"request_id":"counter-02","state":"leased"},"lease":{"request_id":"counter-02","state":"active","fencing_token":9223372036854775808},"resource":{}}}`+"\n")
	if _, code := Exchange(badEventPath, WireRequest{Version: 1, Type: "request", Op: "lease.acquire", RequestID: "counter-02", Data: map[string]any{}}); code != "result_unknown" {
		t.Fatalf("out-of-range event counter code=%s, want result_unknown", code)
	}
	if err := <-badEventDone; err != nil {
		t.Fatal(err)
	}
}

func startLiteralPeer(t *testing.T, path string, frames ...string) <-chan error {
	t.Helper()
	listener, err := net.Listen("unix", path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = listener.Close() })
	done := make(chan error, 1)
	go func() {
		conn, err := listener.Accept()
		if err != nil {
			done <- err
			return
		}
		defer conn.Close()
		_ = conn.SetDeadline(time.Now().Add(3 * time.Second))
		_, err = bufio.NewReader(conn).ReadString('\n')
		if err == nil {
			for _, frame := range frames {
				if _, err = conn.Write([]byte(frame)); err != nil {
					break
				}
			}
		}
		done <- err
	}()
	return done
}

type reviewReadClock struct {
	*controlledLeaseClock
	reads chan struct{}
}

func (c *reviewReadClock) Read() (Sample, error) {
	sample, err := c.controlledLeaseClock.Read()
	select {
	case c.reads <- struct{}{}:
	default:
	}
	return sample, err
}

// Proves a release transaction started behind a held SQLite write uses the advanced clock before considering a queued grant.
func TestReviewGrantAfterTransactionWait(t *testing.T) {
	base := hardEndBaseSample()
	clock := &reviewReadClock{controlledLeaseClock: newControlledLeaseClock(base), reads: make(chan struct{}, 20)}
	s := startHardEndServer(t, hardEndConfig(t, clock))
	addManualResource(t, s, "qa.race", "race-01")
	holder := acquireHardEndLease(t, s, "race-holder", "qa.race", 10*time.Second)
	got := directRequest(t, s, "lease.acquire", "race-waiter", waitData("qa.race", time.Second))
	if got.State != "queued" {
		t.Fatal(got)
	}
	for len(clock.reads) > 0 {
		<-clock.reads
	}
	locked, unlock, txDone := make(chan struct{}), make(chan struct{}), make(chan error, 1)
	go func() {
		txDone <- s.store.db.Transaction(func(tx *sqlite.Tx) error { close(locked); <-unlock; return nil })
	}()
	<-locked
	raw, err := json.Marshal(map[string]any{"version": 1, "type": "request", "op": "lease.release", "data": map[string]any{"lease_id": holder.ID}})
	if err != nil {
		close(unlock)
		<-txDone
		t.Fatal(err)
	}
	responseCh := make(chan Response, 1)
	attempted, continueCall := make(chan struct{}), make(chan struct{})
	var hookOnce sync.Once
	s.store.beforeClockTransactionForTest = func() {
		hookOnce.Do(func() { close(attempted); <-continueCall })
	}
	go func() { result, _ := s.handleBytes(raw, uint32(s.ownerUID)); responseCh <- result }()
	select {
	case <-attempted:
	case <-time.After(3 * time.Second):
		close(continueCall)
		close(unlock)
		<-txDone
		t.Fatal("release never reached its write transaction")
	}
	clock.set(hardEndSampleAfter(base, 2*time.Second, 2*time.Second))
	close(continueCall)
	close(unlock)
	if err := <-txDone; err != nil {
		t.Fatal(err)
	}
	result := <-responseCh
	if result.Code != "" {
		t.Fatal(result.Code)
	}
	state, _, code := s.store.Status("race-waiter", "")
	if code != "" || state != "timed_out" {
		t.Fatalf("waiter after transaction delay = %s/%s, want timed_out; grant used stale pre-lock clock", state, code)
	}
}

// Proves acquisition and the background scheduler use the advanced clock when their write transactions start behind a held SQLite write.
func TestReviewAcquireAndSchedulerAfterTransactionWait(t *testing.T) {
	for _, operation := range []string{"acquire", "scheduler"} {
		t.Run(operation, func(t *testing.T) {
			base := hardEndBaseSample()
			clock := &reviewReadClock{controlledLeaseClock: newControlledLeaseClock(base), reads: make(chan struct{}, 32)}
			s := startHardEndServer(t, hardEndConfig(t, clock))
			addManualResource(t, s, "qa.wait-lock", "lock-01")
			acquireHardEndLease(t, s, "lock-holder", "qa.wait-lock", 10*time.Second)
			if queued := directRequest(t, s, "lease.acquire", "lock-waiter", waitData("qa.wait-lock", time.Second)); queued.State != "queued" {
				t.Fatal(queued)
			}
			var subscription *requestSubscription
			if operation == "scheduler" {
				var err error
				subscription, err = s.store.subscribeRequest("lock-waiter")
				if err != nil {
					t.Fatalf("subscribe to queued request: %v", err)
				}
				defer subscription.cancel()
			}
			for len(clock.reads) > 0 {
				<-clock.reads
			}

			locked, unlock, txDone := make(chan struct{}), make(chan struct{}), make(chan error, 1)
			go func() {
				txDone <- s.store.db.Transaction(func(tx *sqlite.Tx) error { close(locked); <-unlock; return nil })
			}()
			<-locked
			attempted, continueCall := make(chan struct{}), make(chan struct{})
			var hookOnce sync.Once
			s.store.beforeClockTransactionForTest = func() {
				hookOnce.Do(func() { close(attempted); <-continueCall })
			}
			finished := make(chan struct{})
			ctx, cancel := context.WithCancel(context.Background())
			if operation == "acquire" {
				raw, err := json.Marshal(map[string]any{"version": 1, "type": "request", "op": "lease.acquire", "request_id": "lock-trigger", "data": waitData("qa.wait-lock", 5*time.Second)})
				if err != nil {
					cancel()
					close(continueCall)
					close(unlock)
					<-txDone
					t.Fatal(err)
				}
				go func() { defer close(finished); _, _ = s.handleBytes(raw, uint32(s.ownerUID)) }()
			} else {
				go func() { defer close(finished); s.runScheduler(ctx) }()
			}
			select {
			case <-attempted:
			case <-time.After(3 * time.Second):
				cancel()
				close(continueCall)
				close(unlock)
				<-txDone
				t.Fatal("operation never reached its write transaction")
			}
			clock.set(hardEndSampleAfter(base, 2*time.Second, 2*time.Second))
			close(continueCall)
			close(unlock)
			if err := <-txDone; err != nil {
				cancel()
				t.Fatal(err)
			}
			if operation == "scheduler" {
				select {
				case event := <-subscription.ch:
					if event.Name != "request.timed_out" {
						cancel()
						<-finished
						t.Fatalf("scheduler terminal event = %s, want request.timed_out", event.Name)
					}
				case <-time.After(3 * time.Second):
					cancel()
					<-finished
					t.Fatal("scheduler did not publish the waiter timeout")
				}
			} else {
				select {
				case <-finished:
				case <-time.After(3 * time.Second):
					t.Fatal("acquisition did not finish")
				}
			}
			cancel()
			if operation == "scheduler" {
				<-finished
			}
			state, _, code := s.store.Status("lock-waiter", "")
			if code != "" || state != "timed_out" {
				t.Fatalf("waiter after delayed %s = %s/%s, want timed_out", operation, state, code)
			}
		})
	}
}

// Proves the production scheduler loop handles representable deadlines near MAX_INT64 without wrapping.
func TestReviewExtremeSchedulerDeadlines(t *testing.T) {
	base := hardEndBaseSample()
	base.MonotonicNS = math.MaxInt64 - int64(20*time.Second)
	clock := newControlledLeaseClock(base)
	s := startHardEndServer(t, hardEndConfig(t, clock))
	addManualResource(t, s, "qa.extreme", "extreme-01")
	acquireHardEndLease(t, s, "extreme-holder", "qa.extreme", 5*time.Second)
	got := directRequest(t, s, "lease.acquire", "extreme-waiter", map[string]any{"tag": "qa.extreme", "duration_ns": int64(10 * time.Second), "wait_mode": "wait", "wait_timeout_ns": int64(10 * time.Second)})
	if got.State != "queued" {
		t.Fatal(got)
	}
	subscription, err := s.store.subscribeRequest("extreme-waiter")
	if err != nil {
		t.Fatal(err)
	}
	defer subscription.cancel()
	clock.set(hardEndSampleAfter(base, 5*time.Second, 5*time.Second))
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { defer close(done); s.runScheduler(ctx) }()
	defer func() { cancel(); <-done }()
	select {
	case event := <-subscription.ch:
		if event.Name != "lease.granted" {
			t.Fatalf("extreme scheduler event=%s", event.Name)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("scheduler loop did not grant at extreme deadline")
	}
	_, end, _ := hardEndLeaseClock(t, s, "extreme-waiter")
	if end != math.MaxInt64-int64(5*time.Second) {
		t.Fatalf("hard end = %d", end)
	}
	clock.set(Sample{Wall: base.Wall.Add(15 * time.Second), MonotonicNS: end, BootSessionID: base.BootSessionID})
	s.store.signalChanged()
	timeout := time.NewTimer(3 * time.Second)
	defer timeout.Stop()
	for {
		changed := s.store.changes()
		state, _ := hardEndStatus(t, s, "extreme-waiter")
		if state == "expired" {
			break
		}
		select {
		case <-changed:
		case <-timeout.C:
			t.Fatalf("scheduler loop at extreme hard end: %s", state)
		}
	}
}

// Measures overflow through the real scheduler: no wrapping or partial commit, then recovery at queue timeout.
func TestReviewSchedulerGrantOverflow(t *testing.T) {
	base := hardEndBaseSample()
	base.MonotonicNS = math.MaxInt64 - int64(10*time.Second)
	clock := newControlledLeaseClock(base)
	s := startHardEndServer(t, hardEndConfig(t, clock))
	addManualResource(t, s, "qa.overflow", "overflow-01")
	acquireHardEndLease(t, s, "overflow-holder", "qa.overflow", 5*time.Second)
	got := directRequest(t, s, "lease.acquire", "overflow-waiter", map[string]any{"tag": "qa.overflow", "duration_ns": int64(10 * time.Second), "wait_mode": "wait", "wait_timeout_ns": int64(9 * time.Second)})
	if got.State != "queued" {
		t.Fatal(got)
	}
	atEnd := hardEndSampleAfter(base, 5*time.Second, 5*time.Second)
	before := hardEndAuditCount(t, s)
	clock.set(atEnd)
	_, code := s.store.RunScheduler()
	state, _, _ := s.store.Status("overflow-holder", "")
	if code != "storage_unavailable" || state != "leased" || before != hardEndAuditCount(t, s) {
		t.Fatalf("overflow did not fail closed atomically: scheduler=%q holder=%q", code, state)
	}
	t.Log("overflow returns storage_unavailable and rolls back the due expiry until queue timeout; no storage fault was injected")
	runHardEndScheduler(t, s, hardEndSampleAfter(base, 9*time.Second, 9*time.Second))
	holder, _ := hardEndStatus(t, s, "overflow-holder")
	waiter, _ := hardEndStatus(t, s, "overflow-waiter")
	if holder != "expired" || waiter != "timed_out" {
		t.Fatalf("overflow recovery: holder=%s waiter=%s", holder, waiter)
	}
}

// Proves field type validation precedes name, duration, mode and timeout grammar refusals.
func TestReviewWrongFieldTypes(t *testing.T) {
	s := newDirectTestServer(t)
	for _, tc := range []struct{ name, data string }{
		{"tag", `{"tag":7,"duration_ns":1,"wait_mode":"no_wait"}`},
		{"duration", `{"tag":"qa.pool","duration_ns":"1","wait_mode":"no_wait"}`},
		{"mode", `{"tag":"qa.pool","duration_ns":1,"wait_mode":7}`},
		{"timeout", `{"tag":"qa.pool","duration_ns":1,"wait_mode":"wait","wait_timeout_ns":"1"}`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, _ := s.handleBytes([]byte(`{"version":1,"type":"request","op":"lease.acquire","request_id":"type-01","data":`+tc.data+`}`), uint32(s.ownerUID))
			if got.Code != "invalid_request" {
				t.Fatalf("wrong field type = %s, want invalid_request", got.Code)
			}
		})
	}
}

// Proves field-shape and field-type errors precede value grammar, malformed IDs are not echoed, and validly typed bad values retain their dedicated refusal without writes.
func TestReviewFieldTypesPrecedeValueGrammar(t *testing.T) {
	s := newDirectTestServer(t)
	beforeStatus, code := s.store.ServiceStatus()
	if code != "" {
		t.Fatalf("initial service status: %s", code)
	}
	beforeAudit := hardEndAuditCount(t, s)
	for _, tc := range []struct {
		name, raw, wantCode, wantRequestID string
	}{
		{
			name:     "top-level-extra-field-before-request-id-grammar",
			raw:      `{"version":1,"type":"request","op":"lease.acquire","request_id":"bad/id","data":{"tag":"qa.pool","duration_ns":1,"wait_mode":"no_wait"},"extra":true}`,
			wantCode: "invalid_request",
		},
		{
			name:     "top-level-type-before-request-id-grammar",
			raw:      `{"version":1,"type":7,"op":"lease.acquire","request_id":"bad/id","data":{"tag":"qa.pool","duration_ns":1,"wait_mode":"no_wait"}}`,
			wantCode: "invalid_request",
		},
		{
			name:     "data-extra-field-before-request-id-grammar",
			raw:      `{"version":1,"type":"request","op":"lease.wait","request_id":"bad/id","data":{"extra":true}}`,
			wantCode: "invalid_request",
		},
		{
			name:     "data-type-before-request-id-grammar",
			raw:      `{"version":1,"type":"request","op":"lease.acquire","request_id":"bad/id","data":{"tag":"qa.pool","duration_ns":"1","wait_mode":"no_wait"}}`,
			wantCode: "invalid_request",
		},
		{
			name:          "acquire-type-before-tag-grammar",
			raw:           `{"version":1,"type":"request","op":"lease.acquire","request_id":"type-order-acquire","data":{"tag":"qa.bad!","duration_ns":"1","wait_mode":"no_wait"}}`,
			wantCode:      "invalid_request",
			wantRequestID: "type-order-acquire",
		},
		{
			name:     "resource-add-type-before-tag-grammar",
			raw:      `{"version":1,"type":"request","op":"resource.add","data":{"tag":"qa.bad!","resource_id":7,"mode":"manual"}}`,
			wantCode: "invalid_request",
		},
		{
			name:     "recover-type-before-resource-id-grammar",
			raw:      `{"version":1,"type":"request","op":"resource.recover","data":{"resource_id":"bad!","confirm_stopped":"yes"}}`,
			wantCode: "invalid_request",
		},
		{
			name:     "request-id-grammar-after-valid-fields",
			raw:      `{"version":1,"type":"request","op":"lease.acquire","request_id":"bad/id","data":{"tag":"qa.pool","duration_ns":1,"wait_mode":"no_wait"}}`,
			wantCode: "invalid_request_id",
		},
		{
			name:          "valid-types-preserve-tag-refusal",
			raw:           `{"version":1,"type":"request","op":"lease.acquire","request_id":"type-order-control","data":{"tag":"qa.bad!","duration_ns":1,"wait_mode":"no_wait"}}`,
			wantCode:      "invalid_tag",
			wantRequestID: "type-order-control",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, _ := s.handleBytes([]byte(tc.raw), uint32(s.ownerUID))
			if got.State != "error" || got.Code != tc.wantCode || got.RequestID != tc.wantRequestID || len(got.Data) != 0 {
				t.Fatalf("response = %#v, want code=%s request_id=%q and empty data", got, tc.wantCode, tc.wantRequestID)
			}
		})
	}
	for _, id := range []string{"type-order-acquire", "type-order-control"} {
		if _, _, code := s.store.Status(id, ""); code != "unknown_request" {
			t.Fatalf("request %q persisted after refusal: %s", id, code)
		}
	}
	afterStatus, code := s.store.ServiceStatus()
	if code != "" || !reflect.DeepEqual(afterStatus, beforeStatus) || hardEndAuditCount(t, s) != beforeAudit {
		t.Fatalf("invalid fields changed service state: before=%#v after=%#v code=%q audit=%d before=%d", beforeStatus, afterStatus, code, hardEndAuditCount(t, s), beforeAudit)
	}
}

// Proves fragmented/coalesced multi-client frames, malformed-frame closure, field refusal, and privacy using independent literal fixtures.
func TestReviewWireFramingAndPrivacy(t *testing.T) {
	s := newTestServer(t)
	const valid = `{"version":1,"type":"request","op":"service.status","data":{}}`
	t.Run("fragmented-and-coalesced", func(t *testing.T) {
		conn, err := net.Dial("unix", s.socketPath)
		if err != nil {
			t.Fatal(err)
		}
		defer conn.Close()
		_ = conn.SetDeadline(time.Now().Add(3 * time.Second))
		if _, err := conn.Write([]byte(valid[:20])); err != nil {
			t.Fatal(err)
		}
		if _, err := conn.Write([]byte(valid[20:] + "\n" + valid + "\r\n")); err != nil {
			t.Fatal(err)
		}
		reader := bufio.NewReader(conn)
		for i := 0; i < 2; i++ {
			line, err := reader.ReadBytes('\n')
			if err != nil {
				t.Fatal(err)
			}
			var got map[string]json.RawMessage
			if err := json.Unmarshal(line, &got); err != nil {
				t.Fatal(err)
			}
			if len(got) != 5 || string(got["state"]) != `"ok"` || bytes.Contains(line, []byte("boot_session")) || bytes.Contains(line, []byte("monotonic")) {
				t.Fatalf("wire response shape/privacy = %s", line)
			}
		}
	})
	t.Run("two-independent-clients", func(t *testing.T) {
		first, err := net.Dial("unix", s.socketPath)
		if err != nil {
			t.Fatal(err)
		}
		defer first.Close()
		second, err := net.Dial("unix", s.socketPath)
		if err != nil {
			t.Fatal(err)
		}
		defer second.Close()
		_ = first.SetDeadline(time.Now().Add(3 * time.Second))
		_ = second.SetDeadline(time.Now().Add(3 * time.Second))
		if _, err := first.Write([]byte(valid[:17])); err != nil {
			t.Fatal(err)
		}
		if _, err := second.Write([]byte(valid + "\n" + valid + "\n")); err != nil {
			t.Fatal(err)
		}
		if _, err := first.Write([]byte(valid[17:] + "\n")); err != nil {
			t.Fatal(err)
		}
		for client, count := range map[net.Conn]int{first: 1, second: 2} {
			reader := bufio.NewReader(client)
			for i := 0; i < count; i++ {
				line, err := reader.ReadBytes('\n')
				if err != nil {
					t.Fatal(err)
				}
				var got Response
				if err := json.Unmarshal(line, &got); err != nil || got.Version != 1 || got.Type != "response" || got.Op != "service.status" || got.State != "ok" || got.Code != "" {
					t.Fatalf("independent-client response = %#v err=%v", got, err)
				}
			}
		}
	})
	for _, tc := range []struct {
		name string
		raw  []byte
		code string
	}{
		{"empty", []byte("\n"), "invalid_json"},
		{"malformed-json", []byte(`{"version":1,` + "\n"), "invalid_json"},
		{"utf8", []byte{'{', 0xff, '}', '\n'}, "invalid_json"},
		{"duplicate", []byte(`{"version":1,"type":"request","op":"service.status","data":{},"data":{"private-rejected-value":1}}` + "\n"), "invalid_json"},
		{"nested-duplicate", []byte(`{"version":1,"type":"request","op":"service.status","data":{"private-rejected-value":1,"private-rejected-value":2}}` + "\n"), "invalid_json"},
		{"large", []byte(valid + strings.Repeat(" ", maxFrameBytes+1-len(valid)) + "\n"), "frame_too_large"},
		{"exact-limit-crlf", []byte(valid + strings.Repeat(" ", maxFrameBytes-len(valid)) + "\r\n"), ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			conn, err := net.Dial("unix", s.socketPath)
			if err != nil {
				t.Fatal(err)
			}
			defer conn.Close()
			_ = conn.SetDeadline(time.Now().Add(3 * time.Second))
			if _, err := conn.Write(tc.raw); err != nil {
				t.Fatal(err)
			}
			reader := bufio.NewReader(conn)
			line, err := reader.ReadBytes('\n')
			if err != nil {
				t.Fatal(err)
			}
			var got Response
			if err := json.Unmarshal(line, &got); err != nil {
				t.Fatal(err)
			}
			if got.Code != tc.code || bytes.Contains(line, []byte("private-rejected-value")) {
				t.Fatalf("wire refusal code=%s want=%s privacy leak=%v", got.Code, tc.code, bytes.Contains(line, []byte("private-rejected-value")))
			}
			if tc.code != "" {
				if _, err := reader.ReadByte(); err == nil {
					t.Fatal("malformed connection remained open")
				}
			}
		})
	}
	t.Run("unknown-field-keeps-connection-usable", func(t *testing.T) {
		conn, err := net.Dial("unix", s.socketPath)
		if err != nil {
			t.Fatal(err)
		}
		defer conn.Close()
		_ = conn.SetDeadline(time.Now().Add(3 * time.Second))
		if _, err := conn.Write([]byte(`{"version":1,"type":"request","op":"service.status","unexpected":true,"data":{}}` + "\n" + valid + "\n")); err != nil {
			t.Fatal(err)
		}
		reader := bufio.NewReader(conn)
		firstLine, err := reader.ReadBytes('\n')
		if err != nil {
			t.Fatal(err)
		}
		var refused Response
		if err := json.Unmarshal(firstLine, &refused); err != nil || refused.Code != "invalid_request" || len(refused.Data) != 0 {
			t.Fatalf("unknown-field response = %#v err=%v", refused, err)
		}
		secondLine, err := reader.ReadBytes('\n')
		if err != nil {
			t.Fatalf("connection did not accept the next valid frame: %v", err)
		}
		var control Response
		if err := json.Unmarshal(secondLine, &control); err != nil || control.State != "ok" || control.Code != "" {
			t.Fatalf("valid frame after envelope refusal = %#v err=%v", control, err)
		}
	})
	if n := hardEndAuditCount(t, s); n != 0 {
		t.Fatalf("malformed frames wrote %d audit rows", n)
	}
}
