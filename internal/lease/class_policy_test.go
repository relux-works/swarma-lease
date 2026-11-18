package lease

import (
	"bytes"
	"encoding/json"
	"os"
	"reflect"
	"sort"
	"testing"
	"time"
)

func addClassPolicyResource(t *testing.T, server *Server, tag, resourceID, class string) {
	t.Helper()
	state, _, code := server.store.AddResourceWithClass(tag, class, resourceID, "manual", "resource-manager")
	if code != "" || state != "added" {
		t.Fatalf("add class resource %s = %s/%s, want added", resourceID, state, code)
	}
}

func acquireClassPolicyRequest(t *testing.T, server *Server, requestID, requesterID, tag string, duration time.Duration, waitMode string, waitTimeout time.Duration) (string, map[string]any, string) {
	t.Helper()
	var waitNS *int64
	if waitMode == "wait" {
		value := int64(waitTimeout)
		waitNS = &value
	}
	return server.store.Acquire(AcquireInput{
		RequestID:     requestID,
		RequesterID:   requesterID,
		Tag:           tag,
		DurationNS:    int64(duration),
		WaitMode:      waitMode,
		WaitTimeoutNS: waitNS,
	})
}

func classPolicyLease(t *testing.T, data map[string]any) LeaseRecord {
	t.Helper()
	lease, ok := data["lease"].(LeaseRecord)
	if !ok {
		t.Fatalf("request result lease = %#v, want lease record", data["lease"])
	}
	return lease
}

func releaseClassPolicyLease(t *testing.T, server *Server, lease LeaseRecord) {
	t.Helper()
	state, _, code := server.store.Release(lease.ID)
	if code != "" || state != "released" {
		t.Fatalf("release class lease %s = %s/%s, want released", lease.ID, state, code)
	}
}

func statusClassRequest(t *testing.T, server *Server, requestID string) (string, *RequestRecord, *LeaseRecord) {
	t.Helper()
	state, data, code := server.store.Status(requestID, "")
	if code != "" {
		t.Fatalf("status request %s = %s", requestID, code)
	}
	request, _ := data["request"].(*RequestRecord)
	leaseValue, _ := data["lease"].(LeaseRecord)
	var lease *LeaseRecord
	if data["lease"] != nil {
		lease = &leaseValue
	}
	return state, request, lease
}

// TestClassCapsDurationsAndCrossClassOverlap proves each class has one active lease across its pools, Go and JVM leases can overlap, and their duration limits remain distinct.
func TestClassCapsDurationsAndCrossClassOverlap(t *testing.T) {
	server, _ := startQueueStatusFixture(t, newControlledLeaseClock(hardEndBaseSample()))
	addClassPolicyResource(t, server, "mac.go-test-manual", "go-resource-01", "go-test")
	addClassPolicyResource(t, server, "mac.go-test-extra", "go-resource-02", "go-test")
	addClassPolicyResource(t, server, "mac.jvm-build", "jvm-resource-01", "jvm-build")
	addQueueStatusResource(t, server, "qa.generic", "generic-resource-01")

	goState, goData, goCode := acquireClassPolicyRequest(t, server, "request-go-active", "opaque:go/worker λ", "mac.go-test-manual", 20*time.Minute, "no_wait", 0)
	if goState != "granted" || goCode != "" {
		t.Fatalf("exact Go duration = %s/%s, want grant", goState, goCode)
	}
	goLease := classPolicyLease(t, goData)
	secondGoState, _, secondGoCode := acquireClassPolicyRequest(t, server, "request-go-second", "opaque:go/second", "mac.go-test-extra", 20*time.Minute, "no_wait", 0)
	if secondGoState != "error" || secondGoCode != "no_wait_busy" || leaseCount(t, server, "request-go-second") != 0 {
		t.Fatalf("second active Go lease = %s/%s, leases=%d; want busy with no lease", secondGoState, secondGoCode, leaseCount(t, server, "request-go-second"))
	}

	jvmState, jvmData, jvmCode := acquireClassPolicyRequest(t, server, "request-jvm-active", "opaque:jvm/worker β", "mac.jvm-build", 30*time.Minute, "no_wait", 0)
	if jvmState != "granted" || jvmCode != "" {
		t.Fatalf("exact JVM duration while Go is active = %s/%s, want overlapping grant", jvmState, jvmCode)
	}
	jvmLease := classPolicyLease(t, jvmData)
	if goLease.ID == jvmLease.ID || leaseCount(t, server, "request-go-active") != 1 || leaseCount(t, server, "request-jvm-active") != 1 {
		t.Fatalf("cross-class grants = %#v and %#v, want one lease in each class", goLease, jvmLease)
	}

	for _, tc := range []struct {
		requestID string
		tag       string
		duration  time.Duration
		wantCode  string
	}{
		{requestID: "request-go-over", tag: "mac.go-test-manual", duration: 20*time.Minute + time.Nanosecond, wantCode: "duration_exceeds_max"},
		{requestID: "request-jvm-over", tag: "mac.jvm-build", duration: 30*time.Minute + time.Nanosecond, wantCode: "duration_exceeds_max"},
		{requestID: "request-generic-over", tag: "qa.generic", duration: 20*time.Minute + time.Nanosecond, wantCode: "duration_exceeds_max"},
	} {
		state, _, code := acquireClassPolicyRequest(t, server, tc.requestID, "opaque:rejected/caller", tc.tag, tc.duration, "no_wait", 0)
		if state != "error" || code != tc.wantCode || leaseCount(t, server, tc.requestID) != 0 {
			t.Errorf("over-limit request %s = %s/%s leases=%d, want %s with no lease", tc.requestID, state, code, leaseCount(t, server, tc.requestID), tc.wantCode)
		}
	}
}

// TestClassQueueKeepsRequestTagResourceEligibility proves class-wide FIFO and limits do not let a request lease a resource from a different tag pool.
func TestClassQueueKeepsRequestTagResourceEligibility(t *testing.T) {
	server, _ := startQueueStatusFixture(t, newControlledLeaseClock(hardEndBaseSample()))
	addClassPolicyResource(t, server, "mac.go-test-manual", "a-go-resource", "go-test")
	addClassPolicyResource(t, server, "mac.go-test-extra", "z-go-resource", "go-test")

	holderState, holderData, holderCode := acquireClassPolicyRequest(t, server, "request-tag-holder", "opaque:tag/holder", "mac.go-test-manual", time.Minute, "no_wait", 0)
	if holderState != "granted" || holderCode != "" || classPolicyLease(t, holderData).ResourceID != "a-go-resource" {
		t.Fatalf("tag holder = %s/%s %#v, want manual pool resource", holderState, holderCode, holderData)
	}
	waiterState, _, waiterCode := acquireClassPolicyRequest(t, server, "request-tag-waiter", "opaque:tag/waiter", "mac.go-test-extra", time.Minute, "wait", time.Hour)
	if waiterState != "queued" || waiterCode != "" {
		t.Fatalf("different-tag waiter = %s/%s, want class-cap queue", waiterState, waiterCode)
	}
	releaseClassPolicyLease(t, server, classPolicyLease(t, holderData))
	state, _, lease := statusClassRequest(t, server, "request-tag-waiter")
	if state != "leased" || lease == nil || lease.ResourceID != "z-go-resource" {
		t.Fatalf("different-tag waiter result = %s/%#v, want only its own pool resource", state, lease)
	}
}

// TestClassDirectGrantDoesNotBypassUnavailableClassHead proves a no-wait request cannot overtake an older same-class waiter blocked by its tag; it also checks re-promotion and direct grants after the queue drains.
func TestClassDirectGrantDoesNotBypassUnavailableClassHead(t *testing.T) {
	server, _ := startQueueStatusFixture(t, newControlledLeaseClock(hardEndBaseSample()))
	addClassPolicyResource(t, server, "mac.go-test-manual", "a-go-resource", "go-test")
	addClassPolicyResource(t, server, "mac.go-test-extra", "z-go-resource", "go-test")

	holderState, holderData, holderCode := acquireClassPolicyRequest(t, server, "request-go-holder", "opaque:active/go", "mac.go-test-manual", time.Minute, "no_wait", 0)
	if holderState != "granted" || holderCode != "" {
		t.Fatalf("class holder = %s/%s, want grant", holderState, holderCode)
	}
	holder := classPolicyLease(t, holderData)

	waiterState, _, waiterCode := acquireClassPolicyRequest(t, server, "request-go-older-b", "opaque:queued/go-b", "mac.go-test-extra", time.Minute, "wait", time.Hour)
	if waiterState != "queued" || waiterCode != "" {
		t.Fatalf("older tag-B waiter = %s/%s, want queued", waiterState, waiterCode)
	}
	if state, _, code := server.store.SetResource("resource.disable", "z-go-resource", false, "test-caller"); state != "disabled" || code != "" {
		t.Fatalf("disable tag-B resource = %s/%s, want disabled", state, code)
	}
	releaseClassPolicyLease(t, server, holder)
	if state, request, lease := statusClassRequest(t, server, "request-go-older-b"); state != "queued" || request == nil || lease != nil {
		t.Fatalf("older tag-B waiter after holder release = %s/%#v/%#v, want queued without a lease", state, request, lease)
	}

	youngerState, _, youngerCode := acquireClassPolicyRequest(t, server, "request-go-younger-a", "opaque:queued/go-a", "mac.go-test-manual", time.Minute, "no_wait", 0)
	if youngerState != "error" || youngerCode != "no_wait_busy" || leaseCount(t, server, "request-go-younger-a") != 0 {
		t.Fatalf("younger tag-A direct request = %s/%s leases=%d, want no_wait_busy with no lease", youngerState, youngerCode, leaseCount(t, server, "request-go-younger-a"))
	}
	queue, queueCode := server.store.ClassQueueStatus("go-test")
	if queueCode != "" || queue["active_count"] != int64(0) || queue["queued_count"] != int64(1) {
		t.Fatalf("class queue after refused direct request = %#v/%s, want older waiter preserved and no active lease", queue, queueCode)
	}

	if state, _, code := server.store.SetResource("resource.enable", "z-go-resource", false, "test-caller"); state != "enabled" || code != "" {
		t.Fatalf("enable tag-B resource = %s/%s, want enabled", state, code)
	}
	state, _, lease := statusClassRequest(t, server, "request-go-older-b")
	if state != "leased" || lease == nil || lease.ResourceID != "z-go-resource" {
		t.Fatalf("older tag-B waiter after resource returns = %s/%#v, want the tag-B lease", state, lease)
	}
	releaseClassPolicyLease(t, server, *lease)

	controlState, controlData, controlCode := acquireClassPolicyRequest(t, server, "request-go-direct-control", "opaque:direct/go", "mac.go-test-manual", time.Minute, "no_wait", 0)
	if controlState != "granted" || controlCode != "" || classPolicyLease(t, controlData).ResourceID != "a-go-resource" {
		t.Fatalf("direct request after class queue drains = %s/%s %#v, want tag-A grant", controlState, controlCode, controlData)
	}
}

// TestClassDirectGrantIgnoresLowerPriorityQueueFromOtherClass proves a queued Go waiter does not block a direct JVM grant when the JVM class has no queued head.
func TestClassDirectGrantIgnoresLowerPriorityQueueFromOtherClass(t *testing.T) {
	server, _ := startQueueStatusFixture(t, newControlledLeaseClock(hardEndBaseSample()))
	addClassPolicyResource(t, server, "mac.go-test-manual", "a-go-resource", "go-test")
	addClassPolicyResource(t, server, "mac.go-test-extra", "z-go-resource", "go-test")
	addClassPolicyResource(t, server, "mac.jvm-build", "jvm-resource", "jvm-build")

	holderState, holderData, holderCode := acquireClassPolicyRequest(t, server, "request-go-holder", "opaque:active/go", "mac.go-test-manual", time.Minute, "no_wait", 0)
	if holderState != "granted" || holderCode != "" {
		t.Fatalf("Go class holder = %s/%s, want grant", holderState, holderCode)
	}
	waiterState, _, waiterCode := acquireClassPolicyRequest(t, server, "request-go-older", "opaque:queued/go", "mac.go-test-extra", time.Minute, "wait", time.Hour)
	if waiterState != "queued" || waiterCode != "" {
		t.Fatalf("Go class waiter = %s/%s, want queued", waiterState, waiterCode)
	}

	jvmState, jvmData, jvmCode := acquireClassPolicyRequest(t, server, "request-jvm-direct", "opaque:direct/jvm", "mac.jvm-build", time.Minute, "no_wait", 0)
	if jvmState != "granted" || jvmCode != "" || classPolicyLease(t, jvmData).ResourceID != "jvm-resource" {
		t.Fatalf("JVM direct request with lower-priority Go waiter = %s/%s %#v, want JVM grant", jvmState, jvmCode, jvmData)
	}
	if state, request, lease := statusClassRequest(t, server, "request-go-older"); state != "queued" || request == nil || lease != nil {
		t.Fatalf("Go waiter after JVM direct grant = %s/%#v/%#v, want preserved queue", state, request, lease)
	}
	releaseClassPolicyLease(t, server, classPolicyLease(t, holderData))
}

// TestClassQueuePriorityFIFOAndNoPreemption proves build waiters block new Go grants without changing a running Go lease, and both class queues promote in FIFO order.
func TestClassQueuePriorityFIFOAndNoPreemption(t *testing.T) {
	server, _ := startQueueStatusFixture(t, newControlledLeaseClock(hardEndBaseSample()))
	addClassPolicyResource(t, server, "mac.go-test-manual", "go-resource-01", "go-test")
	addClassPolicyResource(t, server, "mac.go-test-extra", "go-resource-02", "go-test")
	addClassPolicyResource(t, server, "mac.jvm-build", "jvm-resource-01", "jvm-build")
	addClassPolicyResource(t, server, "mac.jvm-build", "jvm-resource-02", "jvm-build")

	goState, goData, goCode := acquireClassPolicyRequest(t, server, "request-go-holder", "opaque:active/go", "mac.go-test-manual", time.Minute, "no_wait", 0)
	jvmState, jvmData, jvmCode := acquireClassPolicyRequest(t, server, "request-jvm-holder", "opaque:active/jvm", "mac.jvm-build", time.Minute, "no_wait", 0)
	if goState != "granted" || goCode != "" || jvmState != "granted" || jvmCode != "" {
		t.Fatalf("cross-class holders = Go %s/%s, JVM %s/%s; want both granted", goState, goCode, jvmState, jvmCode)
	}
	goHolder, jvmHolder := classPolicyLease(t, goData), classPolicyLease(t, jvmData)

	for _, waiter := range []struct {
		requestID string
		callerID  string
		tag       string
	}{
		{"request-go-waiter-01", "opaque:queued/go-one", "mac.go-test-manual"},
		{"request-go-waiter-02", "opaque:queued/go-two", "mac.go-test-extra"},
		{"request-jvm-waiter-01", "opaque:queued/jvm-one", "mac.jvm-build"},
		{"request-jvm-waiter-02", "opaque:queued/jvm-two", "mac.jvm-build"},
	} {
		state, _, code := acquireClassPolicyRequest(t, server, waiter.requestID, waiter.callerID, waiter.tag, time.Minute, "wait", time.Hour)
		if state != "queued" || code != "" {
			t.Fatalf("queue %s = %s/%s, want queued", waiter.requestID, state, code)
		}
	}

	state, _, currentGoLease := statusClassRequest(t, server, "request-go-holder")
	if state != "leased" || currentGoLease == nil || currentGoLease.ID != goHolder.ID || currentGoLease.HardEndAt != goHolder.HardEndAt {
		t.Fatalf("Go lease after build arrival = %s/%#v, want unchanged active lease", state, currentGoLease)
	}

	goBefore, code := server.store.ClassQueueStatus("go-test")
	if code != "" {
		t.Fatalf("Go class status before scheduling = %s", code)
	}
	goAfter, code := server.store.ClassQueueStatus("go-test")
	if code != "" || goBefore["queued_count"] != int64(2) || goBefore["active_count"] != int64(1) || goAfter["queued_count"] != int64(2) || goAfter["active_count"] != int64(1) {
		t.Fatalf("Go class snapshot = %#v then %#v/%s, want active=1 queued=2", goBefore, goAfter, code)
	}

	releaseClassPolicyLease(t, server, goHolder)
	if state, request, _ := statusClassRequest(t, server, "request-go-waiter-01"); state != "queued" || request == nil {
		t.Fatalf("Go waiter while build waits = %s/%#v, want queued", state, request)
	}
	blockedState, _, blockedCode := acquireClassPolicyRequest(t, server, "request-go-nonwaiting-priority-blocked", "opaque:priority/blocked", "mac.go-test-extra", time.Minute, "no_wait", 0)
	blockedStatus, statusCode := server.store.ClassQueueStatus("go-test")
	if blockedState != "error" || blockedCode != "no_wait_busy" || leaseCount(t, server, "request-go-nonwaiting-priority-blocked") != 0 || statusCode != "" || blockedStatus["active_count"] != int64(0) || blockedStatus["queued_count"] != int64(2) {
		t.Fatalf("non-waiting Go under queued build = %s/%s lease=%d status=%#v/%s, want refusal with queue unchanged", blockedState, blockedCode, leaseCount(t, server, "request-go-nonwaiting-priority-blocked"), blockedStatus, statusCode)
	}

	releaseClassPolicyLease(t, server, jvmHolder)
	if state, _, lease := statusClassRequest(t, server, "request-jvm-waiter-01"); state != "leased" || lease == nil {
		t.Fatalf("first build waiter after capacity opens = %s/%#v, want granted first", state, lease)
	}
	if state, _, lease := statusClassRequest(t, server, "request-jvm-waiter-02"); state != "queued" || lease != nil {
		t.Fatalf("second build waiter = %s/%#v, want queued behind first", state, lease)
	}
	if state, _, lease := statusClassRequest(t, server, "request-go-waiter-01"); state != "queued" || lease != nil {
		t.Fatalf("Go waiter while a build remains queued = %s/%#v, want queued", state, lease)
	}

	_, _, firstBuildLease := statusClassRequest(t, server, "request-jvm-waiter-01")
	if firstBuildLease == nil {
		t.Fatal("first build waiter has no lease")
	}
	releaseClassPolicyLease(t, server, *firstBuildLease)
	if state, _, lease := statusClassRequest(t, server, "request-jvm-waiter-02"); state != "leased" || lease == nil {
		t.Fatalf("second build waiter after FIFO head ends = %s/%#v, want granted", state, lease)
	}
	if state, _, lease := statusClassRequest(t, server, "request-go-waiter-01"); state != "leased" || lease == nil {
		t.Fatalf("first Go waiter after build queue drains = %s/%#v, want granted", state, lease)
	}
	if state, _, lease := statusClassRequest(t, server, "request-go-waiter-02"); state != "queued" || lease != nil {
		t.Fatalf("second Go waiter = %s/%#v, want queued behind first", state, lease)
	}
	goStatus, goCode := server.store.ClassQueueStatus("go-test")
	jvmStatus, jvmCode := server.store.ClassQueueStatus("jvm-build")
	if goCode != "" || jvmCode != "" || goStatus["active_count"] != int64(1) || jvmStatus["active_count"] != int64(1) {
		t.Fatalf("cross-class overlap after promotion = Go %#v/%s JVM %#v/%s", goStatus, goCode, jvmStatus, jvmCode)
	}
}

// TestClassQueueStatusIsExactReadOnlyAndOpaque proves class status returns only the contracted holder/requester view and does not change database bytes or audit rows.
func TestClassQueueStatusIsExactReadOnlyAndOpaque(t *testing.T) {
	server, dbPath := startQueueStatusFixture(t, newControlledLeaseClock(hardEndBaseSample()))
	addClassPolicyResource(t, server, "mac.go-test-manual", "resource-private-go", "go-test")
	activeID := "opaque:active/worker λ"
	queuedID := "opaque:queued/worker β"
	if state, _, code := acquireClassPolicyRequest(t, server, "request-private-active", activeID, "mac.go-test-manual", time.Minute, "no_wait", 0); state != "granted" || code != "" {
		t.Fatalf("class holder = %s/%s, want grant", state, code)
	}
	if state, _, code := acquireClassPolicyRequest(t, server, "request-private-queued", queuedID, "mac.go-test-manual", time.Minute, "wait", time.Hour); state != "queued" || code != "" {
		t.Fatalf("class waiter = %s/%s, want queued", state, code)
	}
	beforeBytes, err := os.ReadFile(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	beforeAudit := auditRowCount(t, server)

	data, code := server.store.ClassQueueStatus("go-test")
	afterBytes, readErr := os.ReadFile(dbPath)
	if code != "" || readErr != nil || !bytes.Equal(beforeBytes, afterBytes) || auditRowCount(t, server) != beforeAudit {
		t.Fatalf("class status read changed state: code=%s read=%v bytesEqual=%v audits=%d/%d", code, readErr, bytes.Equal(beforeBytes, afterBytes), beforeAudit, auditRowCount(t, server))
	}
	wantFields := []string{"active_count", "active_leases", "class", "oldest_queued_at", "queued_count", "queued_requests"}
	fields := make([]string, 0, len(data))
	for field := range data {
		fields = append(fields, field)
	}
	sort.Strings(fields)
	if !reflect.DeepEqual(fields, wantFields) || data["class"] != "go-test" || data["active_count"] != int64(1) || data["queued_count"] != int64(1) {
		t.Fatalf("class status = %#v, want exact class/count fields", data)
	}
	active, ok := data["active_leases"].([]ClassActiveLease)
	if !ok || len(active) != 1 || active[0].HolderID == nil || *active[0].HolderID != activeID || active[0].State != "active" {
		t.Fatalf("active lease rows = %#v, want opaque holder ID and active state", data["active_leases"])
	}
	queued, ok := data["queued_requests"].([]ClassQueuedRequest)
	if !ok || len(queued) != 1 || queued[0].RequesterID == nil || *queued[0].RequesterID != queuedID || queued[0].State != "queued" {
		t.Fatalf("queued request rows = %#v, want opaque requester ID and queued state", data["queued_requests"])
	}
	encoded, err := json.Marshal(data)
	if err != nil {
		t.Fatal(err)
	}
	var serialized map[string]json.RawMessage
	if err := json.Unmarshal(encoded, &serialized); err != nil {
		t.Fatal(err)
	}
	var activeRows []map[string]json.RawMessage
	var queuedRows []map[string]json.RawMessage
	if err := json.Unmarshal(serialized["active_leases"], &activeRows); err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(serialized["queued_requests"], &queuedRows); err != nil {
		t.Fatal(err)
	}
	assertRowFields := func(label string, row map[string]json.RawMessage, want []string) {
		t.Helper()
		fields := make([]string, 0, len(row))
		for field := range row {
			fields = append(fields, field)
		}
		sort.Strings(fields)
		if !reflect.DeepEqual(fields, want) {
			t.Errorf("%s fields = %v, want exact %v", label, fields, want)
		}
	}
	if len(activeRows) != 1 || len(queuedRows) != 1 {
		t.Fatalf("serialized class rows = active %d queued %d, want one each", len(activeRows), len(queuedRows))
	}
	assertRowFields("active lease", activeRows[0], []string{"granted_at", "hard_end_at", "holder_id", "state"})
	assertRowFields("queued request", queuedRows[0], []string{"queued_at", "requester_id", "state", "wait_deadline_at"})
	for _, forbidden := range []string{"request-private-active", "request-private-queued", "resource-private-go"} {
		if bytes.Contains(encoded, []byte(forbidden)) {
			t.Errorf("class status leaked internal identifier %q: %s", forbidden, encoded)
		}
	}
	if !ValidRequesterID(*active[0].HolderID) || !ValidRequesterID(*queued[0].RequesterID) {
		t.Fatal("class status returned a missing or invalid v3 caller ID")
	}
}

// TestClassWaitUsesFourHourBoundary proves classified waits accept exactly four hours and refuse the next nanosecond without adding a queue entry.
func TestClassWaitUsesFourHourBoundary(t *testing.T) {
	server, _ := startQueueStatusFixture(t, newControlledLeaseClock(hardEndBaseSample()))
	addClassPolicyResource(t, server, "mac.go-test-manual", "go-boundary-01", "go-test")
	if state, _, code := acquireClassPolicyRequest(t, server, "request-boundary-holder", "opaque:holder", "mac.go-test-manual", time.Minute, "no_wait", 0); state != "granted" || code != "" {
		t.Fatalf("boundary holder = %s/%s, want grant", state, code)
	}
	if state, _, code := acquireClassPolicyRequest(t, server, "request-boundary-waiter", "opaque:waiter", "mac.go-test-manual", time.Minute, "wait", 4*time.Hour); state != "queued" || code != "" {
		t.Fatalf("four-hour wait = %s/%s, want queued", state, code)
	}
	if state, _, code := acquireClassPolicyRequest(t, server, "request-over-waiter", "opaque:over-waiter", "mac.go-test-manual", time.Minute, "wait", 4*time.Hour+time.Nanosecond); state != "error" || code != "wait_exceeds_max" {
		t.Fatalf("over-limit wait = %s/%s, want wait_exceeds_max", state, code)
	}
	status, code := server.store.ClassQueueStatus("go-test")
	if code != "" || status["queued_count"] != int64(1) || status["active_count"] != int64(1) {
		t.Fatalf("class status after wait boundary = %#v/%s, want only the exact-boundary waiter", status, code)
	}
}

// TestClassConfigurationAndStatusProtocol proves class admission is allowlisted, the existing tag mapping cannot be reassigned, and class status uses an exact read-only request shape.
func TestClassConfigurationAndStatusProtocol(t *testing.T) {
	server, dbPath := startQueueStatusFixture(t, newControlledLeaseClock(hardEndBaseSample()))
	beforeBytes, err := os.ReadFile(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	beforeAudit := auditRowCount(t, server)
	for _, data := range []map[string]any{
		{"requester_id": "resource-manager", "tag": "mac.go-test-manual", "resource_id": "bad-class-map", "mode": "manual", "class": "jvm-build"},
		{"requester_id": "resource-manager", "tag": "qa.class", "resource_id": "bad-class-name", "mode": "manual", "class": "unknown"},
		{"requester_id": "resource-manager", "tag": "qa.command", "resource_id": "bad-class-mode", "mode": "command", "class": "go-test"},
	} {
		got := directRequest(t, server, "resource.add", "", data)
		if got.State != "error" || got.Code != "invalid_request" || len(got.Data) != 0 {
			t.Errorf("invalid class resource.add %#v = %#v, want empty invalid_request", data, got)
		}
	}
	afterBytes, err := os.ReadFile(dbPath)
	if err != nil || !bytes.Equal(afterBytes, beforeBytes) || auditRowCount(t, server) != beforeAudit {
		t.Fatalf("invalid class additions changed state: error=%v bytesEqual=%v audits=%d/%d", err, bytes.Equal(afterBytes, beforeBytes), beforeAudit, auditRowCount(t, server))
	}

	mapped := directRequest(t, server, "resource.add", "", map[string]any{"requester_id": "resource-manager", "tag": "mac.go-test-manual", "resource_id": "go-class-auto", "mode": "manual"})
	if mapped.State != "added" || mapped.Code != "" {
		t.Fatalf("mapped Go resource.add without class = %#v, want added", mapped)
	}
	state, data, code := acquireClassPolicyRequest(t, server, "request-go-auto-map", "opaque:auto-mapped", "mac.go-test-manual", time.Minute, "no_wait", 0)
	if state != "granted" || code != "" {
		t.Fatalf("automatically mapped Go acquire = %s/%s, want grant", state, code)
	}
	goStatus, code := server.store.ClassQueueStatus("go-test")
	active, ok := goStatus["active_leases"].([]ClassActiveLease)
	if code != "" || !ok || len(active) != 1 || active[0].HolderID == nil || *active[0].HolderID != "opaque:auto-mapped" {
		t.Fatalf("automatically mapped Go class status = %#v/%s, want exact holder ID", goStatus, code)
	}
	releaseClassPolicyLease(t, server, classPolicyLease(t, data))

	added := directRequest(t, server, "resource.add", "", map[string]any{"requester_id": "resource-manager", "tag": "mac.jvm-build", "resource_id": "jvm-class-01", "mode": "manual", "class": "jvm-build"})
	if added.State != "added" || added.Code != "" {
		t.Fatalf("class-configured resource.add = %#v, want added", added)
	}
	statusBytes, err := os.ReadFile(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	statusAudit := auditRowCount(t, server)
	status := directRequest(t, server, "queue.status", "", map[string]any{"class": "jvm-build"})
	statusAfterBytes, readErr := os.ReadFile(dbPath)
	statusData := status.Data
	if status.Code != "" || status.State != "ok" || readErr != nil || !bytes.Equal(statusBytes, statusAfterBytes) || auditRowCount(t, server) != statusAudit {
		t.Fatalf("class status result = %#v; error=%v bytesEqual=%v audits=%d/%d", status, readErr, bytes.Equal(statusBytes, statusAfterBytes), statusAudit, auditRowCount(t, server))
	}
	if statusData["class"] != "jvm-build" || statusData["active_count"] != int64(0) || statusData["queued_count"] != int64(0) || statusData["oldest_queued_at"] != (*string)(nil) {
		t.Fatalf("empty class status = %#v, want empty jvm-build snapshot", statusData)
	}
	invalidStatusBefore, err := os.ReadFile(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	invalidStatusAudit := auditRowCount(t, server)
	for _, data := range []map[string]any{
		{},
		{"class": "unknown"},
		{"class": "go-test", "tag": "mac.go-test-manual"},
		{"class": "go-test", "extra": true},
	} {
		got := directRequest(t, server, "queue.status", "", data)
		if got.State != "error" || got.Code != "invalid_request" || len(got.Data) != 0 {
			t.Errorf("invalid queue.status %#v = %#v, want empty invalid_request", data, got)
		}
	}
	invalidStatusAfter, err := os.ReadFile(dbPath)
	if err != nil || !bytes.Equal(invalidStatusBefore, invalidStatusAfter) || auditRowCount(t, server) != invalidStatusAudit {
		t.Fatalf("invalid queue.status changed state: error=%v bytesEqual=%v audits=%d/%d", err, bytes.Equal(invalidStatusBefore, invalidStatusAfter), invalidStatusAudit, auditRowCount(t, server))
	}
}
