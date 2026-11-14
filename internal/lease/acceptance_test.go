package lease

import (
	"encoding/json"
	"errors"
	"math"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/relux-works/swarma-lease/internal/sqlite"
)

// TestCanonicalDatabaseFixtureSurvivesRestart proves an independently authored v1 database keeps its active lease, FIFO deadline, counter, and audit history, while a later resource addition persists across another restart.
func TestCanonicalDatabaseFixtureSurvivesRestart(t *testing.T) {
	root := shortTestDir(t)
	stateDir := filepath.Join(root, "state")
	writeCanonicalV1Fixture(t, stateDir, int64(os.Geteuid()))
	clock := newControlledLeaseClock(hardEndBaseSample())
	config := Config{StateDir: stateDir, SocketPath: filepath.Join(stateDir, "service.sock"), Clock: clock}
	server, err := Start(config)
	if err != nil {
		t.Fatalf("start from independent canonical database: %v", err)
	}

	activeState, active, code := server.store.Status("fixture-active", "")
	if code != "" || activeState != "leased" {
		t.Fatalf("fixture active request = %s/%s, want leased", activeState, code)
	}
	activeLease, ok := active["lease"].(LeaseRecord)
	if !ok || activeLease.ID != "00000000000000000000000000000001" || activeLease.FencingToken != 5 {
		t.Fatalf("fixture active lease = %#v, want token five and the fixture ID", active["lease"])
	}
	queuedState, queued, code := server.store.Status("fixture-queued", "")
	queuedRequest, requestOK := queued["request"].(*RequestRecord)
	if code != "" || queuedState != "queued" || !requestOK || queuedRequest.QueueSequence == nil || *queuedRequest.QueueSequence != 7 {
		t.Fatalf("fixture queued request = %s/%#v/%s, want original sequence seven", queuedState, queued["request"], code)
	}
	_, _, code = server.store.AddResource("qa.persistence", "persisted-01", "manual")
	if code != "" {
		t.Fatalf("add persistence control resource: %s", code)
	}
	if err := server.Close(); err != nil {
		t.Fatal(err)
	}

	restarted, err := Start(config)
	if err != nil {
		t.Fatalf("restart canonical database: %v", err)
	}
	defer restarted.Close()
	queuedState, queued, code = restarted.store.Status("fixture-queued", "")
	queuedRequest, requestOK = queued["request"].(*RequestRecord)
	if code != "" || queuedState != "queued" || !requestOK || queuedRequest.QueueSequence == nil || *queuedRequest.QueueSequence != 7 || queuedRequest.WaitDeadline == nil || *queuedRequest.WaitDeadline != "2026-11-14T00:00:20.000000000Z" {
		t.Fatalf("queued deadline after restart = %s/%#v/%s, want original queue fixture", queuedState, queued["request"], code)
	}
	resources, code := restarted.store.ServiceStatus()
	if code != "" || findResource(resources["pools"].([]PoolStatus), "persisted-01") == nil {
		t.Fatalf("resource.add did not survive restart: pools=%#v code=%s", resources["pools"], code)
	}
	rows, err := restarted.store.db.Query("SELECT COUNT(*) AS count FROM audit")
	if err != nil || len(rows) != 1 || rowInt(rows[0], "count") != 4 {
		t.Fatalf("fixture and persistence-control audit rows = %#v, %v; want four", rows, err)
	}
}

// TestInvalidCanonicalDatabasesArePreserved proves unsupported schemas and failed integrity checks refuse before changing the authoritative database or creating a socket.
func TestInvalidCanonicalDatabasesArePreserved(t *testing.T) {
	for _, tc := range []struct {
		name, mutation, want string
	}{
		{"wrong-user-version", "PRAGMA user_version=2;", "state_incompatible"},
		{"missing-index", "DROP INDEX requests_tag_queue_sequence;", "state_incompatible"},
		{"extra-object", "CREATE TABLE unexpected(value INTEGER);", "state_incompatible"},
		{"integrity-check", "PRAGMA ignore_check_constraints=ON; UPDATE pools SET next_queue_sequence=0 WHERE tag='qa.fixture'; PRAGMA ignore_check_constraints=OFF;", "state_corrupt"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			root := shortTestDir(t)
			stateDir := filepath.Join(root, "state")
			dbPath := writeCanonicalV1Fixture(t, stateDir, int64(os.Geteuid()))
			db, err := sqlite.Open(dbPath)
			if err != nil {
				t.Fatal(err)
			}
			if err := db.ExecScript(tc.mutation); err != nil {
				_ = db.Close()
				t.Fatalf("prepare isolated invalid fixture: %v", err)
			}
			if err := db.Close(); err != nil {
				t.Fatal(err)
			}
			before, err := os.ReadFile(dbPath)
			if err != nil {
				t.Fatal(err)
			}
			config := Config{StateDir: stateDir, SocketPath: filepath.Join(stateDir, "service.sock"), Clock: newControlledLeaseClock(hardEndBaseSample())}
			server, err := Start(config)
			if server != nil {
				_ = server.Close()
			}
			if err == nil || err.Error() != tc.want {
				t.Fatalf("startup refusal = %v, want %s", err, tc.want)
			}
			after, readErr := os.ReadFile(dbPath)
			if readErr != nil || string(after) != string(before) {
				t.Fatalf("refusal changed the database file: read=%v equal=%v", readErr, string(after) == string(before))
			}
			if _, statErr := os.Lstat(config.SocketPath); !os.IsNotExist(statErr) {
				t.Fatalf("refused startup created a socket: %v", statErr)
			}
		})
	}
}

// TestBootstrapRefusalsPreserveOrphanSidecars proves orphan final WAL state is refused without database creation, while a clock failure before first bootstrap leaves no database or socket.
func TestBootstrapRefusalsPreserveOrphanSidecars(t *testing.T) {
	t.Run("orphan-wal", func(t *testing.T) {
		root := shortTestDir(t)
		stateDir := filepath.Join(root, "state")
		if err := os.Mkdir(stateDir, 0700); err != nil {
			t.Fatal(err)
		}
		walPath := filepath.Join(stateDir, "leases.sqlite-wal")
		if err := os.WriteFile(walPath, []byte("orphan"), 0600); err != nil {
			t.Fatal(err)
		}
		config := Config{StateDir: stateDir, SocketPath: filepath.Join(stateDir, "service.sock"), Clock: newControlledLeaseClock(hardEndBaseSample())}
		if server, err := Start(config); server != nil || err == nil || err.Error() != "state_corrupt" {
			if server != nil {
				_ = server.Close()
			}
			t.Fatalf("orphan final WAL startup = %v, want state_corrupt", err)
		}
		wal, err := os.ReadFile(walPath)
		if err != nil || string(wal) != "orphan" {
			t.Fatalf("orphan WAL changed: bytes=%q err=%v", wal, err)
		}
		if _, err := os.Stat(filepath.Join(stateDir, "leases.sqlite")); !os.IsNotExist(err) {
			t.Fatalf("orphan WAL created a database: %v", err)
		}
	})

	t.Run("clock-before-bootstrap", func(t *testing.T) {
		root := shortTestDir(t)
		stateDir := filepath.Join(root, "state")
		clock := newControlledLeaseClock(hardEndBaseSample())
		clock.fail()
		config := Config{StateDir: stateDir, SocketPath: filepath.Join(stateDir, "service.sock"), Clock: clock}
		if server, err := Start(config); server != nil || err == nil || err.Error() != "clock_unavailable" {
			if server != nil {
				_ = server.Close()
			}
			t.Fatalf("clock failure during first start = %v, want clock_unavailable", err)
		}
		if _, err := os.Stat(filepath.Join(stateDir, "leases.sqlite")); !os.IsNotExist(err) {
			t.Fatalf("clock failure created a database: %v", err)
		}
		if _, err := os.Stat(config.SocketPath); !os.IsNotExist(err) {
			t.Fatalf("clock failure created a socket: %v", err)
		}
	})
}

// TestClockFailureInsideBootstrapAndRecoveryIsAtomic proves a clock that fails after startup preflight leaves no partial first database and does not change an existing database.
func TestClockFailureInsideBootstrapAndRecoveryIsAtomic(t *testing.T) {
	t.Run("bootstrap", func(t *testing.T) {
		root := shortTestDir(t)
		stateDir := filepath.Join(root, "state")
		clock := &failAfterReadClock{sample: hardEndBaseSample(), failAt: 2}
		config := Config{StateDir: stateDir, SocketPath: filepath.Join(stateDir, "service.sock"), Clock: clock}
		server, err := Start(config)
		if server != nil {
			_ = server.Close()
		}
		if err == nil || err.Error() != "clock_unavailable" {
			t.Fatalf("bootstrap clock failure = %v, want clock_unavailable", err)
		}
		entries, err := os.ReadDir(stateDir)
		if err != nil {
			t.Fatal(err)
		}
		for _, entry := range entries {
			if entry.Name() == "leases.sqlite" || strings.HasPrefix(entry.Name(), ".leases.sqlite.init-") {
				t.Fatalf("clock failure left bootstrap artifact %q", entry.Name())
			}
		}
		if _, err := os.Lstat(config.SocketPath); !os.IsNotExist(err) {
			t.Fatalf("clock failure created socket: %v", err)
		}
	})

	t.Run("existing-database-recovery", func(t *testing.T) {
		root := shortTestDir(t)
		stateDir := filepath.Join(root, "state")
		dbPath := writeCanonicalV1Fixture(t, stateDir, int64(os.Geteuid()))
		before, err := os.ReadFile(dbPath)
		if err != nil {
			t.Fatal(err)
		}
		clock := &failAfterReadClock{sample: hardEndBaseSample(), failAt: 2}
		config := Config{StateDir: stateDir, SocketPath: filepath.Join(stateDir, "service.sock"), Clock: clock}
		server, err := Start(config)
		if server != nil {
			_ = server.Close()
		}
		if err == nil || err.Error() != "clock_unavailable" {
			t.Fatalf("recovery clock failure = %v, want clock_unavailable", err)
		}
		after, readErr := os.ReadFile(dbPath)
		if readErr != nil || string(after) != string(before) {
			t.Fatalf("recovery clock failure changed database: read=%v equal=%v", readErr, string(after) == string(before))
		}
		if _, err := os.Lstat(config.SocketPath); !os.IsNotExist(err) {
			t.Fatalf("clock failure created socket: %v", err)
		}
	})
}

type failAfterReadClock struct {
	sample Sample
	failAt int
	reads  int
}

func (c *failAfterReadClock) Read() (Sample, error) {
	c.reads++
	if c.reads == c.failAt {
		return Sample{}, errors.New("injected clock read failure")
	}
	return c.sample, nil
}

// TestWrongFieldTypesRefuseWithoutPersistence proves wrong JSON types return invalid_request with the contract's request-ID and empty-data shape before lookup, grammar or storage effects, while valid controls still work.
func TestWrongFieldTypesRefuseWithoutPersistence(t *testing.T) {
	server := newDirectTestServer(t)
	uid := uint32(server.ownerUID)
	cases := []struct {
		name, raw, requestID string
	}{
		{"acquire-tag", `{"version":1,"type":"request","op":"lease.acquire","request_id":"type-01","data":{"tag":7,"duration_ns":1,"wait_mode":"no_wait"}}`, "type-01"},
		{"acquire-duration", `{"version":1,"type":"request","op":"lease.acquire","request_id":"type-01","data":{"tag":"qa.pool","duration_ns":"1","wait_mode":"no_wait"}}`, "type-01"},
		{"acquire-mode", `{"version":1,"type":"request","op":"lease.acquire","request_id":"type-01","data":{"tag":"qa.pool","duration_ns":1,"wait_mode":7}}`, "type-01"},
		{"acquire-timeout", `{"version":1,"type":"request","op":"lease.acquire","request_id":"type-01","data":{"tag":"qa.pool","duration_ns":1,"wait_mode":"wait","wait_timeout_ns":"1"}}`, "type-01"},
		{"acquire-key-type", `{"version":1,"type":"request","op":"lease.acquire","request_id":7,"data":{"tag":"qa.pool","duration_ns":1,"wait_mode":"no_wait"}}`, ""},
		{"version-type", `{"version":"1","type":"request","op":"service.status","data":{}}`, ""},
		{"message-type", `{"version":1,"type":7,"op":"service.status","data":{}}`, ""},
		{"data-type", `{"version":1,"type":"request","op":"service.status","data":[]}`, ""},
		{"resource-tag", `{"version":1,"type":"request","op":"resource.add","data":{"tag":7,"resource_id":"qa-01","mode":"manual"}}`, ""},
		{"resource-id", `{"version":1,"type":"request","op":"resource.add","data":{"tag":"qa.pool","resource_id":7,"mode":"manual"}}`, ""},
		{"resource-mode", `{"version":1,"type":"request","op":"resource.add","data":{"tag":"qa.pool","resource_id":"qa-01","mode":true}}`, ""},
		{"disable-id", `{"version":1,"type":"request","op":"resource.disable","data":{"resource_id":7}}`, ""},
		{"recover-confirmation", `{"version":1,"type":"request","op":"resource.recover","data":{"resource_id":"missing-01","confirm_stopped":false}}`, ""},
		{"recover-confirmation-type", `{"version":1,"type":"request","op":"resource.recover","data":{"resource_id":"missing-01","confirm_stopped":"true"}}`, ""},
		{"release-id", `{"version":1,"type":"request","op":"lease.release","data":{"lease_id":7}}`, ""},
		{"status-request-id", `{"version":1,"type":"request","op":"lease.status","data":{"request_id":7}}`, ""},
		{"status-lease-id", `{"version":1,"type":"request","op":"lease.status","data":{"lease_id":7}}`, ""},
		{"audit-limit", `{"version":1,"type":"request","op":"audit.list","data":{"limit":"1"}}`, ""},
		{"audit-cursor", `{"version":1,"type":"request","op":"audit.list","data":{"limit":1,"before_sequence":"1"}}`, ""},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, _ := server.handleBytes([]byte(tc.raw), uid)
			if got.State != "error" || got.Code != "invalid_request" || got.RequestID != tc.requestID || len(got.Data) != 0 {
				t.Fatalf("wrong-type response = %#v, want invalid_request with request_id=%q and empty data", got, tc.requestID)
			}
		})
	}
	if rows := hardEndAuditCount(t, server); rows != 0 {
		t.Fatalf("wrong field types produced %d audit rows", rows)
	}
	if status := directRequest(t, server, "lease.status", "", map[string]any{"request_id": "type-01"}); status.Code != "unknown_request" {
		t.Fatalf("wrong type reserved request ID: %#v", status)
	}
	if added := directRequest(t, server, "resource.add", "", map[string]any{"tag": "qa.pool", "resource_id": "qa-01", "mode": "manual"}); added.State != "added" {
		t.Fatalf("valid resource control = %#v", added)
	}
	if grant := directRequest(t, server, "lease.acquire", "type-control", map[string]any{"tag": "qa.pool", "duration_ns": int64(time.Second), "wait_mode": "no_wait"}); grant.State != "granted" {
		t.Fatalf("valid acquisition control = %#v", grant)
	}
}

// TestRejectedInputIsNotEchoedOrPersisted proves an invalid private-looking value is absent from the reply, durable tables, and audit while a valid operation still commits.
func TestRejectedInputIsNotEchoedOrPersisted(t *testing.T) {
	server := newDirectTestServer(t)
	const marker = "private/rejected/value"
	raw := []byte(`{"version":1,"type":"request","op":"lease.acquire","request_id":"privacy-01","data":{"tag":"private/rejected/value","duration_ns":1,"wait_mode":"no_wait"}}`)
	got, _ := server.handleBytes(raw, uint32(server.ownerUID))
	encoded, err := json.Marshal(got)
	if err != nil {
		t.Fatal(err)
	}
	if got.Code != "invalid_tag" || got.RequestID != "privacy-01" || strings.Contains(string(encoded), marker) {
		t.Fatalf("rejected-value response = %s, want unreflected invalid_tag", encoded)
	}
	for _, check := range []struct {
		query string
		args  []any
	}{
		{"SELECT COUNT(*) AS count FROM requests WHERE request_id LIKE ? OR tag LIKE ? OR outcome_code LIKE ?", []any{"%" + marker + "%", "%" + marker + "%", "%" + marker + "%"}},
		{"SELECT COUNT(*) AS count FROM leases WHERE id LIKE ? OR request_id LIKE ? OR resource_id LIKE ?", []any{"%" + marker + "%", "%" + marker + "%", "%" + marker + "%"}},
		{"SELECT COUNT(*) AS count FROM resources WHERE id LIKE ? OR tag LIKE ? OR state LIKE ?", []any{"%" + marker + "%", "%" + marker + "%", "%" + marker + "%"}},
		{"SELECT COUNT(*) AS count FROM pools WHERE tag LIKE ? OR mode LIKE ?", []any{"%" + marker + "%", "%" + marker + "%"}},
		{"SELECT COUNT(*) AS count FROM audit WHERE operation LIKE ? OR request_id LIKE ? OR target_id LIKE ? OR result_code LIKE ?", []any{"%" + marker + "%", "%" + marker + "%", "%" + marker + "%", "%" + marker + "%"}},
	} {
		rows, err := server.store.db.Query(check.query, check.args...)
		if err != nil || len(rows) != 1 || rowInt(rows[0], "count") != 0 {
			t.Fatalf("rejected value persisted: query=%q rows=%#v err=%v", check.query, rows, err)
		}
	}
	if hardEndAuditCount(t, server) != 0 {
		t.Fatal("syntax refusal wrote an audit row")
	}
	if added := directRequest(t, server, "resource.add", "", map[string]any{"tag": "qa.privacy", "resource_id": "privacy-control", "mode": "manual"}); added.State != "added" {
		t.Fatalf("valid neighboring resource control = %#v", added)
	}
}

// TestQueueSequenceExhaustionPreservesFIFO proves the last representable queue position is accepted and the next waiter refuses without changing existing rows or renumbering.
func TestQueueSequenceExhaustionPreservesFIFO(t *testing.T) {
	clock := newControlledLeaseClock(hardEndBaseSample())
	server := startHardEndServer(t, hardEndConfig(t, clock))
	addManualResource(t, server, "qa.sequence", "sequence-01")
	acquireHardEndLease(t, server, "sequence-holder", "qa.sequence", time.Minute)
	first := directRequest(t, server, "lease.acquire", "sequence-first", waitData("qa.sequence", 30*time.Second))
	if first.State != "queued" {
		t.Fatalf("first waiter = %#v", first)
	}
	if _, err := server.store.db.Exec("UPDATE pools SET next_queue_sequence=? WHERE tag=?", int64(math.MaxInt64-1), "qa.sequence"); err != nil {
		t.Fatal(err)
	}
	last := directRequest(t, server, "lease.acquire", "sequence-last", waitData("qa.sequence", 30*time.Second))
	lastRequest, ok := last.Data["request"].(*RequestRecord)
	if last.State != "queued" || !ok || lastRequest.QueueSequence == nil || *lastRequest.QueueSequence != math.MaxInt64-1 {
		t.Fatalf("last representable waiter = %#v, want queued at MAX_INT64-1", last)
	}
	before := hardEndAuditCount(t, server)
	refused := directRequest(t, server, "lease.acquire", "sequence-exhausted", waitData("qa.sequence", 30*time.Second))
	request, requestOK := refused.Data["request"].(*RequestRecord)
	if refused.State != "error" || refused.Code != "queue_sequence_exhausted" || refused.RequestID != "sequence-exhausted" || !requestOK || request.State != "refused" || request.OutcomeCode == nil || *request.OutcomeCode != "queue_sequence_exhausted" {
		t.Fatalf("exhausted queue response = %#v, want durable refusal", refused)
	}
	firstState, firstData := hardEndStatus(t, server, "sequence-first")
	lastState, lastData := hardEndStatus(t, server, "sequence-last")
	firstRequest, firstOK := firstData["request"].(*RequestRecord)
	storedLastRequest, lastOK := lastData["request"].(*RequestRecord)
	if firstState != "queued" || !firstOK || firstRequest.QueueSequence == nil || *firstRequest.QueueSequence != 1 || lastState != "queued" || !lastOK || storedLastRequest.QueueSequence == nil || *storedLastRequest.QueueSequence != math.MaxInt64-1 {
		t.Fatalf("existing queue rows changed: first=%#v last=%#v", firstData["request"], lastData["request"])
	}
	rows, err := server.store.db.Query("SELECT next_queue_sequence FROM pools WHERE tag=?", "qa.sequence")
	if err != nil || len(rows) != 1 || rowInt(rows[0], "next_queue_sequence") != math.MaxInt64 || hardEndAuditCount(t, server) != before+1 {
		t.Fatalf("sequence/exhaustion effects = %#v err=%v audit=%d, want MAX_INT64 and one refusal", rows, err, hardEndAuditCount(t, server))
	}
}

// TestUnknownLookupRefusalsKeepCommittedState proves valid-format missing IDs return their exact codes and data shapes without changing a neighboring active lease or its audit history.
func TestUnknownLookupRefusalsKeepCommittedState(t *testing.T) {
	server := newDirectTestServer(t)
	addManualResource(t, server, "qa.lookup", "lookup-01")
	active := directRequest(t, server, "lease.acquire", "lookup-holder", map[string]any{"tag": "qa.lookup", "duration_ns": int64(time.Minute), "wait_mode": "no_wait"})
	if active.State != "granted" {
		t.Fatalf("valid lookup control grant = %#v", active)
	}
	lease := active.Data["lease"].(LeaseRecord)
	before := hardEndAuditCount(t, server)
	cases := []struct {
		name, op, idField, id, wantCode, responseID string
	}{
		{"request", "lease.status", "request_id", "missing-request", "unknown_request", "missing-request"},
		{"lease-lookup", "lease.status", "lease_id", "00000000000000000000000000000002", "unknown_lease", ""},
		{"lease-release", "lease.release", "lease_id", "00000000000000000000000000000002", "unknown_lease", ""},
		{"resource-disable", "resource.disable", "resource_id", "missing-resource", "unknown_resource", ""},
		{"resource-enable", "resource.enable", "resource_id", "missing-resource", "unknown_resource", ""},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := directRequest(t, server, tc.op, "", map[string]any{tc.idField: tc.id})
			if got.State != "error" || got.Code != tc.wantCode || got.RequestID != tc.responseID || got.Data == nil || len(got.Data) != 0 {
				t.Fatalf("missing-target result = %#v, want %s and request_id=%q with empty data", got, tc.wantCode, tc.responseID)
			}
		})
	}
	state, data := hardEndStatus(t, server, "lookup-holder")
	if state != "leased" || data["lease"].(LeaseRecord).ID != lease.ID || hardEndAuditCount(t, server) != before {
		t.Fatalf("unknown-target refusals changed lease or audit: state=%s data=%#v audits=%d", state, data, hardEndAuditCount(t, server))
	}
}

// TestCommandPoolRefusalFollowsFieldGrammar proves an invalid wait-mode value beats adapter policy, while a valid command-mode request is durably refused and a manual-pool control still grants.
func TestCommandPoolRefusalFollowsFieldGrammar(t *testing.T) {
	clock := newControlledLeaseClock(hardEndBaseSample())
	server := startHardEndServer(t, hardEndConfig(t, clock))
	invalid := directRequest(t, server, "lease.acquire", "command-bad-mode", map[string]any{"tag": "mac.go-test", "duration_ns": int64(time.Second), "wait_mode": "later"})
	if invalid.Code != "invalid_wait_mode" || invalid.RequestID != "command-bad-mode" || len(invalid.Data) != 0 {
		t.Fatalf("invalid mode on command pool = %#v, want syntax refusal", invalid)
	}
	if status := directRequest(t, server, "lease.status", "", map[string]any{"request_id": "command-bad-mode"}); status.Code != "unknown_request" {
		t.Fatalf("invalid mode reserved a key: %#v", status)
	}
	refused := directRequest(t, server, "lease.acquire", "command-adapter-required", map[string]any{"tag": "mac.go-test", "duration_ns": int64(time.Second), "wait_mode": "no_wait"})
	if refused.Code != "adapter_required" || refused.RequestID != "command-adapter-required" || refused.Data["request"].(*RequestRecord).State != "refused" || refused.Data["lease"] != nil {
		t.Fatalf("valid command-mode request = %#v, want persisted adapter refusal", refused)
	}
	addManualResource(t, server, "qa.command-control", "command-control-01")
	if grant := directRequest(t, server, "lease.acquire", "manual-command-control", map[string]any{"tag": "qa.command-control", "duration_ns": int64(time.Second), "wait_mode": "no_wait"}); grant.State != "granted" {
		t.Fatalf("manual-pool control = %#v, want grant", grant)
	}
}

// TestSecondServiceInstanceRefusesWithoutDisturbingOwner proves a held service lock returns instance_running while the first listener and durable state remain available.
func TestSecondServiceInstanceRefusesWithoutDisturbingOwner(t *testing.T) {
	config := hardEndConfig(t, newControlledLeaseClock(hardEndBaseSample()))
	first, err := Start(config)
	if err != nil {
		t.Fatal(err)
	}
	defer first.Close()
	before, err := os.Lstat(config.SocketPath)
	if err != nil {
		t.Fatal(err)
	}
	second, err := Start(config)
	if second != nil {
		_ = second.Close()
	}
	if err == nil || err.Error() != "instance_running" {
		t.Fatalf("second start = %v, want instance_running", err)
	}
	after, err := os.Lstat(config.SocketPath)
	if err != nil {
		t.Fatalf("second start removed the first listener path: %v", err)
	}
	if !os.SameFile(before, after) {
		t.Fatal("second start replaced the first listener inode")
	}
	if status := directRequest(t, first, "service.status", "", map[string]any{}); status.State != "ok" {
		t.Fatalf("original service after second-start refusal = %#v", status)
	}
}

// TestGrantCannotBeCancelledAfterCommit proves a cancellation that loses to a committed grant returns request_not_cancellable and leaves the exact lease and audit count intact.
func TestGrantCannotBeCancelledAfterCommit(t *testing.T) {
	server := newDirectTestServer(t)
	addManualResource(t, server, "qa.cancel-race", "cancel-race-01")
	grant := directRequest(t, server, "lease.acquire", "cancel-race-key", map[string]any{"tag": "qa.cancel-race", "duration_ns": int64(time.Minute), "wait_mode": "no_wait"})
	if grant.State != "granted" {
		t.Fatalf("grant control = %#v", grant)
	}
	lease := grant.Data["lease"].(LeaseRecord)
	before := hardEndAuditCount(t, server)
	refused := directRequest(t, server, "lease.cancel", "cancel-race-key", map[string]any{})
	request, requestOK := refused.Data["request"].(*RequestRecord)
	resultLease, leaseOK := refused.Data["lease"].(LeaseRecord)
	if refused.State != "error" || refused.Code != "request_not_cancellable" || refused.RequestID != "cancel-race-key" || !requestOK || request.State != "leased" || !leaseOK || resultLease.ID != lease.ID {
		t.Fatalf("cancel after grant = %#v, want request_not_cancellable and original lease", refused)
	}
	state, data := hardEndStatus(t, server, "cancel-race-key")
	if state != "leased" || data["lease"].(LeaseRecord).ID != lease.ID || hardEndAuditCount(t, server) != before {
		t.Fatalf("cancel-after-grant refusal changed state or audit: state=%s data=%#v audits=%d", state, data, hardEndAuditCount(t, server))
	}
}

// TestTerminalAcquireReplaysNeverReallocate proves retries after cancellation, timeout, release, and expiry return their durable terminal outcome without creating a second lease.
func TestTerminalAcquireReplaysNeverReallocate(t *testing.T) {
	base := hardEndBaseSample()
	clock := newControlledLeaseClock(base)
	server := startHardEndServer(t, hardEndConfig(t, clock))
	for _, tag := range []string{"qa.cancel-replay", "qa.timeout-replay", "qa.release-replay", "qa.expiry-replay"} {
		addManualResource(t, server, tag, strings.ReplaceAll(tag, ".", "-")+"-01")
	}
	acquire := func(id, tag string, duration, timeout time.Duration) Response {
		data := map[string]any{"tag": tag, "duration_ns": int64(duration), "wait_mode": "no_wait"}
		if timeout > 0 {
			data["wait_mode"] = "wait"
			data["wait_timeout_ns"] = int64(timeout)
		}
		return directRequest(t, server, "lease.acquire", id, data)
	}
	for _, tc := range []struct {
		name, tag, expected string
		duration, timeout   time.Duration
	}{
		{"cancelled", "qa.cancel-replay", "request_cancelled", time.Minute, time.Minute},
		{"timed-out", "qa.timeout-replay", "request_timed_out", time.Minute, time.Second},
		{"released", "qa.release-replay", "lease_released", time.Minute, 0},
		{"expired", "qa.expiry-replay", "lease_expired", 5 * time.Second, 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			id := "replay-" + tc.name
			if tc.name == "cancelled" || tc.name == "timed-out" {
				if holder := acquire("holder-"+tc.name, tc.tag, time.Minute, 0); holder.State != "granted" {
					t.Fatalf("holder grant = %#v", holder)
				}
			}
			first := acquire(id, tc.tag, tc.duration, tc.timeout)
			if tc.timeout > 0 && first.State != "queued" {
				t.Fatalf("queued first result = %#v", first)
			}
			if tc.timeout == 0 && first.State != "granted" {
				t.Fatalf("grant first result = %#v", first)
			}
			switch tc.name {
			case "cancelled":
				if cancelled := directRequest(t, server, "lease.cancel", id, map[string]any{}); cancelled.State != "cancelled" {
					t.Fatalf("cancel = %#v", cancelled)
				}
			case "timed-out":
				clock.set(hardEndSampleAfter(base, time.Second, time.Second))
				if _, code := server.store.RunScheduler(); code != "" {
					t.Fatalf("commit due timeout before replay: %s", code)
				}
			case "released":
				lease := first.Data["lease"].(LeaseRecord)
				if released := directRequest(t, server, "lease.release", "", map[string]any{"lease_id": lease.ID}); released.State != "released" {
					t.Fatalf("release = %#v", released)
				}
			case "expired":
				clock.set(hardEndSampleAfter(base, 6*time.Second, 6*time.Second))
				if _, code := server.store.RunScheduler(); code != "" {
					t.Fatalf("commit due expiry before replay: %s", code)
				}
			}
			before := hardEndAuditCount(t, server)
			replay := acquire(id, tc.tag, tc.duration, tc.timeout)
			if replay.Code != tc.expected || replay.State != "error" || replay.RequestID != id || len(replay.Data) != 3 {
				t.Fatalf("terminal acquire replay = %#v, want %s and stored request result", replay, tc.expected)
			}
			state, data := hardEndStatus(t, server, id)
			if state == "leased" || data["request"].(*RequestRecord).State == "queued" || leaseCount(t, server, id) > 1 || hardEndAuditCount(t, server) != before {
				t.Fatalf("terminal retry changed durable state: state=%s data=%#v leases=%d audit=%d", state, data, leaseCount(t, server, id), hardEndAuditCount(t, server))
			}
		})
	}
}

func writeCanonicalV1Fixture(t *testing.T, stateDir string, ownerUID int64) string {
	t.Helper()
	if err := os.Mkdir(stateDir, 0700); err != nil {
		t.Fatal(err)
	}
	dbPath := filepath.Join(stateDir, "leases.sqlite")
	file, err := os.OpenFile(dbPath, os.O_CREATE|os.O_EXCL|os.O_RDWR, 0600)
	if err != nil {
		t.Fatal(err)
	}
	if err := file.Close(); err != nil {
		t.Fatal(err)
	}
	db, err := sqlite.Open(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	schema, err := os.ReadFile(filepath.Join("testdata", "canonical-v1.sql"))
	if err != nil {
		_ = db.Close()
		t.Fatal(err)
	}
	if err := db.ExecScript(string(schema)); err != nil {
		_ = db.Close()
		t.Fatalf("create independent v1 fixture schema: %v", err)
	}
	const at = "2026-11-14T00:00:00.000000000Z"
	const waitDeadline = "2026-11-14T00:00:20.000000000Z"
	const hardEnd = "2026-11-14T00:00:10.000000000Z"
	const bootID = hardEndBootA
	statements := []struct {
		query string
		args  []any
	}{
		{"INSERT INTO settings(singleton_id,schema_version,owner_uid,max_lease_ns,max_wait_ns,boot_session_id,created_at) VALUES(1,1,?,?,?,?,?)", []any{ownerUID, maxLeaseNS, maxWaitNS, bootID, at}},
		{"INSERT INTO pools(tag,mode,next_queue_sequence,created_at) VALUES(?,?,?,?)", []any{"mac.go-test", "command", int64(1), at}},
		{"INSERT INTO pools(tag,mode,next_queue_sequence,created_at) VALUES(?,?,?,?)", []any{"mac.swift-test", "command", int64(1), at}},
		{"INSERT INTO pools(tag,mode,next_queue_sequence,created_at) VALUES(?,?,?,?)", []any{"qa.fixture", "manual", int64(8), at}},
		{"INSERT INTO resources(id,tag,state,fencing_token,created_at,updated_at) VALUES(?,?, 'available',0,?,?)", []any{"go-test-01", "mac.go-test", at, at}},
		{"INSERT INTO resources(id,tag,state,fencing_token,created_at,updated_at) VALUES(?,?, 'available',0,?,?)", []any{"swift-test-01", "mac.swift-test", at, at}},
		{"INSERT INTO resources(id,tag,state,fencing_token,created_at,updated_at) VALUES(?,?, 'leased',5,?,?)", []any{"qa-fixture-01", "qa.fixture", at, at}},
		{"INSERT INTO requests(request_id,tag,duration_ns,wait_mode,wait_timeout_ns,queue_sequence,queued_at,wait_deadline_at,queue_boot_session_id,queue_started_monotonic_ns,wait_deadline_monotonic_ns,state,outcome_code,created_at,updated_at) VALUES(?,?,?, 'no_wait',NULL,NULL,NULL,NULL,NULL,NULL,NULL,'leased',NULL,?,?)", []any{"fixture-active", "qa.fixture", int64(10 * time.Second), at, at}},
		{"INSERT INTO requests(request_id,tag,duration_ns,wait_mode,wait_timeout_ns,queue_sequence,queued_at,wait_deadline_at,queue_boot_session_id,queue_started_monotonic_ns,wait_deadline_monotonic_ns,state,outcome_code,created_at,updated_at) VALUES(?,?,?, 'wait',?,7,?,?,?, ?, ?, 'queued',NULL,?,?)", []any{"fixture-queued", "qa.fixture", int64(10 * time.Second), int64(20 * time.Second), at, waitDeadline, bootID, int64(time.Second), int64(21 * time.Second), at, at}},
		{"INSERT INTO leases(id,request_id,resource_id,state,duration_ns,granted_at,hard_end_at,boot_session_id,granted_monotonic_ns,hard_end_monotonic_ns,fencing_token,ended_at,ended_resource_state,ended_fencing_token) VALUES(?,?,?,'active',?,?,?,?,?,?,?,NULL,NULL,NULL)", []any{"00000000000000000000000000000001", "fixture-active", "qa-fixture-01", int64(10 * time.Second), at, hardEnd, bootID, int64(time.Second), int64(11 * time.Second), int64(5)}},
		{"INSERT INTO audit(sequence,at,operation,request_id,target_id,result_code,from_state,to_state) VALUES(1,?,'resource.add',NULL,?,'ok',NULL,'available')", []any{at, "qa-fixture-01"}},
		{"INSERT INTO audit(sequence,at,operation,request_id,target_id,result_code,from_state,to_state) VALUES(2,?,'lease.acquire',?,?,'ok',NULL,'leased')", []any{at, "fixture-active", "fixture-active"}},
		{"INSERT INTO audit(sequence,at,operation,request_id,target_id,result_code,from_state,to_state) VALUES(3,?,'lease.acquire',?,?,'ok',NULL,'queued')", []any{at, "fixture-queued", "fixture-queued"}},
	}
	for _, statement := range statements {
		if _, err := db.Exec(statement.query, statement.args...); err != nil {
			_ = db.Close()
			t.Fatalf("populate independent v1 fixture: %v", err)
		}
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(dbPath, 0600); err != nil {
		t.Fatal(err)
	}
	return dbPath
}
