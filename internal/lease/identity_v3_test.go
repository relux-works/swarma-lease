package lease

import (
	"bytes"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/relux-works/swarma-lease/internal/sqlite"
)

// TestRequesterIDValidationUsesPrintableUTF8ByteLimits proves caller IDs accept printable UTF-8 up to 128 bytes and reject missing, non-printable, invalid UTF-8, and over-limit values.
func TestRequesterIDValidationUsesPrintableUTF8ByteLimits(t *testing.T) {
	for _, value := range []string{"opaque caller:/ λ", "😀", strings.Repeat("λ", 64)} {
		if !ValidRequesterID(value) {
			t.Errorf("ValidRequesterID(%q) = false, want true (bytes=%d)", value, len(value))
		}
	}
	for _, value := range []string{"", strings.Repeat("x", 129), "line\nbreak", "\u200d", string([]byte{0xff})} {
		if ValidRequesterID(value) {
			t.Errorf("ValidRequesterID(%q) = true, want false", value)
		}
	}
}

// TestRequesterIDRoundTripsThroughLeaseStatusAndAudit proves a maximum-length opaque caller ID is preserved exactly in acquire, lease, status, caller audit rows, and system grant rows.
func TestRequesterIDRoundTripsThroughLeaseStatusAndAudit(t *testing.T) {
	server := newDirectTestServer(t)
	addManualResource(t, server, "qa.identity", "identity-resource")
	addManualResource(t, server, "qa.identity", "identity-resource-escaped")
	callerID := strings.Repeat("λ", 64)
	grant := directRequest(t, server, "lease.acquire", "identity-roundtrip", map[string]any{
		"requester_id": callerID,
		"tag":          "qa.identity",
		"duration_ns":  int64(time.Minute),
		"wait_mode":    "no_wait",
	})
	request, requestOK := grant.Data["request"].(*RequestRecord)
	lease, leaseOK := grant.Data["lease"].(LeaseRecord)
	if grant.State != "granted" || !requestOK || !leaseOK || request.RequesterID == nil || *request.RequesterID != callerID || request.LegacyUnattributed || lease.HolderID == nil || *lease.HolderID != callerID || lease.LegacyUnattributed {
		t.Fatalf("grant identity = %#v, want byte-exact caller ID on a new request and lease", grant)
	}

	status := directRequest(t, server, "lease.status", "", map[string]any{"request_id": "identity-roundtrip"})
	statusRequest, statusRequestOK := status.Data["request"].(*RequestRecord)
	statusLease, statusLeaseOK := status.Data["lease"].(LeaseRecord)
	if status.Code != "" || status.State != "leased" || !statusRequestOK || !statusLeaseOK || statusRequest.RequesterID == nil || *statusRequest.RequesterID != callerID || statusLease.HolderID == nil || *statusLease.HolderID != callerID {
		t.Fatalf("status identity = %#v, want same byte-exact caller ID", status)
	}

	history, code := server.store.History(20, nil)
	entries, entriesOK := history["entries"].([]AuditEntry)
	identityRows := 0
	callerRows, systemRows := 0, 0
	if code != "" || !entriesOK {
		t.Fatalf("history result = %#v/%s, want audit entries", history, code)
	}
	for _, entry := range entries {
		if stringValue(entry.RequestID) != "identity-roundtrip" {
			continue
		}
		identityRows++
		if entry.RequesterID == nil || *entry.RequesterID != callerID || entry.HolderID == nil || *entry.HolderID != callerID || entry.LegacyUnattributed {
			t.Errorf("audit identity = %#v, want exact caller/holder ID", entry)
		}
		switch entry.ActorKind {
		case "caller":
			callerRows++
			if entry.TargetID == nil || *entry.TargetID != "identity-roundtrip" || entry.Cause != nil {
				t.Errorf("direct acquire audit row = %#v, want caller request row", entry)
			}
		case "system":
			systemRows++
			if entry.ActorComponent == nil || *entry.ActorComponent != "scheduler" || entry.Cause == nil || *entry.Cause != "grant" {
				t.Errorf("immediate grant audit row = %#v, want system grant", entry)
			}
		default:
			t.Errorf("new v3 audit row has actor_kind %q, want caller or system", entry.ActorKind)
		}
	}
	if identityRows != 3 || callerRows != 1 || systemRows != 2 {
		t.Fatalf("identity audit rows = total:%d caller:%d system:%d, want total:3 caller:1 system:2", identityRows, callerRows, systemRows)
	}

	escaped := []byte(`{"version":1,"type":"request","op":"lease.acquire","request_id":"escaped-surrogate","data":{"requester_id":"\ud83d\ude00","tag":"qa.identity","duration_ns":60000000000,"wait_mode":"no_wait"}}`)
	escapedGrant, _ := server.handleBytes(escaped, uint32(server.ownerUID))
	escapedRequest, escapedRequestOK := escapedGrant.Data["request"].(*RequestRecord)
	escapedLease, escapedLeaseOK := escapedGrant.Data["lease"].(LeaseRecord)
	if escapedGrant.State != "granted" || !escapedRequestOK || !escapedLeaseOK || escapedRequest.RequesterID == nil || *escapedRequest.RequesterID != "😀" || escapedLease.HolderID == nil || *escapedLease.HolderID != "😀" {
		t.Fatalf("escaped surrogate identity = %#v, want decoded printable Unicode preserved", escapedGrant)
	}
}

// TestV3AuditRejectsMissingCallerButAcceptsResourceCaller proves caller rows require an identity, system rows may omit one, and resource caller rows keep their parent request ID empty.
func TestV3AuditRejectsMissingCallerButAcceptsResourceCaller(t *testing.T) {
	server := newDirectTestServer(t)
	at := "2026-11-18T12:00:00.000000000Z"
	insert := "INSERT INTO audit(at,operation,actor_kind,actor_component,cause,cause_ref_kind,cause_ref_id,origin,request_id,requester_id,holder_id,legacy_unattributed,target_id,result_code,from_state,to_state) VALUES(?, 'resource.add', 'caller', NULL, NULL, NULL, NULL, NULL, NULL, ?, NULL, 0, ?, 'ok', NULL, 'available')"
	if _, err := server.store.db.Exec(insert, at, nil, "missing-caller-resource"); err == nil {
		t.Fatal("caller audit accepted an empty caller ID, want schema constraint rejection")
	}
	rows, err := server.store.db.Query("SELECT COUNT(*) AS n FROM audit")
	if err != nil || len(rows) != 1 || rowInt(rows[0], "n") != 0 {
		t.Fatalf("audit after rejected insert = %#v, %v; want no partial row", rows, err)
	}
	if _, err := server.store.db.Exec(insert, at, "opaque caller:/ λ", "caller-backed-resource"); err != nil {
		t.Fatalf("v3 audit rejected valid caller control: %v", err)
	}
	_, err = server.store.db.Exec(`INSERT INTO audit(at,operation,actor_kind,actor_component,cause,cause_ref_kind,cause_ref_id,origin,request_id,requester_id,holder_id,legacy_unattributed,target_id,result_code,from_state,to_state)
VALUES(?, 'scheduler', 'system', 'scheduler', 'exhaustion', 'resource', 'system-resource', NULL, NULL, NULL, NULL, 0, 'system-resource', 'ok', 'available', 'disabled')`, at)
	if err != nil {
		t.Fatalf("v3 system audit rejected a callerless system control: %v", err)
	}
	rows, err = server.store.db.Query("SELECT actor_kind,actor_component,cause,request_id,requester_id,holder_id,legacy_unattributed,target_id FROM audit ORDER BY sequence")
	if err != nil || len(rows) != 2 || rowString(rows[0], "actor_kind") != "caller" || rows[0]["request_id"] != nil || rows[0]["requester_id"] != "opaque caller:/ λ" || rows[0]["holder_id"] != nil || rowInt(rows[0], "legacy_unattributed") != 0 || rowString(rows[1], "actor_kind") != "system" || rows[1]["requester_id"] != nil || rowString(rows[1], "cause") != "exhaustion" {
		t.Fatalf("caller-backed resource audit = %#v, %v; want empty parent and exact caller", rows, err)
	}
	before := int64(len(rows))
	_, err = server.store.db.Exec(`INSERT INTO audit(at,operation,actor_kind,actor_component,cause,cause_ref_kind,cause_ref_id,origin,request_id,requester_id,holder_id,legacy_unattributed,target_id,result_code,from_state,to_state)
VALUES(?, 'scheduler', 'caller', 'scheduler', 'promotion', 'request', 'bad-request', NULL, 'bad-request', 'opaque caller:/ λ', NULL, 0, 'bad-grant', 'ok', 'queued', 'leased')`, at)
	if err == nil {
		t.Fatal("v3 audit accepted a scheduler transition labeled caller")
	}
	rows, err = server.store.db.Query("SELECT COUNT(*) AS n FROM audit")
	if err != nil || len(rows) != 1 || rowInt(rows[0], "n") != before {
		t.Fatalf("audit after caller-labeled system rejection = %#v, %v; want no inserted row", rows, err)
	}
}

// TestInvalidRequesterIDsHaveNoDurableEffects proves malformed, absent, and unprintable caller IDs refuse before request, lease, or audit writes while a valid resource-control fixture remains unchanged.
func TestInvalidRequesterIDsHaveNoDurableEffects(t *testing.T) {
	server := newDirectTestServer(t)
	addManualResource(t, server, "qa.identity", "identity-resource")
	beforeStatus, statusCode := server.store.ServiceStatus()
	if statusCode != "" {
		t.Fatalf("initial status = %s", statusCode)
	}
	beforeAudit := hardEndAuditCount(t, server)
	cases := []struct {
		name string
		raw  []byte
	}{
		{name: "missing", raw: acquireRawWithRequester("missing-requester", nil)},
		{name: "empty", raw: acquireRawWithRequester("empty-requester", []byte(`""`))},
		{name: "control", raw: acquireRawWithRequester("control-requester", marshalRequesterID(t, "bad\nvalue"))},
		{name: "too-long", raw: acquireRawWithRequester("long-requester", marshalRequesterID(t, strings.Repeat("x", 129)))},
		{name: "invalid-utf8", raw: acquireRawWithRequester("utf8-requester", []byte{'"', 'x', 0xff, '"'})},
		{name: "unpaired-high-surrogate", raw: acquireRawWithRequester("high-surrogate-requester", []byte(`"\ud800"`))},
		{name: "unpaired-low-surrogate", raw: acquireRawWithRequester("low-surrogate-requester", []byte(`"\udc00"`))},
		{name: "mismatched-surrogates", raw: acquireRawWithRequester("mismatched-surrogate-requester", []byte(`"\ud800\u0041"`))},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			response, _ := server.handleBytes(tc.raw, uint32(server.ownerUID))
			if response.State != "error" || response.Code != "invalid_requester_id" || response.RequestID == "" || len(response.Data) != 0 {
				t.Fatalf("invalid identity response = %#v, want typed refusal with empty data", response)
			}
			if _, _, code := server.store.Status(response.RequestID, ""); code != "unknown_request" {
				t.Fatalf("invalid identity reserved request %q: %s", response.RequestID, code)
			}
			if got := hardEndAuditCount(t, server); got != beforeAudit {
				t.Fatalf("invalid identity wrote audit rows: before=%d after=%d", beforeAudit, got)
			}
			afterStatus, code := server.store.ServiceStatus()
			if code != "" || !reflect.DeepEqual(afterStatus, beforeStatus) {
				t.Fatalf("invalid identity changed service state: code=%s before=%#v after=%#v", code, beforeStatus, afterStatus)
			}
		})
	}
}

// TestRequesterIDMismatchOnRetryDoesNotDuplicateEffects proves an idempotency key cannot be replayed under another caller ID and the refusal preserves the original request, lease, and audit history.
func TestRequesterIDMismatchOnRetryDoesNotDuplicateEffects(t *testing.T) {
	server := newDirectTestServer(t)
	addManualResource(t, server, "qa.identity", "identity-resource")
	state, _, code := server.store.Acquire(AcquireInput{RequestID: "identity-idempotent", RequesterID: "caller-one", Tag: "qa.identity", DurationNS: int64(time.Minute), WaitMode: "no_wait"})
	if state != "granted" || code != "" {
		t.Fatalf("initial request = %s/%s, want granted", state, code)
	}
	beforeAudit := hardEndAuditCount(t, server)
	beforeLeases, err := server.store.db.Query("SELECT COUNT(*) AS count FROM leases")
	if err != nil || len(beforeLeases) != 1 {
		t.Fatalf("count initial leases = %#v, %v", beforeLeases, err)
	}

	state, data, code := server.store.Acquire(AcquireInput{RequestID: "identity-idempotent", RequesterID: "caller-two", Tag: "qa.identity", DurationNS: int64(time.Minute), WaitMode: "no_wait"})
	request, requestOK := data["request"].(*RequestRecord)
	if state != "error" || code != "request_id_conflict" || !requestOK || request.RequesterID == nil || *request.RequesterID != "caller-one" {
		t.Fatalf("changed-caller retry = %s/%s/%#v, want conflict with the original request", state, code, data)
	}
	afterLeases, err := server.store.db.Query("SELECT COUNT(*) AS count FROM leases")
	if err != nil || len(afterLeases) != 1 || rowInt(afterLeases[0], "count") != rowInt(beforeLeases[0], "count") || hardEndAuditCount(t, server) != beforeAudit {
		t.Fatalf("changed-caller retry duplicated durable effects: leases=%#v audit-before=%d audit-after=%d", afterLeases, beforeAudit, hardEndAuditCount(t, server))
	}
}

// TestV2MigrationPreservesLegacyLeaseAndQueueState proves v2 migration keeps active lease fencing and hard ends, per-pool queue order and deadlines, terminal records, and audit history while marking every old caller unattributed.
func TestV2MigrationPreservesLegacyLeaseAndQueueState(t *testing.T) {
	stateDir := filepath.Join(shortTestDir(t), "state")
	dbPath := writeCanonicalV2IdentityFixture(t, stateDir, int64(os.Geteuid()))
	db, err := sqlite.Open(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	if err := db.Transaction(func(tx *sqlite.Tx) error { return migrateDatabase(tx, int64(os.Geteuid())) }); err != nil {
		_ = db.Close()
		t.Fatalf("migrate canonical v2 fixture: %v", err)
	}
	if err := validateDatabase(db, int64(os.Geteuid())); err != nil {
		_ = db.Close()
		t.Fatalf("validate migrated v3 database: %v", err)
	}

	goPool, err := db.Query("SELECT class,next_queue_sequence FROM pools WHERE tag='mac.go-test-manual'")
	if err != nil || len(goPool) != 1 || nullableString(goPool[0], "class") == nil || *nullableString(goPool[0], "class") != "go-test" {
		t.Fatalf("migrated Go pool = %#v, %v, want class go-test", goPool, err)
	}
	jvmPool, err := db.Query("SELECT class,next_queue_sequence FROM pools WHERE tag='mac.jvm-build'")
	if err != nil || len(jvmPool) != 1 || nullableString(jvmPool[0], "class") == nil || *nullableString(jvmPool[0], "class") != "jvm-build" {
		t.Fatalf("migrated JVM pool = %#v, %v, want class jvm-build", jvmPool, err)
	}
	classes, err := db.Query("SELECT class,next_queue_sequence FROM lease_classes ORDER BY class")
	if err != nil || len(classes) != 2 || rowString(classes[0], "class") != "go-test" || rowInt(classes[0], "next_queue_sequence") != 4 || rowString(classes[1], "class") != "jvm-build" || rowInt(classes[1], "next_queue_sequence") != 2 {
		t.Fatalf("migrated class counters = %#v, %v, want original per-pool sequence positions", classes, err)
	}

	requests, err := db.Query("SELECT request_id,requester_id,legacy_unattributed,class,queue_sequence,class_queue_sequence,queued_at,wait_deadline_at,state,outcome_code FROM requests ORDER BY request_id")
	if err != nil || len(requests) != 6 {
		t.Fatalf("migrated requests = %#v, %v, want six historical records", requests, err)
	}
	byID := make(map[string]sqlite.Row, len(requests))
	for _, row := range requests {
		byID[rowString(row, "request_id")] = row
		if row["requester_id"] != nil || rowInt(row, "legacy_unattributed") != 1 {
			t.Errorf("migrated request identity = %#v, want explicitly legacy and null", row)
		}
	}
	for _, want := range []struct {
		id, class, state, deadline string
		sequence, classSequence    int64
	}{
		{"go-queued-early", "go-test", "queued", "2026-11-14T00:00:20.000000000Z", 1, 1},
		{"go-queued-late", "go-test", "queued", "2026-11-14T00:00:32.000000000Z", 3, 2},
		{"jvm-queued", "jvm-build", "queued", "2026-11-14T00:00:31.000000000Z", 1, 1},
	} {
		row, ok := byID[want.id]
		if !ok || nullableString(row, "class") == nil || *nullableString(row, "class") != want.class || rowString(row, "state") != want.state || rowInt(row, "queue_sequence") != want.sequence || rowInt(row, "class_queue_sequence") != want.classSequence || rowString(row, "wait_deadline_at") != want.deadline {
			t.Errorf("migrated queued request %q = %#v, want preserved pool/class sequence and deadline", want.id, row)
		}
	}
	terminal := byID["go-terminal"]
	if terminal == nil || rowString(terminal, "state") != "timed_out" || rowString(terminal, "outcome_code") != "request_timed_out" || rowInt(terminal, "queue_sequence") != 2 || terminal["requester_id"] != nil {
		t.Fatalf("migrated terminal request = %#v, want prior timeout and queue history", terminal)
	}
	legacyQueued := byID["legacy-queued"]
	if legacyQueued == nil || legacyQueued["class"] != nil || legacyQueued["class_queue_sequence"] != nil || rowInt(legacyQueued, "queue_sequence") != 1 || rowString(legacyQueued, "wait_deadline_at") != "2026-11-14T00:00:30.000000000Z" {
		t.Fatalf("migrated unclassified queue row = %#v, want preserved legacy queue metadata without a class", legacyQueued)
	}

	leases, err := db.Query("SELECT id,request_id,holder_id,legacy_unattributed,hard_end_at,fencing_token,state FROM leases")
	if err != nil || len(leases) != 1 {
		t.Fatalf("migrated active lease = %#v, %v", leases, err)
	}
	if rowString(leases[0], "request_id") != "go-active" || leases[0]["holder_id"] != nil || rowInt(leases[0], "legacy_unattributed") != 1 || rowString(leases[0], "hard_end_at") != "2026-11-14T00:00:10.000000000Z" || rowInt(leases[0], "fencing_token") != 7 || rowString(leases[0], "state") != "active" {
		t.Fatalf("migrated lease = %#v, want original active hard end and fence", leases[0])
	}
	audit, err := db.Query("SELECT sequence,actor_kind,requester_id,holder_id,legacy_unattributed,origin FROM audit ORDER BY sequence")
	if err != nil || len(audit) != 5 {
		t.Fatalf("migrated audit history = %#v, %v, want five rows", audit, err)
	}
	for index, row := range audit {
		if rowInt(row, "sequence") != int64(index+1) || rowString(row, "actor_kind") != "legacy" || row["requester_id"] != nil || row["holder_id"] != nil || rowInt(row, "legacy_unattributed") != 1 || rowString(row, "origin") != "pre_v3" {
			t.Errorf("migrated audit row %d = %#v, want exact sequence and legacy attribution", index, row)
		}
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
}

// TestV2MigrationRollsBackAfterInjectedFailure proves an interruption after all migration writes restores the complete v2 table state, schema, and version.
func TestV2MigrationRollsBackAfterInjectedFailure(t *testing.T) {
	stateDir := filepath.Join(shortTestDir(t), "state")
	dbPath := writeCanonicalV2IdentityFixture(t, stateDir, int64(os.Geteuid()))
	beforeBytes, err := os.ReadFile(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	beforeState := snapshotCanonicalTables(t, dbPath)
	db, err := sqlite.Open(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	injected := errors.New("injected migration caller failure")
	err = db.Transaction(func(tx *sqlite.Tx) error {
		if err := migrateDatabase(tx, int64(os.Geteuid())); err != nil {
			return err
		}
		return injected
	})
	if !errors.Is(err, injected) {
		_ = db.Close()
		t.Fatalf("migration transaction = %v, want injected caller failure", err)
	}
	if err := validateDatabaseVersion(db, int64(os.Geteuid()), schemaV2Version); err != nil {
		_ = db.Close()
		t.Fatalf("database after rollback is not valid v2: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	afterBytes, err := os.ReadFile(dbPath)
	if err != nil || !bytes.Equal(afterBytes, beforeBytes) {
		t.Fatalf("migration rollback changed database bytes: read=%v equal=%v", err, bytes.Equal(afterBytes, beforeBytes))
	}
	if afterState := snapshotCanonicalTables(t, dbPath); !reflect.DeepEqual(afterState, beforeState) {
		t.Fatalf("migration rollback changed table state:\nbefore=%#v\nafter=%#v", beforeState, afterState)
	}
}

func acquireRawWithRequester(requestID string, requesterJSON []byte) []byte {
	requesterField := ""
	if requesterJSON != nil {
		requesterField = `,"requester_id":` + string(requesterJSON)
	}
	return []byte(`{"version":1,"type":"request","op":"lease.acquire","request_id":"` + requestID + `","data":{"tag":"qa.identity","duration_ns":60000000000,"wait_mode":"no_wait"` + requesterField + `}}`)
}

func marshalRequesterID(t *testing.T, value string) []byte {
	t.Helper()
	encoded, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	return encoded
}

func writeCanonicalV2IdentityFixture(t *testing.T, stateDir string, ownerUID int64) string {
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
	schema, err := os.ReadFile(filepath.Join("testdata", "canonical-v2.sql"))
	if err != nil {
		_ = db.Close()
		t.Fatal(err)
	}
	if err := db.ExecScript(string(schema)); err != nil {
		_ = db.Close()
		t.Fatalf("create independent v2 fixture schema: %v", err)
	}
	const at = "2026-11-14T00:00:00.000000000Z"
	const bootID = hardEndBootA
	statements := []struct {
		query string
		args  []any
	}{
		{"INSERT INTO settings(singleton_id,schema_version,owner_uid,max_lease_ns,max_wait_ns,boot_session_id,created_at) VALUES(1,2,?,?,?,?,?)", []any{ownerUID, maxLeaseNS, maxWaitNS, bootID, at}},
		{"INSERT INTO pools(tag,mode,next_queue_sequence,created_at) VALUES(?,?,?,?)", []any{"mac.go-test-manual", "manual", int64(4), at}},
		{"INSERT INTO pools(tag,mode,next_queue_sequence,created_at) VALUES(?,?,?,?)", []any{"mac.jvm-build", "manual", int64(2), at}},
		{"INSERT INTO pools(tag,mode,next_queue_sequence,created_at) VALUES(?,?,?,?)", []any{"qa.legacy", "manual", int64(2), at}},
		{"INSERT INTO resources(id,tag,state,fencing_token,created_at,updated_at) VALUES(?,?, 'leased',7,?,?)", []any{"go-resource", "mac.go-test-manual", at, at}},
		{"INSERT INTO resources(id,tag,state,fencing_token,created_at,updated_at) VALUES(?,?, 'available',0,?,?)", []any{"jvm-resource", "mac.jvm-build", at, at}},
		{"INSERT INTO resources(id,tag,state,fencing_token,created_at,updated_at) VALUES(?,?, 'available',0,?,?)", []any{"legacy-resource", "qa.legacy", at, at}},
		v2RequestInsert("go-active", "mac.go-test-manual", "no_wait", nil, nil, nil, nil, nil, nil, nil, "leased", nil, at),
		v2RequestInsert("go-queued-early", "mac.go-test-manual", "wait", ptrInt64(int64(20*time.Second)), ptrInt64(1), ptrString(at), ptrString("2026-11-14T00:00:20.000000000Z"), ptrString(bootID), ptrInt64(int64(time.Second)), ptrInt64(int64(21*time.Second)), "queued", nil, at),
		v2RequestInsert("go-terminal", "mac.go-test-manual", "wait", ptrInt64(int64(20*time.Second)), ptrInt64(2), ptrString(at), ptrString("2026-11-14T00:00:20.000000000Z"), ptrString(bootID), ptrInt64(int64(time.Second)), ptrInt64(int64(21*time.Second)), "timed_out", ptrString("request_timed_out"), at),
		v2RequestInsert("go-queued-late", "mac.go-test-manual", "wait", ptrInt64(int64(30*time.Second)), ptrInt64(3), ptrString("2026-11-14T00:00:02.000000000Z"), ptrString("2026-11-14T00:00:32.000000000Z"), ptrString(bootID), ptrInt64(int64(2*time.Second)), ptrInt64(int64(32*time.Second)), "queued", nil, at),
		v2RequestInsert("jvm-queued", "mac.jvm-build", "wait", ptrInt64(int64(30*time.Second)), ptrInt64(1), ptrString("2026-11-14T00:00:01.000000000Z"), ptrString("2026-11-14T00:00:31.000000000Z"), ptrString(bootID), ptrInt64(int64(time.Second)), ptrInt64(int64(31*time.Second)), "queued", nil, at),
		v2RequestInsert("legacy-queued", "qa.legacy", "wait", ptrInt64(int64(30*time.Second)), ptrInt64(1), ptrString(at), ptrString("2026-11-14T00:00:30.000000000Z"), ptrString(bootID), ptrInt64(int64(time.Second)), ptrInt64(int64(31*time.Second)), "queued", nil, at),
		{"INSERT INTO leases(id,request_id,resource_id,state,duration_ns,granted_at,hard_end_at,boot_session_id,granted_monotonic_ns,hard_end_monotonic_ns,fencing_token,ended_at,ended_resource_state,ended_fencing_token) VALUES(?,?,?,'active',?,?,?,?,?,?,?,NULL,NULL,NULL)", []any{"00000000000000000000000000000007", "go-active", "go-resource", int64(10 * time.Second), at, "2026-11-14T00:00:10.000000000Z", bootID, int64(time.Second), int64(11 * time.Second), int64(7)}},
		{"INSERT INTO audit(sequence,at,operation,request_id,target_id,result_code,from_state,to_state) VALUES(1,?,'resource.add',NULL,?,'ok',NULL,'available')", []any{at, "go-resource"}},
		{"INSERT INTO audit(sequence,at,operation,request_id,target_id,result_code,from_state,to_state) VALUES(2,?,'lease.acquire',?,?,'ok',NULL,'leased')", []any{at, "go-active", "go-active"}},
		{"INSERT INTO audit(sequence,at,operation,request_id,target_id,result_code,from_state,to_state) VALUES(3,?,'lease.acquire',?,?,'ok',NULL,'queued')", []any{at, "go-queued-early", "go-queued-early"}},
		{"INSERT INTO audit(sequence,at,operation,request_id,target_id,result_code,from_state,to_state) VALUES(4,?,'scheduler',?,?,'request_timed_out','queued','timed_out')", []any{at, "go-terminal", "go-terminal"}},
		{"INSERT INTO audit(sequence,at,operation,request_id,target_id,result_code,from_state,to_state) VALUES(5,?,'lease.acquire',?,?,'ok',NULL,'queued')", []any{at, "jvm-queued", "jvm-queued"}},
	}
	for _, statement := range statements {
		if _, err := db.Exec(statement.query, statement.args...); err != nil {
			_ = db.Close()
			t.Fatalf("populate canonical v2 fixture: %v", err)
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

func v2RequestInsert(requestID, tag, waitMode string, waitTimeout, queueSequence *int64, queuedAt, waitDeadline, queueBoot *string, queueStarted, deadline *int64, state string, outcome *string, createdAt string) struct {
	query string
	args  []any
} {
	var waitTimeoutValue, queueSequenceValue, queuedAtValue, waitDeadlineValue, queueBootValue, queueStartedValue, deadlineValue, outcomeValue any
	if waitTimeout != nil {
		waitTimeoutValue = *waitTimeout
	}
	if queueSequence != nil {
		queueSequenceValue = *queueSequence
	}
	if queuedAt != nil {
		queuedAtValue = *queuedAt
	}
	if waitDeadline != nil {
		waitDeadlineValue = *waitDeadline
	}
	if queueBoot != nil {
		queueBootValue = *queueBoot
	}
	if queueStarted != nil {
		queueStartedValue = *queueStarted
	}
	if deadline != nil {
		deadlineValue = *deadline
	}
	if outcome != nil {
		outcomeValue = *outcome
	}
	return struct {
		query string
		args  []any
	}{
		query: "INSERT INTO requests(request_id,tag,duration_ns,wait_mode,wait_timeout_ns,queue_sequence,queued_at,wait_deadline_at,queue_boot_session_id,queue_started_monotonic_ns,wait_deadline_monotonic_ns,state,outcome_code,created_at,updated_at) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
		args:  []any{requestID, tag, int64(10 * time.Second), waitMode, waitTimeoutValue, queueSequenceValue, queuedAtValue, waitDeadlineValue, queueBootValue, queueStartedValue, deadlineValue, state, outcomeValue, createdAt, createdAt},
	}
}

func ptrInt64(value int64) *int64    { return &value }
func ptrString(value string) *string { return &value }
