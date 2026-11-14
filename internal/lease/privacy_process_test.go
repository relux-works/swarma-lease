package lease

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/relux-works/swarma-lease/internal/sqlite"
)

const (
	privacyHelperEnv = "SWARMA_LEASE_PRIVACY_HELPER"
	privacyBootID    = "00000000-0000-4000-8000-000000000777"
	privacyMonoNS    = int64(712345678901234567)
)

type privacyMarkerClock struct{}

func (privacyMarkerClock) Read() (Sample, error) {
	return Sample{
		Wall:          time.Date(2026, 11, 14, 9, 0, 0, 0, time.UTC),
		MonotonicNS:   privacyMonoNS,
		BootSessionID: privacyBootID,
	}, nil
}

// TestServicePrivacyCaptureHelper hosts an isolated service in a child test process so its stdout and stderr can be captured independently.
func TestServicePrivacyCaptureHelper(t *testing.T) {
	if os.Getenv(privacyHelperEnv) != "1" {
		return
	}
	stateDir := os.Getenv("SWARMA_LEASE_PRIVACY_STATE")
	socketPath := os.Getenv("SWARMA_LEASE_PRIVACY_SOCKET")
	readyPath := os.Getenv("SWARMA_LEASE_PRIVACY_READY")
	if stateDir == "" || socketPath == "" || readyPath == "" {
		t.Fatal("privacy helper paths are missing")
	}
	server, err := Start(Config{StateDir: stateDir, SocketPath: socketPath, Clock: privacyMarkerClock{}})
	if err != nil {
		t.Fatalf("start isolated privacy service: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	serveDone := make(chan error, 1)
	go func() { serveDone <- server.Serve(ctx) }()
	stop := make(chan os.Signal, 1)
	signal.Notify(stop, os.Interrupt, syscall.SIGTERM)
	defer signal.Stop(stop)
	if err := os.WriteFile(readyPath, []byte("ready\n"), 0600); err != nil {
		_ = server.Close()
		t.Fatalf("publish helper readiness: %v", err)
	}
	<-stop
	cancel()
	if err := <-serveDone; err != nil {
		t.Fatalf("stop isolated privacy service: %v", err)
	}
	if err := server.Close(); err != nil {
		t.Fatalf("close isolated privacy service: %v", err)
	}
}

// TestRejectedSocketInputDoesNotReachServiceProcessOutput proves a live service persists no rejected secret-like, command, or environment input, keeps those and synthetic clock markers out of responses and process output, and exposes only documented fields for valid results and audit rows.
func TestRejectedSocketInputDoesNotReachServiceProcessOutput(t *testing.T) {
	root := shortTestDir(t)
	stateDir := filepath.Join(root, "state")
	socketPath := filepath.Join(root, "service.sock")
	readyPath := filepath.Join(root, "ready")
	var stdout, stderr bytes.Buffer
	testBinary, err := filepath.Abs(os.Args[0])
	if err != nil {
		t.Fatalf("resolve test helper binary: %v", err)
	}
	cmd := exec.Command(testBinary, "-test.run=^TestServicePrivacyCaptureHelper$")
	cmd.Env = []string{
		privacyHelperEnv + "=1",
		"SWARMA_LEASE_PRIVACY_STATE=" + stateDir,
		"SWARMA_LEASE_PRIVACY_SOCKET=" + socketPath,
		"SWARMA_LEASE_PRIVACY_READY=" + readyPath,
	}
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	if err := cmd.Start(); err != nil {
		t.Fatalf("start isolated privacy service process: %v", err)
	}
	finished := false
	t.Cleanup(func() {
		if finished {
			return
		}
		_ = cmd.Process.Signal(syscall.SIGTERM)
		_ = cmd.Wait()
	})
	readyDeadline := time.Now().Add(5 * time.Second)
	for {
		if _, err := os.Stat(readyPath); err == nil {
			break
		} else if !os.IsNotExist(err) {
			t.Fatalf("check privacy service readiness: %v", err)
		}
		if time.Now().After(readyDeadline) {
			t.Fatal("isolated privacy service did not become ready")
		}
		time.Sleep(time.Millisecond)
	}

	const secretMarker = "private/rejected/value-marker"
	const commandMarker = "synthetic-command-secret-marker"
	const environmentMarker = "synthetic-environment-secret-marker"
	rejectedRaw := fmt.Sprintf(`{"version":1,"type":"request","op":"lease.acquire","request_id":"privacy-rejection","data":{"tag":%q,"duration_ns":1,"wait_mode":"no_wait","command":%q,"environment":{"SYNTHETIC_TOKEN":%q},"boot_session_id":%q,"monotonic_ns":%d}}`, secretMarker, commandMarker, environmentMarker, privacyBootID, privacyMonoNS)
	rejected, _ := exchangeRawSocketRequest(t, socketPath, rejectedRaw)
	if rejected.Code != "invalid_request" || rejected.State != "error" || rejected.RequestID != "privacy-rejection" || len(rejected.Data) != 0 {
		t.Fatalf("rejected private-looking frame = %#v, want exact invalid_request refusal", rejected)
	}
	rejectedOutput, err := json.Marshal(rejected)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(rejectedOutput), secretMarker) || strings.Contains(string(rejectedOutput), commandMarker) || strings.Contains(string(rejectedOutput), environmentMarker) || strings.Contains(string(rejectedOutput), privacyBootID) || strings.Contains(string(rejectedOutput), fmt.Sprint(privacyMonoNS)) {
		t.Fatalf("rejected frame output reflected private markers: %s", rejectedOutput)
	}
	assertPrivacyDatabaseCounts(t, stateDir, 0, 0)
	assertPrivacyRequestMarkersAbsentFromDatabase(t, stateDir, secretMarker, commandMarker, environmentMarker)

	added, addedFields := exchangeRawSocketRequest(t, socketPath, `{"version":1,"type":"request","op":"resource.add","data":{"tag":"qa.privacy","resource_id":"privacy-control","mode":"manual"}}`)
	if added.State != "added" || added.Code != "" || len(added.Data) != 2 || added.Data["pool"] == nil || added.Data["resource"] == nil {
		t.Fatalf("valid resource control = %#v, want allowlisted pool/resource result", added)
	}
	assertPrivacyResponseFields(t, addedFields, false)
	granted, grantedFields := exchangeRawSocketRequest(t, socketPath, `{"version":1,"type":"request","op":"lease.acquire","request_id":"privacy-control","data":{"tag":"qa.privacy","duration_ns":1000000000,"wait_mode":"no_wait"}}`)
	if granted.State != "granted" || granted.Code != "" || granted.RequestID != "privacy-control" || len(granted.Data) != 3 || granted.Data["request"] == nil || granted.Data["lease"] == nil || granted.Data["resource"] == nil {
		t.Fatalf("valid acquisition control = %#v, want allowlisted request/lease/resource result", granted)
	}
	assertPrivacyResponseFields(t, grantedFields, true)
	history, historyFields := exchangeRawSocketRequest(t, socketPath, `{"version":1,"type":"request","op":"audit.list","data":{"limit":10}}`)
	entries, ok := history.Data["entries"].([]any)
	if history.State != "ok" || history.Code != "" || !ok || len(entries) != 4 {
		t.Fatalf("valid audit control = %#v, want four allowed transition rows", history)
	}
	assertPrivacyResponseFields(t, historyFields, false)
	allowedAuditFields := map[string]bool{"sequence": true, "at": true, "operation": true, "request_id": true, "target_id": true, "result_code": true, "from_state": true, "to_state": true}
	for index, value := range entries {
		entry, ok := value.(map[string]any)
		if !ok || len(entry) != len(allowedAuditFields) {
			t.Fatalf("audit entry %d = %#v, want exact allowlisted shape", index, value)
		}
		for key := range entry {
			if !allowedAuditFields[key] {
				t.Fatalf("audit entry %d has unexpected field %q: %#v", index, key, entry)
			}
		}
	}

	if err := cmd.Process.Signal(syscall.SIGTERM); err != nil {
		t.Fatalf("stop isolated privacy service process: %v", err)
	}
	if err := cmd.Wait(); err != nil {
		t.Fatalf("privacy service test process exited with error: %v; stdout=%q stderr=%q", err, stdout.String(), stderr.String())
	}
	finished = true
	allOutput := stdout.String() + stderr.String()
	for _, marker := range []string{secretMarker, commandMarker, environmentMarker, privacyBootID, fmt.Sprint(privacyMonoNS)} {
		if strings.Contains(allOutput, marker) {
			t.Fatalf("service process output contains private marker %q: stdout=%q stderr=%q", marker, stdout.String(), stderr.String())
		}
	}
	responses := []Response{rejected, added, granted, history}
	for _, response := range responses {
		encoded, err := json.Marshal(response)
		if err != nil {
			t.Fatal(err)
		}
		for _, marker := range []string{secretMarker, commandMarker, environmentMarker, privacyBootID, fmt.Sprint(privacyMonoNS)} {
			if strings.Contains(string(encoded), marker) {
				t.Fatalf("service response contains private marker %q: %s", marker, encoded)
			}
		}
	}
	assertPrivacyDatabaseCounts(t, stateDir, 4, 1)
}

func assertPrivacyDatabaseCounts(t *testing.T, stateDir string, wantAudit, wantRequests int64) {
	t.Helper()
	db, err := sqlite.Open(filepath.Join(stateDir, "leases.sqlite"))
	if err != nil {
		t.Fatalf("open privacy fixture database: %v", err)
	}
	defer db.Close()
	rows, err := db.Query("SELECT (SELECT COUNT(*) FROM audit) AS audits,(SELECT COUNT(*) FROM requests) AS requests")
	if err != nil || len(rows) != 1 || rows[0]["audits"] != wantAudit || rows[0]["requests"] != wantRequests {
		t.Fatalf("privacy fixture durable counts = %#v, %v; want audit=%d requests=%d", rows, err, wantAudit, wantRequests)
	}
}

func assertPrivacyRequestMarkersAbsentFromDatabase(t *testing.T, stateDir string, markers ...string) {
	t.Helper()
	checks := []string{
		"SELECT COUNT(*) AS count FROM requests WHERE instr(request_id,?)>0 OR instr(tag,?)>0 OR instr(outcome_code,?)>0",
		"SELECT COUNT(*) AS count FROM leases WHERE instr(id,?)>0 OR instr(request_id,?)>0 OR instr(resource_id,?)>0 OR instr(state,?)>0",
		"SELECT COUNT(*) AS count FROM resources WHERE instr(id,?)>0 OR instr(tag,?)>0 OR instr(state,?)>0",
		"SELECT COUNT(*) AS count FROM pools WHERE instr(tag,?)>0 OR instr(mode,?)>0",
		"SELECT COUNT(*) AS count FROM audit WHERE instr(operation,?)>0 OR instr(request_id,?)>0 OR instr(target_id,?)>0 OR instr(result_code,?)>0 OR instr(from_state,?)>0 OR instr(to_state,?)>0",
	}
	db, err := sqlite.Open(filepath.Join(stateDir, "leases.sqlite"))
	if err != nil {
		t.Fatalf("open privacy fixture database for marker check: %v", err)
	}
	defer db.Close()
	for _, marker := range markers {
		for _, query := range checks {
			placeholders := strings.Count(query, "?")
			args := make([]any, placeholders)
			for index := range args {
				args[index] = marker
			}
			rows, err := db.Query(query, args...)
			if err != nil {
				t.Fatalf("query rejected marker %q with %q: %v", marker, query, err)
			}
			if len(rows) != 1 || rows[0]["count"] != int64(0) {
				t.Fatalf("rejected marker %q persisted: rows=%#v", marker, rows)
			}
		}
	}
}

func assertPrivacyResponseFields(t *testing.T, fields map[string]json.RawMessage, hasRequestID bool) {
	t.Helper()
	want := map[string]bool{"version": true, "type": true, "op": true, "state": true, "data": true}
	if hasRequestID {
		want["request_id"] = true
	}
	if len(fields) != len(want) {
		t.Fatalf("response envelope = %#v, want exactly %#v", fields, want)
	}
	for field := range fields {
		if !want[field] {
			t.Fatalf("unexpected response field %q in %#v", field, fields)
		}
	}
	for field := range want {
		if _, ok := fields[field]; !ok {
			t.Fatalf("missing response field %q in %#v", field, fields)
		}
	}
}
