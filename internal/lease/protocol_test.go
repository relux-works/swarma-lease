package lease

import "testing"

// TestRequestEnvelopeAllowsItsDeclaredKeyAndRejectsUnknownKeys proves acquire request IDs remain accepted while unknown envelope keys are refused without reserving their IDs.
func TestRequestEnvelopeAllowsItsDeclaredKeyAndRejectsUnknownKeys(t *testing.T) {
	server := newTestServer(t)
	uid := uint32(server.ownerUID)

	valid := request(t, server, uid, map[string]any{
		"version": 1, "type": "request", "op": "lease.acquire", "request_id": "valid-request",
		"data": map[string]any{"tag": "missing.pool", "duration_ns": int64(1_000_000_000), "wait_mode": "no_wait"},
	})
	if valid.Code != "unknown_tag" {
		t.Fatalf("declared request_id acquire = %#v, want unknown_tag after envelope acceptance", valid)
	}

	invalid := request(t, server, uid, map[string]any{
		"version": 1, "type": "request", "op": "lease.acquire", "request_id": "extra-key-request", "unexpected": true,
		"data": map[string]any{"tag": "missing.pool", "duration_ns": int64(1_000_000_000), "wait_mode": "no_wait"},
	})
	if invalid.Code != "invalid_request" {
		t.Fatalf("unknown envelope key = %#v, want invalid_request", invalid)
	}

	status := request(t, server, uid, map[string]any{
		"version": 1, "type": "request", "op": "lease.status",
		"data": map[string]any{"request_id": "extra-key-request"},
	})
	if status.Code != "unknown_request" {
		t.Fatalf("status after envelope refusal = %#v, want no reserved request", status)
	}
}
