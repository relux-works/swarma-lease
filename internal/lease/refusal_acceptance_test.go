package lease

import (
	"bufio"
	"encoding/json"
	"net"
	"testing"
	"time"
)

// TestEarlyVersionRefusalsPreserveOnlyValidRequestKeys proves the live socket service keeps only contract-valid request IDs on early version refusals, preserves version precedence, and creates no durable effects.
func TestEarlyVersionRefusalsPreserveOnlyValidRequestKeys(t *testing.T) {
	server := newTestServer(t)
	cases := []struct {
		name, raw, op, code, requestID string
		wantRequestID                  bool
	}{
		{
			name: "unsupported-acquire",
			raw:  `{"version":2,"type":"request","op":"lease.acquire","request_id":"v2-acquire","data":{"tag":"qa.version","duration_ns":1,"wait_mode":"no_wait"}}`,
			op:   "lease.acquire", code: "unsupported_version", requestID: "v2-acquire", wantRequestID: true,
		},
		{
			name: "unsupported-wait",
			raw:  `{"version":2,"type":"request","op":"lease.wait","request_id":"v2-wait","data":{}}`,
			op:   "lease.wait", code: "unsupported_version", requestID: "v2-wait", wantRequestID: true,
		},
		{
			name: "unsupported-cancel",
			raw:  `{"version":2,"type":"request","op":"lease.cancel","request_id":"v2-cancel","data":{}}`,
			op:   "lease.cancel", code: "unsupported_version", requestID: "v2-cancel", wantRequestID: true,
		},
		{
			name: "unsupported-status-request-lookup",
			raw:  `{"version":2,"type":"request","op":"lease.status","data":{"request_id":"v2-status"}}`,
			op:   "lease.status", code: "unsupported_version", requestID: "v2-status", wantRequestID: true,
		},
		{
			name: "status-with-extra-data-does-not-reflect-lookup",
			raw:  `{"version":2,"type":"request","op":"lease.status","data":{"request_id":"v2-status-extra","lease_id":"00000000000000000000000000000001"}}`,
			op:   "lease.status", code: "unsupported_version",
		},
		{
			name: "status-invalid-lookup-stays-hidden",
			raw:  `{"version":2,"type":"request","op":"lease.status","data":{"request_id":"bad/id"}}`,
			op:   "lease.status", code: "unsupported_version",
		},
		{
			name: "status-wrong-envelope-type-stays-hidden",
			raw:  `{"version":2,"type":"response","op":"lease.status","data":{"request_id":"v2-status-type"}}`,
			op:   "lease.status", code: "unsupported_version",
		},
		{
			name: "status-extra-top-level-field-stays-hidden",
			raw:  `{"version":2,"type":"request","op":"lease.status","request_id":"top-level-key","data":{"request_id":"v2-status-top-extra"}}`,
			op:   "lease.status", code: "unsupported_version",
		},
		{
			name: "wrong-version-type",
			raw:  `{"version":"1","type":"request","op":"lease.acquire","request_id":"type-version","data":{"tag":"qa.version","duration_ns":1,"wait_mode":"no_wait"}}`,
			op:   "lease.acquire", code: "invalid_request", requestID: "type-version", wantRequestID: true,
		},
		{
			name: "invalid-key-stays-hidden",
			raw:  `{"version":2,"type":"request","op":"lease.wait","request_id":"bad/id","data":{}}`,
			op:   "lease.wait", code: "unsupported_version",
		},
		{
			name: "unknown-operation-stays-protocol",
			raw:  `{"version":2,"type":"request","op":"private-operation","request_id":"unknown-key","data":{}}`,
			op:   "protocol", code: "unsupported_version",
		},
		{
			name: "invalid-key-grammar-still-refuses",
			raw:  `{"version":1,"type":"request","op":"lease.acquire","request_id":"bad/id","data":{"tag":7,"duration_ns":1,"wait_mode":"no_wait"}}`,
			op:   "lease.acquire", code: "invalid_request",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			response, fields := exchangeRawServiceRequest(t, server, tc.raw)
			if response.Version != 1 || response.Type != "response" || response.Op != tc.op || response.State != "error" || response.Code != tc.code || len(response.Data) != 0 {
				t.Fatalf("response = %#v, want op=%s code=%s with empty data", response, tc.op, tc.code)
			}
			if tc.wantRequestID {
				if response.RequestID != tc.requestID {
					t.Fatalf("request_id = %q, want %q", response.RequestID, tc.requestID)
				}
			} else if response.RequestID != "" {
				t.Fatalf("invalid or unknown request ID was reflected: %q", response.RequestID)
			}
			wantFields := map[string]bool{"version": true, "type": true, "op": true, "state": true, "data": true, "code": true}
			if tc.wantRequestID {
				wantFields["request_id"] = true
			}
			if len(fields) != len(wantFields) {
				t.Fatalf("response fields = %#v, want exactly %#v", fields, wantFields)
			}
			for field := range fields {
				if !wantFields[field] {
					t.Fatalf("unexpected response field %q in %#v", field, fields)
				}
			}
			for field := range wantFields {
				if _, ok := fields[field]; !ok {
					t.Fatalf("missing response field %q in %#v", field, fields)
				}
			}
		})
	}
	if rows := hardEndAuditCount(t, server); rows != 0 {
		t.Fatalf("version and syntax refusals wrote %d audit rows", rows)
	}
	for _, requestID := range []string{"v2-acquire", "v2-wait", "v2-cancel", "v2-status", "type-version"} {
		if _, _, code := server.store.Status(requestID, ""); code != "unknown_request" {
			t.Fatalf("early refusal reserved %s: %s", requestID, code)
		}
	}
	if added, _ := exchangeRawServiceRequest(t, server, `{"version":1,"type":"request","op":"resource.add","data":{"tag":"qa.version","resource_id":"version-control","mode":"manual"}}`); added.State != "added" || added.Code != "" {
		t.Fatalf("valid resource control = %#v, want added", added)
	}
	granted, _ := exchangeRawServiceRequest(t, server, `{"version":1,"type":"request","op":"lease.acquire","request_id":"version-control","data":{"tag":"qa.version","duration_ns":1000000000,"wait_mode":"no_wait"}}`)
	if granted.State != "granted" || granted.Code != "" || granted.RequestID != "version-control" || granted.Data["lease"] == nil || granted.Data["resource"] == nil {
		t.Fatalf("valid acquisition control = %#v, want granted lease and resource", granted)
	}
}

func exchangeRawServiceRequest(t *testing.T, server *Server, raw string) (Response, map[string]json.RawMessage) {
	t.Helper()
	return exchangeRawSocketRequest(t, server.socketPath, raw)
}

func exchangeRawSocketRequest(t *testing.T, socketPath, raw string) (Response, map[string]json.RawMessage) {
	t.Helper()
	conn, err := net.DialUnix("unix", nil, &net.UnixAddr{Name: socketPath, Net: "unix"})
	if err != nil {
		t.Fatalf("connect to service: %v", err)
	}
	defer conn.Close()
	if err := conn.SetDeadline(time.Now().Add(3 * time.Second)); err != nil {
		t.Fatalf("set service request deadline: %v", err)
	}
	if _, err := conn.Write(append([]byte(raw), '\n')); err != nil {
		t.Fatalf("write service request: %v", err)
	}
	line, err := bufio.NewReader(conn).ReadBytes('\n')
	if err != nil {
		t.Fatalf("read service response: %v", err)
	}
	var response Response
	if err := json.Unmarshal(line, &response); err != nil {
		t.Fatalf("decode service response %q: %v", line, err)
	}
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(line, &fields); err != nil {
		t.Fatalf("decode response fields %q: %v", line, err)
	}
	return response, fields
}
