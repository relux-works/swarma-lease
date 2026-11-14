package lease

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"net"
	"time"
)

type WireRequest struct {
	Version   int    `json:"version"`
	Type      string `json:"type"`
	Op        string `json:"op"`
	RequestID string `json:"request_id,omitempty"`
	Data      any    `json:"data"`
}

func Exchange(socketPath string, request WireRequest) (Response, string) {
	return ExchangeContext(context.Background(), socketPath, request)
}

func ExchangeContext(ctx context.Context, socketPath string, request WireRequest) (Response, string) {
	encoded, err := json.Marshal(request)
	if err != nil || len(encoded) > maxFrameBytes {
		return response(request.Op, request.RequestID, "error", "invalid_request", emptyData()), "invalid_request"
	}
	encoded = append(encoded, '\n')
	conn, err := (&net.Dialer{Timeout: 3 * time.Second}).DialContext(ctx, "unix", socketPath)
	if err != nil {
		if ctx.Err() != nil {
			return interrupted(request, 0), "client_interrupted"
		}
		return response(request.Op, request.RequestID, "error", "daemon_unavailable", emptyData()), "daemon_unavailable"
	}
	defer conn.Close()
	stopClose := context.AfterFunc(ctx, func() { _ = conn.Close() })
	defer stopClose()
	written := 0
	for written < len(encoded) {
		if ctx.Err() != nil {
			return interrupted(request, written), "client_interrupted"
		}
		n, writeErr := conn.Write(encoded[written:])
		written += n
		if writeErr != nil {
			if ctx.Err() != nil {
				return interrupted(request, written), "client_interrupted"
			}
			code := "result_unknown"
			if written == 0 {
				code = "daemon_unavailable"
			}
			return response(request.Op, request.RequestID, "error", code, emptyData()), code
		}
		if n == 0 {
			if ctx.Err() != nil {
				return interrupted(request, written), "client_interrupted"
			}
			return response(request.Op, request.RequestID, "error", "result_unknown", emptyData()), "result_unknown"
		}
	}
	reader := bufio.NewReaderSize(conn, maxFrameBytes+2)
	line, err := readFrame(reader)
	if err != nil {
		if ctx.Err() != nil {
			return interrupted(request, written), "client_interrupted"
		}
		return response(request.Op, request.RequestID, "error", "result_unknown", emptyData()), "result_unknown"
	}
	var result Response
	decoder := json.NewDecoder(bytes.NewReader(line))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&result); err != nil || result.Version != 1 || result.Type != "response" || result.Op != request.Op || result.Data == nil {
		return response(request.Op, request.RequestID, "error", "result_unknown", emptyData()), "result_unknown"
	}
	if request.RequestID != "" && result.RequestID != request.RequestID {
		return response(request.Op, request.RequestID, "error", "result_unknown", emptyData()), "result_unknown"
	}
	if result.State != "queued" || request.Op != "lease.acquire" && request.Op != "lease.wait" {
		return result, ""
	}
	line, err = readFrame(reader)
	if err != nil {
		if ctx.Err() != nil {
			return interrupted(request, written), "client_interrupted"
		}
		return response(request.Op, request.RequestID, "error", "result_unknown", emptyData()), "result_unknown"
	}
	var event TerminalEvent
	eventDecoder := json.NewDecoder(bytes.NewReader(line))
	eventDecoder.DisallowUnknownFields()
	if err := eventDecoder.Decode(&event); err != nil || event.Version != 1 || event.Type != "event" || event.RequestID != request.RequestID || event.Sequence <= 0 || event.Data == nil {
		return response(request.Op, request.RequestID, "error", "result_unknown", emptyData()), "result_unknown"
	}
	requestRecord, ok := event.Data["request"].(map[string]any)
	if !ok || requestRecord["request_id"] != request.RequestID || len(event.Data) != 3 {
		return response(request.Op, request.RequestID, "error", "result_unknown", emptyData()), "result_unknown"
	}
	switch event.Name {
	case "lease.granted":
		leaseRecord, leaseOK := event.Data["lease"].(map[string]any)
		if _, resourceOK := event.Data["resource"].(map[string]any); !resourceOK || !leaseOK || leaseRecord["request_id"] != request.RequestID {
			return response(request.Op, request.RequestID, "error", "result_unknown", emptyData()), "result_unknown"
		}
		switch requestRecord["state"] {
		case "leased":
			if leaseRecord["state"] != "active" {
				return response(request.Op, request.RequestID, "error", "result_unknown", emptyData()), "result_unknown"
			}
			return response(request.Op, request.RequestID, "granted", "", event.Data), ""
		case "released":
			return response(request.Op, request.RequestID, "error", "lease_released", event.Data), "lease_released"
		case "expired":
			return response(request.Op, request.RequestID, "error", "lease_expired", event.Data), "lease_expired"
		default:
			return response(request.Op, request.RequestID, "error", "result_unknown", emptyData()), "result_unknown"
		}
	case "request.timed_out":
		if event.Data["lease"] != nil || event.Data["resource"] != nil || requestRecord["state"] != "timed_out" {
			return response(request.Op, request.RequestID, "error", "result_unknown", emptyData()), "result_unknown"
		}
		return response(request.Op, request.RequestID, "error", "request_timed_out", event.Data), ""
	case "request.cancelled":
		if event.Data["lease"] != nil || event.Data["resource"] != nil || requestRecord["state"] != "cancelled" {
			return response(request.Op, request.RequestID, "error", "result_unknown", emptyData()), "result_unknown"
		}
		return response(request.Op, request.RequestID, "error", "request_cancelled", event.Data), ""
	default:
		return response(request.Op, request.RequestID, "error", "result_unknown", emptyData()), "result_unknown"
	}
}

func interrupted(request WireRequest, written int) Response {
	attemptState := "unknown"
	if written == 0 {
		attemptState = "not_sent"
	}
	return response(request.Op, request.RequestID, "error", "client_interrupted", map[string]any{"attempt_state": attemptState})
}

func readFrame(reader *bufio.Reader) ([]byte, error) {
	line, err := reader.ReadSlice('\n')
	if err != nil || len(line) > maxFrameBytes+1 {
		return nil, err
	}
	line = line[:len(line)-1]
	if len(line) > 0 && line[len(line)-1] == '\r' {
		line = line[:len(line)-1]
	}
	return line, nil
}
