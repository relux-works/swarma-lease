package lease

import (
	"bufio"
	"context"
	"encoding/json"
	"io"
	"net"
	"path/filepath"
	"testing"
	"time"
)

func listenTestSocket(t *testing.T) (*net.UnixListener, string) {
	t.Helper()
	path := filepath.Join(shortTestDir(t), "service.sock")
	listener, err := net.ListenUnix("unix", &net.UnixAddr{Name: path, Net: "unix"})
	if err != nil {
		t.Fatalf("listen test socket: %v", err)
	}
	t.Cleanup(func() { _ = listener.Close() })
	return listener, path
}

func readTestRequest(t *testing.T, conn *net.UnixConn) WireRequest {
	t.Helper()
	_ = conn.SetReadDeadline(time.Now().Add(3 * time.Second))
	line, err := bufio.NewReaderSize(conn, maxFrameBytes+2).ReadSlice('\n')
	if err != nil {
		t.Fatalf("read client request: %v", err)
	}
	var request WireRequest
	if err := json.Unmarshal(line[:len(line)-1], &request); err != nil {
		t.Fatalf("decode client request: %v", err)
	}
	return request
}

// TestExchangeWaitsForTheTerminalEventProves a queued acknowledgement is never exposed as the CLI result and the final event is bound to the original key.
func TestExchangeWaitsForTheTerminalEvent(t *testing.T) {
	listener, path := listenTestSocket(t)
	serverDone := make(chan struct{})
	go func() {
		defer close(serverDone)
		conn, err := listener.AcceptUnix()
		if err != nil {
			return
		}
		defer conn.Close()
		request := readTestRequest(t, conn)
		queued := response(request.Op, request.RequestID, "queued", "", map[string]any{"request": map[string]any{"request_id": request.RequestID}, "lease": nil, "resource": nil})
		_ = writeResponse(conn, queued)
		event := TerminalEvent{Version: 1, Type: "event", Name: "lease.granted", RequestID: request.RequestID, Sequence: 7, Data: map[string]any{"request": map[string]any{"request_id": request.RequestID, "state": "leased"}, "lease": map[string]any{"id": "00000000000000000000000000000001", "request_id": request.RequestID, "state": "active"}, "resource": map[string]any{"id": "qa-01"}}}
		_ = writeTerminalEvent(conn, event)
	}()
	request := WireRequest{Version: 1, Type: "request", Op: "lease.acquire", RequestID: "req-01", Data: map[string]any{"tag": "qa.pool", "duration_ns": int64(time.Second), "wait_mode": "wait", "wait_timeout_ns": int64(time.Minute)}}
	result, code := Exchange(path, request)
	<-serverDone
	if code != "" || result.State != "granted" || result.RequestID != "req-01" || result.Code != "" || result.Data["lease"].(map[string]any)["id"] != "00000000000000000000000000000001" {
		t.Fatalf("blocking exchange = (%#v, %q), want final grant", result, code)
	}
}

// TestExchangeDistinguishesTransportFailures proves an unsent request is daemon_unavailable, while a closed socket after receiving request bytes is result_unknown.
func TestExchangeDistinguishesTransportFailures(t *testing.T) {
	missing := filepath.Join(shortTestDir(t), "missing.sock")
	request := WireRequest{Version: 1, Type: "request", Op: "lease.acquire", RequestID: "req-01", Data: map[string]any{}}
	if result, code := Exchange(missing, request); code != "daemon_unavailable" || result.Code != "daemon_unavailable" || result.RequestID != request.RequestID {
		t.Fatalf("pre-send failure = (%#v, %q), want daemon_unavailable", result, code)
	}
	listener, path := listenTestSocket(t)
	serverDone := make(chan struct{})
	go func() {
		defer close(serverDone)
		conn, err := listener.AcceptUnix()
		if err != nil {
			return
		}
		_ = readTestRequest(t, conn)
		_ = conn.Close()
	}()
	if result, code := Exchange(path, request); code != "result_unknown" || result.Code != "result_unknown" || result.RequestID != request.RequestID {
		t.Fatalf("post-send failure = (%#v, %q), want result_unknown", result, code)
	}
	<-serverDone
}

// TestExchangeInterruptionLeavesQueuedWorkUntouched proves interruption after sending a wait request reports unknown attempt state and sends no second frame or implicit cancellation.
func TestExchangeInterruptionLeavesQueuedWorkUntouched(t *testing.T) {
	listener, path := listenTestSocket(t)
	queuedSent := make(chan struct{})
	serverDone := make(chan error, 1)
	go func() {
		conn, err := listener.AcceptUnix()
		if err != nil {
			serverDone <- err
			return
		}
		defer conn.Close()
		request := readTestRequest(t, conn)
		if err := writeResponse(conn, response(request.Op, request.RequestID, "queued", "", map[string]any{"request": map[string]any{}, "lease": nil, "resource": nil})); err != nil {
			serverDone <- err
			return
		}
		close(queuedSent)
		_ = conn.SetReadDeadline(time.Now().Add(3 * time.Second))
		_, readErr := bufio.NewReaderSize(conn, maxFrameBytes+2).ReadSlice('\n')
		if readErr == nil {
			serverDone <- io.ErrUnexpectedEOF
			return
		}
		serverDone <- nil
	}()
	ctx, cancel := context.WithCancel(context.Background())
	request := WireRequest{Version: 1, Type: "request", Op: "lease.acquire", RequestID: "interrupt-01", Data: map[string]any{"tag": "qa.pool", "duration_ns": int64(time.Second), "wait_mode": "wait", "wait_timeout_ns": int64(time.Minute)}}
	resultCh := make(chan Response, 1)
	codeCh := make(chan string, 1)
	go func() {
		result, code := ExchangeContext(ctx, path, request)
		resultCh <- result
		codeCh <- code
	}()
	<-queuedSent
	cancel()
	result, code := <-resultCh, <-codeCh
	if err := <-serverDone; err != nil {
		t.Fatalf("server observed an extra frame: %v", err)
	}
	attempt := result.Data["attempt_state"]
	if code != "client_interrupted" || result.Code != "client_interrupted" || result.RequestID != request.RequestID || attempt != "unknown" {
		t.Fatalf("interrupted exchange = (%#v, %q), want unknown attempt", result, code)
	}
	_ = cancel
}
