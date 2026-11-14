package lease

import (
	"bufio"
	"bytes"
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
	encoded, err := json.Marshal(request)
	if err != nil || len(encoded) > maxFrameBytes {
		return response(request.Op, request.RequestID, "error", "invalid_request", emptyData()), "invalid_request"
	}
	encoded = append(encoded, '\n')
	conn, err := net.DialTimeout("unix", socketPath, 3*time.Second)
	if err != nil {
		return response(request.Op, request.RequestID, "error", "daemon_unavailable", emptyData()), "daemon_unavailable"
	}
	defer conn.Close()
	written := 0
	for written < len(encoded) {
		n, writeErr := conn.Write(encoded[written:])
		written += n
		if writeErr != nil {
			code := "result_unknown"
			if written == 0 {
				code = "daemon_unavailable"
			}
			return response(request.Op, request.RequestID, "error", code, emptyData()), code
		}
		if n == 0 {
			return response(request.Op, request.RequestID, "error", "result_unknown", emptyData()), "result_unknown"
		}
	}
	reader := bufio.NewReaderSize(conn, maxFrameBytes+2)
	line, err := reader.ReadSlice('\n')
	if err != nil || len(line) > maxFrameBytes+1 {
		return response(request.Op, request.RequestID, "error", "result_unknown", emptyData()), "result_unknown"
	}
	line = line[:len(line)-1]
	if len(line) > 0 && line[len(line)-1] == '\r' {
		line = line[:len(line)-1]
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
	return result, ""
}
