package cli

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/relux-works/swarma-lease/internal/lease"
)

type commandError struct {
	op        string
	requestID string
	code      string
}

type parsedFlags struct {
	values map[string]string
	bools  map[string]bool
}

func Run(args []string, stdout, stderr io.Writer) int {
	if len(args) > 0 && args[0] == "serve" {
		if len(args) != 1 {
			return writeCLIError(stdout, "cli", "", "invalid_request")
		}
		return runServe(stderr)
	}
	socket, err := lease.DefaultSocketPath()
	if err != nil {
		return writeCLIError(stdout, "cli", "", lease.ErrorCode(err))
	}
	return RunAtSocket(args, stdout, stderr, socket)
}

func RunAtSocket(args []string, stdout, stderr io.Writer, socket string) int {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	return runAtSocketContext(ctx, args, stdout, stderr, socket)
}

func runAtSocketContext(ctx context.Context, args []string, stdout, stderr io.Writer, socket string) int {
	_ = stderr
	request, parseErr := buildRequest(args)
	if parseErr != nil {
		return writeCLIError(stdout, parseErr.op, parseErr.requestID, parseErr.code)
	}
	result, _ := lease.ExchangeContext(ctx, socket, request)
	return writeResponse(stdout, result)
}

func runServe(stderr io.Writer) int {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	server, err := lease.Start(lease.Config{})
	if err != nil {
		writeServiceLog(stderr, "error", "", lease.ErrorCode(err))
		return 69
	}
	writeServiceLog(stderr, "info", "service_started", "")
	err = server.Serve(ctx)
	closeErr := server.Close()
	if err != nil {
		writeServiceLog(stderr, "error", "", lease.ErrorCode(err))
		return 69
	}
	if closeErr != nil {
		writeServiceLog(stderr, "error", "", lease.ErrorCode(closeErr))
		return 69
	}
	writeServiceLog(stderr, "info", "service_stopped", "")
	return 0
}

func writeServiceLog(stderr io.Writer, severity, event, code string) {
	if code != "" {
		_, _ = fmt.Fprintf(stderr, "timestamp=%s severity=%s code=%s\n", time.Now().UTC().Format(time.RFC3339Nano), severity, code)
		return
	}
	_, _ = fmt.Fprintf(stderr, "timestamp=%s severity=%s event=%s\n", time.Now().UTC().Format(time.RFC3339Nano), severity, event)
}

func buildRequest(args []string) (lease.WireRequest, *commandError) {
	if len(args) < 2 {
		return lease.WireRequest{}, &commandError{op: "cli", code: "invalid_request"}
	}
	request := lease.WireRequest{Version: 1, Type: "request", Data: map[string]any{}}
	var valueFlags, boolFlags []string
	var mode string
	switch {
	case args[0] == "resource" && args[1] == "add":
		request.Op, mode = "resource.add", "resource-add"
		valueFlags, boolFlags = []string{"--tag", "--id", "--mode"}, []string{"--json"}
	case args[0] == "resource" && (args[1] == "disable" || args[1] == "enable"):
		request.Op, mode = "resource."+args[1], "resource-one"
		valueFlags, boolFlags = []string{"--id"}, []string{"--json"}
	case args[0] == "resource" && args[1] == "recover":
		request.Op, mode = "resource.recover", "resource-recover"
		valueFlags, boolFlags = []string{"--id"}, []string{"--confirm-stopped", "--json"}
	case args[0] == "lease" && args[1] == "request":
		request.Op, mode = "lease.acquire", "lease-request"
		valueFlags, boolFlags = []string{"--tag", "--duration", "--wait-timeout", "--request-id"}, []string{"--wait", "--no-wait", "--json"}
	case args[0] == "lease" && (args[1] == "wait" || args[1] == "cancel"):
		request.Op, mode = "lease."+args[1], "lease-key"
		valueFlags, boolFlags = []string{"--request-id"}, []string{"--json"}
	case args[0] == "lease" && args[1] == "release":
		request.Op, mode = "lease.release", "lease-release"
		valueFlags, boolFlags = []string{"--id"}, []string{"--json"}
	case args[0] == "lease" && args[1] == "status":
		request.Op, mode = "lease.status", "lease-status"
		valueFlags, boolFlags = []string{"--request-id", "--id"}, []string{"--json"}
	case args[0] == "service" && args[1] == "status":
		request.Op, mode = "service.status", "no-fields"
		boolFlags = []string{"--json"}
	case args[0] == "history":
		request.Op, mode = "audit.list", "history"
		valueFlags, boolFlags = []string{"--limit", "--before-sequence"}, []string{"--json"}
	default:
		return lease.WireRequest{}, &commandError{op: "cli", code: "invalid_request"}
	}
	flagArgs := args[2:]
	if mode == "history" {
		flagArgs = args[1:]
	}
	flags, err := parseFlags(flagArgs, valueFlags, boolFlags)
	if err != nil || !flags.bools["--json"] {
		return lease.WireRequest{}, &commandError{op: request.Op, code: "invalid_request"}
	}
	safeRequestID := ""
	if mode == "lease-request" {
		if candidate := flags.values["--request-id"]; validRequestID(candidate) {
			safeRequestID = candidate
		}
	}
	data := make(map[string]any)
	switch mode {
	case "resource-add":
		tag := flags.values["--tag"]
		id := flags.values["--id"]
		poolMode := flags.values["--mode"]
		if !validName(tag) {
			return lease.WireRequest{}, &commandError{op: request.Op, code: "invalid_tag"}
		}
		if !validName(id) {
			return lease.WireRequest{}, &commandError{op: request.Op, code: "invalid_resource_id"}
		}
		if poolMode != "manual" && poolMode != "command" {
			return lease.WireRequest{}, &commandError{op: request.Op, code: "invalid_pool_mode"}
		}
		data["tag"], data["resource_id"], data["mode"] = tag, id, poolMode
	case "resource-one":
		id := flags.values["--id"]
		if !validName(id) {
			return lease.WireRequest{}, &commandError{op: request.Op, code: "invalid_resource_id"}
		}
		data["resource_id"] = id
	case "resource-recover":
		id := flags.values["--id"]
		if !validName(id) {
			return lease.WireRequest{}, &commandError{op: request.Op, code: "invalid_resource_id"}
		}
		if !flags.bools["--confirm-stopped"] {
			return lease.WireRequest{}, &commandError{op: request.Op, code: "invalid_request"}
		}
		data["resource_id"], data["confirm_stopped"] = id, true
	case "lease-request":
		tag := flags.values["--tag"]
		if !validName(tag) {
			return lease.WireRequest{}, &commandError{op: request.Op, requestID: safeRequestID, code: "invalid_tag"}
		}
		duration, err := ParseDuration(flags.values["--duration"])
		if err != nil {
			return lease.WireRequest{}, &commandError{op: request.Op, requestID: safeRequestID, code: "invalid_duration"}
		}
		if flags.bools["--wait"] == flags.bools["--no-wait"] {
			return lease.WireRequest{}, &commandError{op: request.Op, requestID: safeRequestID, code: "invalid_wait_mode"}
		}
		waitMode := "no_wait"
		if flags.bools["--wait"] {
			waitMode = "wait"
		}
		var waitTimeout *int64
		if waitMode == "wait" {
			value, parseErr := ParseDuration(flags.values["--wait-timeout"])
			if parseErr != nil {
				return lease.WireRequest{}, &commandError{op: request.Op, requestID: safeRequestID, code: "invalid_wait_timeout"}
			}
			waitTimeout = &value
		} else if _, present := flags.values["--wait-timeout"]; present {
			return lease.WireRequest{}, &commandError{op: request.Op, requestID: safeRequestID, code: "invalid_wait_timeout"}
		}
		requestID := flags.values["--request-id"]
		if requestID == "" {
			requestID, err = newRequestID()
			if err != nil {
				return lease.WireRequest{}, &commandError{op: request.Op, code: "storage_unavailable"}
			}
		} else if !validRequestID(requestID) {
			return lease.WireRequest{}, &commandError{op: request.Op, code: "invalid_request_id"}
		}
		request.RequestID = requestID
		data["tag"], data["duration_ns"], data["wait_mode"] = tag, duration, waitMode
		if waitTimeout != nil {
			data["wait_timeout_ns"] = *waitTimeout
		}
	case "lease-key":
		requestID := flags.values["--request-id"]
		if !validRequestID(requestID) {
			return lease.WireRequest{}, &commandError{op: request.Op, code: "invalid_request_id"}
		}
		request.RequestID = requestID
	case "lease-release":
		id := flags.values["--id"]
		if !validLeaseID(id) {
			return lease.WireRequest{}, &commandError{op: request.Op, code: "invalid_lease_id"}
		}
		data["lease_id"] = id
	case "lease-status":
		requestID, hasRequestID := flags.values["--request-id"]
		leaseID, hasLeaseID := flags.values["--id"]
		if hasRequestID == hasLeaseID {
			return lease.WireRequest{}, &commandError{op: request.Op, code: "invalid_request"}
		}
		if hasRequestID {
			if !validRequestID(requestID) {
				return lease.WireRequest{}, &commandError{op: request.Op, code: "invalid_request_id"}
			}
			data["request_id"] = requestID
		} else {
			if !validLeaseID(leaseID) {
				return lease.WireRequest{}, &commandError{op: request.Op, code: "invalid_lease_id"}
			}
			data["lease_id"] = leaseID
		}
	case "no-fields":
	case "history":
		limit := int64(20)
		if raw, present := flags.values["--limit"]; present {
			parsed, err := strconv.ParseInt(raw, 10, 64)
			if err != nil || parsed < 1 || parsed > 100 {
				return lease.WireRequest{}, &commandError{op: request.Op, code: "invalid_request"}
			}
			limit = parsed
		}
		data["limit"] = limit
		if raw, present := flags.values["--before-sequence"]; present {
			parsed, err := strconv.ParseInt(raw, 10, 64)
			if err != nil || parsed <= 0 {
				return lease.WireRequest{}, &commandError{op: request.Op, code: "invalid_request"}
			}
			data["before_sequence"] = parsed
		}
	}
	request.Data = data
	return request, nil
}

func parseFlags(args, valueNames, boolNames []string) (parsedFlags, error) {
	valuesAllowed := make(map[string]bool, len(valueNames))
	boolAllowed := make(map[string]bool, len(boolNames))
	for _, name := range valueNames {
		valuesAllowed[name] = true
	}
	for _, name := range boolNames {
		boolAllowed[name] = true
	}
	result := parsedFlags{values: make(map[string]string), bools: make(map[string]bool)}
	for i := 0; i < len(args); i++ {
		name, inline, hasInline := strings.Cut(args[i], "=")
		if boolAllowed[name] {
			if hasInline || result.bools[name] {
				return parsedFlags{}, errors.New("invalid flags")
			}
			result.bools[name] = true
			continue
		}
		if !valuesAllowed[name] || result.hasValue(name) {
			return parsedFlags{}, errors.New("invalid flags")
		}
		value := inline
		if !hasInline {
			if i+1 >= len(args) {
				return parsedFlags{}, errors.New("missing value")
			}
			i++
			value = args[i]
		}
		if value == "" {
			return parsedFlags{}, errors.New("missing value")
		}
		result.values[name] = value
	}
	return result, nil
}

func (f parsedFlags) hasValue(name string) bool {
	_, exists := f.values[name]
	return exists
}

func ParseDuration(input string) (int64, error) {
	if input == "" {
		return 0, errors.New("invalid duration")
	}
	unit := ""
	for _, suffix := range []string{"ns", "us", "ms", "s", "m", "h"} {
		if strings.HasSuffix(input, suffix) {
			unit = suffix
			break
		}
	}
	if unit == "" {
		return 0, errors.New("invalid duration")
	}
	digits := strings.TrimSuffix(input, unit)
	if digits == "" {
		return 0, errors.New("invalid duration")
	}
	for _, r := range digits {
		if r < '0' || r > '9' {
			return 0, errors.New("invalid duration")
		}
	}
	amount, err := strconv.ParseInt(digits, 10, 64)
	if err != nil || amount <= 0 {
		return 0, errors.New("invalid duration")
	}
	unitNS := map[string]int64{"ns": 1, "us": 1_000, "ms": 1_000_000, "s": 1_000_000_000, "m": 60_000_000_000, "h": 3_600_000_000_000}[unit]
	if amount > math.MaxInt64/unitNS {
		return 0, errors.New("invalid duration")
	}
	return amount * unitNS, nil
}

func newRequestID() (string, error) {
	var raw [16]byte
	if _, err := rand.Read(raw[:]); err != nil {
		return "", err
	}
	return hex.EncodeToString(raw[:]), nil
}

func validName(value string) bool { return validTagID(value) }

func validRequestID(value string) bool {
	if len(value) < 1 || len(value) > 128 {
		return false
	}
	first := value[0]
	if !((first >= 'A' && first <= 'Z') || (first >= 'a' && first <= 'z') || (first >= '0' && first <= '9')) {
		return false
	}
	for i := range value {
		c := value[i]
		if !((c >= 'A' && c <= 'Z') || (c >= 'a' && c <= 'z') || (c >= '0' && c <= '9') || c == '.' || c == '_' || c == ':' || c == '-') {
			return false
		}
	}
	return true
}

func validLeaseID(value string) bool {
	if len(value) != 32 {
		return false
	}
	for i := range value {
		c := value[i]
		if !((c >= '0' && c <= '9') || (c >= 'a' && c <= 'f')) {
			return false
		}
	}
	return true
}

func validTagID(value string) bool {
	if value == "" {
		return false
	}
	previousSeparator := true
	for i := range value {
		c := value[i]
		if c >= 'a' && c <= 'z' || c >= '0' && c <= '9' {
			previousSeparator = false
			continue
		}
		if (c == '.' || c == '_' || c == '-') && !previousSeparator && i < len(value)-1 {
			previousSeparator = true
			continue
		}
		return false
	}
	return !previousSeparator
}

func writeCLIError(stdout io.Writer, op, requestID, code string) int {
	return writeResponse(stdout, lease.Response{Version: 1, Type: "response", Op: op, RequestID: requestID, State: "error", Data: map[string]any{}, Code: code})
}

func writeResponse(stdout io.Writer, response lease.Response) int {
	encoded, err := json.Marshal(response)
	if err != nil {
		return 69
	}
	encoded = append(encoded, '\n')
	if _, err := stdout.Write(encoded); err != nil {
		return 69
	}
	return exitStatus(response.Code)
}

func exitStatus(code string) int {
	switch code {
	case "":
		return 0
	case "invalid_json", "frame_too_large", "unsupported_version", "unknown_operation", "invalid_request", "invalid_tag", "invalid_resource_id", "invalid_lease_id", "invalid_request_id", "invalid_duration", "invalid_wait_mode", "invalid_wait_timeout", "invalid_pool_mode":
		return 2
	case "unknown_tag", "unknown_resource", "unknown_lease", "unknown_request":
		return 3
	case "request_timed_out", "request_cancelled", "lease_released", "lease_expired":
		return 4
	case "duration_exceeds_max", "wait_exceeds_max", "adapter_required", "pool_disabled", "queue_sequence_exhausted", "request_id_conflict", "duplicate_resource_id", "pool_mode_mismatch", "invalid_transition", "request_not_cancellable":
		return 5
	case "no_wait_busy":
		return 75
	case "client_interrupted":
		return 130
	case "socket_path_too_long":
		return 69
	default:
		return 69
	}
}
