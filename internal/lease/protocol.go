package lease

import (
	"bytes"
	"encoding/json"
	"io"
	"regexp"
	"strconv"
)

var (
	tagIDPattern     = regexp.MustCompile(`^[a-z0-9]+([._-][a-z0-9]+)*$`)
	requestIDPattern = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$`)
	leaseIDPattern   = regexp.MustCompile(`^[0-9a-f]{32}$`)
	integerPattern   = regexp.MustCompile(`^-?(0|[1-9][0-9]*)$`)
)

func validUUID(value string) bool {
	if len(value) != 36 || value[8] != '-' || value[13] != '-' || value[18] != '-' || value[23] != '-' {
		return false
	}
	for i, r := range value {
		if i == 8 || i == 13 || i == 18 || i == 23 {
			continue
		}
		if !((r >= '0' && r <= '9') || (r >= 'a' && r <= 'f')) {
			return false
		}
	}
	return true
}

func jsonObjectKeysUnique(raw []byte) bool {
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.UseNumber()
	first, err := decoder.Token()
	if err != nil || first != json.Delim('{') {
		return false
	}
	if err := scanObject(decoder); err != nil {
		return false
	}
	if _, err := decoder.Token(); err != io.EOF {
		return false
	}
	return true
}

func scanObject(decoder *json.Decoder) error {
	seen := make(map[string]struct{})
	for decoder.More() {
		keyToken, err := decoder.Token()
		if err != nil {
			return err
		}
		key, ok := keyToken.(string)
		if !ok {
			return io.ErrUnexpectedEOF
		}
		if _, duplicate := seen[key]; duplicate {
			return io.ErrUnexpectedEOF
		}
		seen[key] = struct{}{}
		if err := scanValue(decoder); err != nil {
			return err
		}
	}
	end, err := decoder.Token()
	if err != nil || end != json.Delim('}') {
		return io.ErrUnexpectedEOF
	}
	return nil
}

func scanArray(decoder *json.Decoder) error {
	for decoder.More() {
		if err := scanValue(decoder); err != nil {
			return err
		}
	}
	end, err := decoder.Token()
	if err != nil || end != json.Delim(']') {
		return io.ErrUnexpectedEOF
	}
	return nil
}

func scanValue(decoder *json.Decoder) error {
	token, err := decoder.Token()
	if err != nil {
		return err
	}
	if delimiter, ok := token.(json.Delim); ok {
		switch delimiter {
		case '{':
			return scanObject(decoder)
		case '[':
			return scanArray(decoder)
		default:
			return io.ErrUnexpectedEOF
		}
	}
	return nil
}

func (s *Server) handleJSON(raw []byte) (Response, bool) {
	var top map[string]json.RawMessage
	if err := json.Unmarshal(raw, &top); err != nil || top == nil {
		return response("protocol", "", "error", "invalid_json", emptyData()), true
	}
	var opName string
	if len(top["op"]) == 0 || json.Unmarshal(top["op"], &opName) != nil {
		return response("protocol", "", "error", "invalid_request", emptyData()), false
	}
	known := knownOperation(opName)
	responseOp := opName
	if !known {
		responseOp = "protocol"
	}
	requestID := ""
	if len(top["version"]) == 0 || len(top["type"]) == 0 || len(top["data"]) == 0 {
		return response(responseOp, "", "error", "invalid_request", emptyData()), false
	}
	if isKeyOperation(opName) {
		if len(top["request_id"]) == 0 {
			return response(opName, "", "error", "invalid_request", emptyData()), false
		}
		if json.Unmarshal(top["request_id"], &requestID) != nil || !requestIDPattern.MatchString(requestID) {
			return response(opName, "", "error", "invalid_request_id", emptyData()), false
		}
	} else if len(top["request_id"]) != 0 {
		return response(opName, "", "error", "invalid_request", emptyData()), false
	}
	if !known {
		return response("protocol", "", "error", "unknown_operation", emptyData()), false
	}
	allowedTopLevelKeys := []string{"version", "type", "op", "data"}
	allowedTopLevelKeys = append(allowedTopLevelKeys, optionalRequestKey(opName)...)
	if !onlyKeys(top, allowedTopLevelKeys...) {
		return response(opName, requestID, "error", "invalid_request", emptyData()), false
	}
	var version int64
	if err := strictInt(top["version"], &version); err != nil {
		return response(opName, requestID, "error", "invalid_request", emptyData()), false
	}
	var messageType string
	if json.Unmarshal(top["type"], &messageType) != nil || messageType != "request" {
		return response(opName, requestID, "error", "invalid_request", emptyData()), false
	}
	if version != 1 {
		return response(opName, requestID, "error", "unsupported_version", emptyData()), false
	}
	var rawData map[string]json.RawMessage
	if err := json.Unmarshal(top["data"], &rawData); err != nil || rawData == nil {
		return response(opName, requestID, "error", "invalid_request", emptyData()), false
	}

	state, data, code, responseID, malformed := s.dispatch(opName, requestID, rawData)
	if malformed {
		return response(opName, responseID, "error", code, emptyData()), false
	}
	return response(opName, responseID, state, code, data), false
}

func (s *Server) dispatch(op, requestID string, data map[string]json.RawMessage) (string, map[string]any, string, string, bool) {
	responseID := requestID
	invalid := func(code string, includeID bool) (string, map[string]any, string, string, bool) {
		if !includeID {
			responseID = ""
		}
		return "error", emptyData(), code, responseID, false
	}
	if op == "lease.status" && len(data) == 1 && onlyKeys(data, "request_id") {
		var lookupID string
		if json.Unmarshal(data["request_id"], &lookupID) != nil || !requestIDPattern.MatchString(lookupID) {
			return invalid("invalid_request_id", false)
		}
		responseID = lookupID
		state, result, code := s.store.Status(lookupID, "")
		return state, result, code, responseID, false
	}
	if op == "lease.status" && len(data) == 1 && onlyKeys(data, "lease_id") {
		var lookupID string
		if json.Unmarshal(data["lease_id"], &lookupID) != nil || !leaseIDPattern.MatchString(lookupID) {
			return invalid("invalid_lease_id", false)
		}
		state, result, code := s.store.Status("", lookupID)
		return state, result, code, "", false
	}
	switch op {
	case "lease.acquire":
		if !hasOnlyRequiredOptional(data, []string{"tag", "duration_ns", "wait_mode"}, []string{"wait_timeout_ns"}) {
			return invalid("invalid_request", true)
		}
		var input AcquireInput
		if json.Unmarshal(data["tag"], &input.Tag) != nil || !tagIDPattern.MatchString(input.Tag) {
			return invalid("invalid_tag", true)
		}
		if strictInt(data["duration_ns"], &input.DurationNS) != nil || input.DurationNS <= 0 {
			return invalid("invalid_duration", true)
		}
		if json.Unmarshal(data["wait_mode"], &input.WaitMode) != nil || (input.WaitMode != "wait" && input.WaitMode != "no_wait") {
			return invalid("invalid_wait_mode", true)
		}
		if input.WaitMode == "wait" {
			if len(data["wait_timeout_ns"]) == 0 || strictInt(data["wait_timeout_ns"], new(int64)) != nil {
				return invalid("invalid_wait_timeout", true)
			}
			var wait int64
			if strictInt(data["wait_timeout_ns"], &wait) != nil || wait <= 0 {
				return invalid("invalid_wait_timeout", true)
			}
			input.WaitTimeoutNS = &wait
		} else if len(data["wait_timeout_ns"]) != 0 {
			return invalid("invalid_wait_timeout", true)
		}
		input.RequestID = requestID
		sample, err := s.clock.Read()
		if err != nil {
			return invalid("clock_unavailable", true)
		}
		state, result, code := s.store.Acquire(input, sample)
		return state, result, code, responseID, false
	case "resource.add":
		if !onlyKeys(data, "tag", "resource_id", "mode") || len(data) != 3 {
			return invalid("invalid_request", false)
		}
		var tag, id, mode string
		if json.Unmarshal(data["tag"], &tag) != nil || !tagIDPattern.MatchString(tag) {
			return invalid("invalid_tag", false)
		}
		if json.Unmarshal(data["resource_id"], &id) != nil || !tagIDPattern.MatchString(id) {
			return invalid("invalid_resource_id", false)
		}
		if json.Unmarshal(data["mode"], &mode) != nil || (mode != "manual" && mode != "command") {
			return invalid("invalid_pool_mode", false)
		}
		sample, err := s.clock.Read()
		if err != nil {
			return invalid("clock_unavailable", false)
		}
		state, result, code := s.store.AddResource(tag, id, mode, sample)
		return state, result, code, "", false
	case "resource.disable", "resource.enable":
		if !onlyKeys(data, "resource_id") || len(data) != 1 {
			return invalid("invalid_request", false)
		}
		id, code := parseResourceID(data["resource_id"])
		if code != "" {
			return invalid(code, false)
		}
		sample, err := s.clock.Read()
		if err != nil {
			return invalid("clock_unavailable", false)
		}
		state, result, code := s.store.SetResource(op, id, false, sample)
		return state, result, code, "", false
	case "resource.recover":
		if !onlyKeys(data, "resource_id", "confirm_stopped") || len(data) != 2 {
			return invalid("invalid_request", false)
		}
		id, code := parseResourceID(data["resource_id"])
		if code != "" {
			return invalid(code, false)
		}
		var confirm bool
		if json.Unmarshal(data["confirm_stopped"], &confirm) != nil || !confirm {
			return invalid("invalid_request", false)
		}
		sample, err := s.clock.Read()
		if err != nil {
			return invalid("clock_unavailable", false)
		}
		state, result, code := s.store.SetResource(op, id, true, sample)
		return state, result, code, "", false
	case "lease.release":
		if !onlyKeys(data, "lease_id") || len(data) != 1 {
			return invalid("invalid_request", false)
		}
		var id string
		if json.Unmarshal(data["lease_id"], &id) != nil || !leaseIDPattern.MatchString(id) {
			return invalid("invalid_lease_id", false)
		}
		sample, err := s.clock.Read()
		if err != nil {
			return invalid("clock_unavailable", false)
		}
		state, result, code := s.store.Release(id, sample)
		return state, result, code, "", false
	case "service.status":
		if len(data) != 0 {
			return invalid("invalid_request", false)
		}
		result, code := s.store.ServiceStatus()
		state := "ok"
		if code != "" {
			state = "error"
		}
		return state, result, code, "", false
	case "audit.list":
		if !hasOnlyRequiredOptional(data, []string{"limit"}, []string{"before_sequence"}) {
			return invalid("invalid_request", false)
		}
		var limit int64
		if strictInt(data["limit"], &limit) != nil || limit < 1 || limit > 100 {
			return invalid("invalid_request", false)
		}
		var before *int64
		if raw := data["before_sequence"]; len(raw) != 0 {
			var value int64
			if strictInt(raw, &value) != nil || value <= 0 {
				return invalid("invalid_request", false)
			}
			before = &value
		}
		result, code := s.store.History(limit, before)
		state := "ok"
		if code != "" {
			state = "error"
		}
		return state, result, code, "", false
	case "lease.wait", "lease.cancel":
		if len(data) != 0 {
			return invalid("invalid_request", true)
		}
		sample, err := s.clock.Read()
		if err != nil {
			return invalid("clock_unavailable", true)
		}
		if op == "lease.wait" {
			state, result, code := s.store.Wait(requestID, sample)
			return state, result, code, responseID, false
		}
		state, result, code := s.store.Cancel(requestID, sample)
		return state, result, code, responseID, false
	case "lease.status":
		return invalid("invalid_request", false)
	default:
		return "error", emptyData(), "unknown_operation", "", false
	}
}

func knownOperation(op string) bool {
	switch op {
	case "lease.acquire", "lease.wait", "lease.cancel", "lease.release", "lease.status", "resource.add", "resource.disable", "resource.enable", "resource.recover", "service.status", "audit.list":
		return true
	default:
		return false
	}
}

func isKeyOperation(op string) bool {
	return op == "lease.acquire" || op == "lease.wait" || op == "lease.cancel"
}

func optionalRequestKey(op string) []string {
	if isKeyOperation(op) {
		return []string{"request_id"}
	}
	return nil
}

func onlyKeys(values map[string]json.RawMessage, required ...string) bool {
	want := make(map[string]struct{}, len(required))
	for _, key := range required {
		want[key] = struct{}{}
	}
	for key := range values {
		if _, exists := want[key]; !exists {
			return false
		}
	}
	return true
}

func hasOnlyRequiredOptional(values map[string]json.RawMessage, required, optional []string) bool {
	want := make(map[string]struct{}, len(required)+len(optional))
	for _, key := range append(append([]string{}, required...), optional...) {
		want[key] = struct{}{}
	}
	for _, key := range required {
		if len(values[key]) == 0 {
			return false
		}
	}
	for key := range values {
		if _, exists := want[key]; !exists {
			return false
		}
	}
	return true
}

func strictInt(raw json.RawMessage, target *int64) error {
	text := string(raw)
	if !integerPattern.MatchString(text) {
		return strconv.ErrSyntax
	}
	value, err := strconv.ParseInt(text, 10, 64)
	if err != nil {
		return strconv.ErrRange
	}
	*target = value
	return nil
}

func parseResourceID(raw json.RawMessage) (string, string) {
	var id string
	if json.Unmarshal(raw, &id) != nil || !tagIDPattern.MatchString(id) {
		return "", "invalid_resource_id"
	}
	return id, ""
}
