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
	responseOp := "protocol"
	opName, hasOperationString := jsonStringValue(top["op"])
	if hasOperationString && knownOperation(opName) {
		responseOp = opName
	}
	var version int64
	if !parseIntField(top["version"], &version) {
		return response(responseOp, "", "error", "invalid_request", emptyData()), false
	}
	if version != 1 {
		return response(responseOp, "", "error", "unsupported_version", emptyData()), false
	}
	if len(top["op"]) == 0 || !hasOperationString {
		return response("protocol", "", "error", "invalid_request", emptyData()), false
	}
	if !knownOperation(opName) {
		return response("protocol", "", "error", "unknown_operation", emptyData()), false
	}
	requestID := ""
	responseID := ""
	if isKeyOperation(opName) {
		if len(top["request_id"]) == 0 {
			return response(opName, "", "error", "invalid_request", emptyData()), false
		}
		var validType bool
		requestID, validType = jsonStringValue(top["request_id"])
		if !validType {
			return response(opName, "", "error", "invalid_request", emptyData()), false
		}
		if requestIDPattern.MatchString(requestID) {
			responseID = requestID
		}
	} else if len(top["request_id"]) != 0 {
		return response(opName, "", "error", "invalid_request", emptyData()), false
	}
	allowedTopLevelKeys := []string{"version", "type", "op", "data"}
	allowedTopLevelKeys = append(allowedTopLevelKeys, optionalRequestKey(opName)...)
	if !onlyKeys(top, allowedTopLevelKeys...) || len(top["type"]) == 0 || len(top["data"]) == 0 {
		return response(opName, responseID, "error", "invalid_request", emptyData()), false
	}
	var messageType string
	if messageType, _ = jsonStringValue(top["type"]); messageType != "request" {
		return response(opName, responseID, "error", "invalid_request", emptyData()), false
	}
	var rawData map[string]json.RawMessage
	if err := json.Unmarshal(top["data"], &rawData); err != nil || rawData == nil {
		return response(opName, responseID, "error", "invalid_request", emptyData()), false
	}

	state, data, code, responseID, malformed := s.dispatch(opName, requestID, rawData)
	if malformed {
		return response(opName, responseID, "error", code, emptyData()), false
	}
	return response(opName, responseID, state, code, data), false
}

func (s *Server) dispatch(op, requestID string, data map[string]json.RawMessage) (string, map[string]any, string, string, bool) {
	responseID := ""
	if isKeyOperation(op) && requestIDPattern.MatchString(requestID) {
		responseID = requestID
	}
	invalid := func(code string, includeID bool) (string, map[string]any, string, string, bool) {
		if !includeID {
			responseID = ""
		}
		return "error", emptyData(), code, responseID, false
	}
	if op == "lease.status" && len(data) == 1 && onlyKeys(data, "request_id") {
		lookupID, validType := jsonStringValue(data["request_id"])
		if !validType {
			return invalid("invalid_request", false)
		}
		if !requestIDPattern.MatchString(lookupID) {
			return invalid("invalid_request_id", false)
		}
		responseID = lookupID
		state, result, code := s.store.Status(lookupID, "")
		return state, result, code, responseID, false
	}
	if op == "lease.status" && len(data) == 1 && onlyKeys(data, "lease_id") {
		lookupID, validType := jsonStringValue(data["lease_id"])
		if !validType {
			return invalid("invalid_request", false)
		}
		if !leaseIDPattern.MatchString(lookupID) {
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
		var validType bool
		input.Tag, validType = jsonStringValue(data["tag"])
		if !validType {
			return invalid("invalid_request", true)
		}
		if !parseIntField(data["duration_ns"], &input.DurationNS) {
			return invalid("invalid_request", true)
		}
		input.WaitMode, validType = jsonStringValue(data["wait_mode"])
		if !validType {
			return invalid("invalid_request", true)
		}
		var waitTimeout int64
		waitTimeoutPresent := len(data["wait_timeout_ns"]) != 0
		if waitTimeoutPresent && !parseIntField(data["wait_timeout_ns"], &waitTimeout) {
			return invalid("invalid_request", true)
		}
		if !requestIDPattern.MatchString(requestID) {
			return invalid("invalid_request_id", false)
		}
		if !tagIDPattern.MatchString(input.Tag) {
			return invalid("invalid_tag", true)
		}
		if input.DurationNS <= 0 {
			return invalid("invalid_duration", true)
		}
		if input.WaitMode != "wait" && input.WaitMode != "no_wait" {
			return invalid("invalid_wait_mode", true)
		}
		if input.WaitMode == "wait" {
			if !waitTimeoutPresent {
				return invalid("invalid_wait_timeout", true)
			}
			if waitTimeout <= 0 {
				return invalid("invalid_wait_timeout", true)
			}
			input.WaitTimeoutNS = &waitTimeout
		} else if waitTimeoutPresent {
			return invalid("invalid_wait_timeout", true)
		}
		input.RequestID = requestID
		state, result, code := s.store.Acquire(input)
		return state, result, code, responseID, false
	case "resource.add":
		if !onlyKeys(data, "tag", "resource_id", "mode") || len(data) != 3 {
			return invalid("invalid_request", false)
		}
		var tag, id, mode string
		var validType bool
		tag, validType = jsonStringValue(data["tag"])
		if !validType {
			return invalid("invalid_request", false)
		}
		id, validType = jsonStringValue(data["resource_id"])
		if !validType {
			return invalid("invalid_request", false)
		}
		mode, validType = jsonStringValue(data["mode"])
		if !validType {
			return invalid("invalid_request", false)
		}
		if !tagIDPattern.MatchString(tag) {
			return invalid("invalid_tag", false)
		}
		if !tagIDPattern.MatchString(id) {
			return invalid("invalid_resource_id", false)
		}
		if mode != "manual" && mode != "command" {
			return invalid("invalid_pool_mode", false)
		}
		state, result, code := s.store.AddResource(tag, id, mode)
		return state, result, code, "", false
	case "resource.disable", "resource.enable":
		if !onlyKeys(data, "resource_id") || len(data) != 1 {
			return invalid("invalid_request", false)
		}
		id, code := parseResourceID(data["resource_id"])
		if code != "" {
			return invalid(code, false)
		}
		state, result, code := s.store.SetResource(op, id, false)
		return state, result, code, "", false
	case "resource.recover":
		if !onlyKeys(data, "resource_id", "confirm_stopped") || len(data) != 2 {
			return invalid("invalid_request", false)
		}
		id, validType := jsonStringValue(data["resource_id"])
		if !validType {
			return invalid("invalid_request", false)
		}
		var confirm bool
		if !jsonBoolValue(data["confirm_stopped"], &confirm) {
			return invalid("invalid_request", false)
		}
		if !tagIDPattern.MatchString(id) {
			return invalid("invalid_resource_id", false)
		}
		if !confirm {
			return invalid("invalid_request", false)
		}
		state, result, code := s.store.SetResource(op, id, true)
		return state, result, code, "", false
	case "lease.release":
		if !onlyKeys(data, "lease_id") || len(data) != 1 {
			return invalid("invalid_request", false)
		}
		id, validType := jsonStringValue(data["lease_id"])
		if !validType {
			return invalid("invalid_request", false)
		}
		if !leaseIDPattern.MatchString(id) {
			return invalid("invalid_lease_id", false)
		}
		state, result, code := s.store.Release(id)
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
		if !parseIntField(data["limit"], &limit) || limit < 1 || limit > 100 {
			return invalid("invalid_request", false)
		}
		var before *int64
		if raw := data["before_sequence"]; len(raw) != 0 {
			var value int64
			if !parseIntField(raw, &value) || value <= 0 {
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
		if !requestIDPattern.MatchString(requestID) {
			return invalid("invalid_request_id", false)
		}
		if op == "lease.wait" {
			state, result, code := s.store.Wait(requestID)
			return state, result, code, responseID, false
		}
		state, result, code := s.store.Cancel(requestID)
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

func jsonStringValue(raw json.RawMessage) (string, bool) {
	trimmed := bytes.TrimSpace(raw)
	if len(trimmed) == 0 || trimmed[0] != '"' {
		return "", false
	}
	var value string
	if err := json.Unmarshal(trimmed, &value); err != nil {
		return "", false
	}
	return value, true
}

func jsonBoolValue(raw json.RawMessage, target *bool) bool {
	trimmed := bytes.TrimSpace(raw)
	if !bytes.Equal(trimmed, []byte("true")) && !bytes.Equal(trimmed, []byte("false")) {
		return false
	}
	return json.Unmarshal(trimmed, target) == nil
}

func parseIntField(raw json.RawMessage, target *int64) bool {
	trimmed := bytes.TrimSpace(raw)
	if !integerPattern.Match(trimmed) {
		return false
	}
	return strictInt(trimmed, target) == nil
}

func parseResourceID(raw json.RawMessage) (string, string) {
	id, validType := jsonStringValue(raw)
	if !validType {
		return "", "invalid_request"
	}
	if !tagIDPattern.MatchString(id) {
		return "", "invalid_resource_id"
	}
	return id, ""
}
