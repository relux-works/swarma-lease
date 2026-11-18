package lease

import (
	"encoding/json"
	"time"
)

const (
	maxLeaseNS        = int64(20 * time.Minute)
	maxWaitNS         = int64(4 * time.Hour)
	schemaV1Version   = int64(1)
	schemaV2Version   = int64(2)
	schemaVersion     = int64(3)
	schemaV1MaxWaitNS = int64(30 * time.Minute)
	dateLayout        = "2006-01-02T15:04:05.000000000Z"
)

func ValidLeaseClass(value string) bool {
	return value == "go-test" || value == "jvm-build"
}

func mappedLeaseClass(tag string) *string {
	switch tag {
	case "mac.go-test-manual":
		return stringPtr("go-test")
	case "mac.jvm-build":
		return stringPtr("jvm-build")
	default:
		return nil
	}
}

type Config struct {
	StateDir   string
	SocketPath string
	Clock      Clock
}

type Response struct {
	Version   int            `json:"version"`
	Type      string         `json:"type"`
	Op        string         `json:"op"`
	RequestID string         `json:"request_id,omitempty"`
	State     string         `json:"state"`
	Data      map[string]any `json:"data"`
	Code      string         `json:"code,omitempty"`
}

type RequestRecord struct {
	RequestID          string  `json:"request_id"`
	RequesterID        *string `json:"requester_id"`
	LegacyUnattributed bool    `json:"legacy_unattributed"`
	Tag                string  `json:"tag"`
	Class              *string `json:"-"`
	DurationNS         int64   `json:"duration_ns"`
	WaitMode           string  `json:"wait_mode"`
	WaitTimeout        *int64  `json:"wait_timeout_ns"`
	QueueSequence      *int64  `json:"queue_sequence"`
	ClassQueueSequence *int64  `json:"-"`
	QueuedAt           *string `json:"queued_at"`
	WaitDeadline       *string `json:"wait_deadline_at"`
	State              string  `json:"state"`
	OutcomeCode        *string `json:"outcome_code"`
}

type ClassActiveLease struct {
	HolderID  *string `json:"holder_id"`
	State     string  `json:"state"`
	GrantedAt string  `json:"granted_at"`
	HardEndAt string  `json:"hard_end_at"`
}

type ClassQueuedRequest struct {
	RequesterID    *string `json:"requester_id"`
	State          string  `json:"state"`
	QueuedAt       string  `json:"queued_at"`
	WaitDeadlineAt string  `json:"wait_deadline_at"`
}

type ClassQueueStatus struct {
	Class          string               `json:"class"`
	ActiveCount    int64                `json:"active_count"`
	QueuedCount    int64                `json:"queued_count"`
	OldestQueuedAt *string              `json:"oldest_queued_at"`
	ActiveLeases   []ClassActiveLease   `json:"active_leases"`
	QueuedRequests []ClassQueuedRequest `json:"queued_requests"`
}

type LeaseRecord struct {
	ID                 string  `json:"id"`
	RequestID          string  `json:"request_id"`
	HolderID           *string `json:"holder_id"`
	LegacyUnattributed bool    `json:"legacy_unattributed"`
	Tag                string  `json:"tag"`
	ResourceID         string  `json:"resource_id"`
	State              string  `json:"state"`
	DurationNS         int64   `json:"duration_ns"`
	GrantedAt          string  `json:"granted_at"`
	HardEndAt          string  `json:"hard_end_at"`
	FencingToken       int64   `json:"fencing_token"`
	EndedAt            *string `json:"ended_at"`
	EndedResourceState *string `json:"ended_resource_state"`
	EndedFencingToken  *int64  `json:"ended_fencing_token"`
}

type ResourceRecord struct {
	ID           string `json:"id"`
	Tag          string `json:"tag"`
	Mode         string `json:"mode"`
	State        string `json:"state"`
	FencingToken int64  `json:"fencing_token"`
}

type PoolRecord struct {
	Tag  string `json:"tag"`
	Mode string `json:"mode"`
}

type PoolStatus struct {
	Tag       string           `json:"tag"`
	Mode      string           `json:"mode"`
	Resources []ResourceRecord `json:"resources"`
}

type Counts struct {
	QueuedRequests   int64 `json:"queued_requests"`
	ActiveLeases     int64 `json:"active_leases"`
	TerminalRequests int64 `json:"terminal_requests"`
	AuditRows        int64 `json:"audit_rows"`
}

type AuditEntry struct {
	Sequence           int64   `json:"sequence"`
	At                 string  `json:"at"`
	Operation          string  `json:"operation"`
	ActorKind          string  `json:"actor_kind"`
	ActorComponent     *string `json:"actor_component"`
	Cause              *string `json:"cause"`
	CauseRefKind       *string `json:"cause_ref_kind"`
	CauseRefID         *string `json:"cause_ref_id"`
	Origin             *string `json:"origin"`
	RequestID          *string `json:"request_id"`
	RequesterID        *string `json:"requester_id"`
	HolderID           *string `json:"holder_id"`
	LegacyUnattributed bool    `json:"legacy_unattributed"`
	TargetID           *string `json:"target_id"`
	ResultCode         string  `json:"result_code"`
	FromState          *string `json:"from_state"`
	ToState            *string `json:"to_state"`
}

type socketRequest struct {
	Version   json.RawMessage `json:"version"`
	Type      json.RawMessage `json:"type"`
	Op        json.RawMessage `json:"op"`
	RequestID json.RawMessage `json:"request_id"`
	Data      json.RawMessage `json:"data"`
}

type fault struct {
	code      string
	requestID string
	data      map[string]any
}

func response(op, requestID, state, code string, data map[string]any) Response {
	if data == nil {
		data = emptyData()
	}
	return Response{Version: 1, Type: "response", Op: op, RequestID: requestID, State: state, Code: code, Data: data}
}

func emptyData() map[string]any { return map[string]any{} }

func (f *fault) Error() string { return f.code }
