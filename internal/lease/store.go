package lease

import (
	"crypto/rand"
	"encoding/hex"
	"errors"
	"math"
	"time"

	"github.com/relux-works/swarma-lease/internal/sqlite"
)

type AcquireInput struct {
	RequestID     string
	Tag           string
	DurationNS    int64
	WaitMode      string
	WaitTimeoutNS *int64
}

func (s *Store) withClockTransaction(fn func(*sqlite.Tx, Sample) error) error {
	if s.beforeClockTransactionForTest != nil {
		s.beforeClockTransactionForTest()
	}
	return s.db.Transaction(func(tx *sqlite.Tx) error {
		sample, err := readClockSample(s.clock)
		if err != nil {
			return err
		}
		return fn(tx, sample)
	})
}

func (s *Store) recoverBoot() error {
	var events []TerminalEvent
	err := s.withClockTransaction(func(tx *sqlite.Tx, sample Sample) error {
		rows, err := tx.Query("SELECT boot_session_id FROM settings WHERE singleton_id=1")
		if err != nil || len(rows) != 1 {
			return sqlite.ErrStorage
		}
		oldBoot := rowString(rows[0], "boot_session_id")
		if err := s.applyDue(tx, sample, "startup.recovery", &events); err != nil {
			return err
		}
		if oldBoot != sample.BootSessionID {
			if _, err := tx.Exec("UPDATE settings SET boot_session_id=? WHERE singleton_id=1", sample.BootSessionID); err != nil {
				return err
			}
		}
		if err := disableAllExhaustedResources(tx, sample); err != nil {
			return err
		}
		return s.scheduleQueued(tx, sample, &events)
	})
	if err == nil {
		s.publishEvents(events)
	}
	return err
}

func (s *Store) Acquire(in AcquireInput) (string, map[string]any, string) {
	if in.DurationNS <= 0 {
		return "error", emptyData(), "invalid_duration"
	}
	if in.WaitMode != "wait" && in.WaitMode != "no_wait" {
		return "error", emptyData(), "invalid_wait_mode"
	}
	if in.WaitMode == "wait" && (in.WaitTimeoutNS == nil || *in.WaitTimeoutNS <= 0) || in.WaitMode == "no_wait" && in.WaitTimeoutNS != nil {
		return "error", emptyData(), "invalid_wait_timeout"
	}
	var state, code string
	var data map[string]any
	var events []TerminalEvent
	err := s.withClockTransaction(func(tx *sqlite.Tx, sample Sample) error {
		if err := s.scheduleTx(tx, sample, "scheduler", &events); err != nil {
			return err
		}
		old, err := requestByID(tx, in.RequestID)
		if err != nil {
			return err
		}
		if old != nil {
			data, err = requestResult(tx, in.RequestID)
			if err != nil {
				return err
			}
			if old.Tag != in.Tag || old.DurationNS != in.DurationNS || old.WaitMode != in.WaitMode || !equalIntPointer(old.WaitTimeout, in.WaitTimeoutNS) {
				state, code = "error", "request_id_conflict"
				return nil
			}
			switch old.State {
			case "leased":
				state = "granted"
			case "refused":
				state, code = "error", stringValue(old.OutcomeCode)
			case "released", "expired":
				state, code = "error", stringValue(old.OutcomeCode)
			case "queued":
				state = "queued"
			default:
				state, code = "error", stringValue(old.OutcomeCode)
			}
			return nil
		}
		if in.DurationNS > maxLeaseNS {
			return s.refuse(tx, in, sample, "duration_exceeds_max", &state, &code, &data)
		}
		if in.WaitMode == "wait" && *in.WaitTimeoutNS > maxWaitNS {
			return s.refuse(tx, in, sample, "wait_exceeds_max", &state, &code, &data)
		}
		pools, err := tx.Query("SELECT mode FROM pools WHERE tag=?", in.Tag)
		if err != nil {
			return err
		}
		if len(pools) == 0 {
			return s.refuse(tx, in, sample, "unknown_tag", &state, &code, &data)
		}
		if rowString(pools[0], "mode") == "command" {
			return s.refuse(tx, in, sample, "adapter_required", &state, &code, &data)
		}
		resources, err := tx.Query("SELECT id FROM resources WHERE tag=? AND state='available' ORDER BY id COLLATE BINARY LIMIT 1", in.Tag)
		if err != nil {
			return err
		}
		if len(resources) == 0 {
			totalRows, err := tx.Query("SELECT COUNT(*) AS total,SUM(CASE WHEN state='disabled' THEN 1 ELSE 0 END) AS disabled FROM resources WHERE tag=?", in.Tag)
			if err != nil || len(totalRows) != 1 {
				return sqlite.ErrStorage
			}
			if rowInt(totalRows[0], "total") > 0 && rowInt(totalRows[0], "total") == rowInt(totalRows[0], "disabled") {
				return s.refuse(tx, in, sample, "pool_disabled", &state, &code, &data)
			}
			if in.WaitMode == "no_wait" {
				return s.refuse(tx, in, sample, "no_wait_busy", &state, &code, &data)
			}
			if err := s.enqueueRequest(tx, in, sample, &state, &code, &data); err != nil {
				return err
			}
			return s.scheduleQueued(tx, sample, &events)
		}
		resource, err := loadResource(tx, rowString(resources[0], "id"))
		if err != nil {
			return err
		}
		request := RequestRecord{RequestID: in.RequestID, Tag: in.Tag, DurationNS: in.DurationNS, WaitMode: in.WaitMode, WaitTimeout: in.WaitTimeoutNS}
		if err := grantRequest(tx, sample, request, resource, "lease.acquire", nil); err != nil {
			return err
		}
		data, err = requestResult(tx, in.RequestID)
		if err != nil {
			return err
		}
		state = "granted"
		return nil
	})
	if err != nil {
		return "error", emptyData(), storageCode(err)
	}
	s.publishEvents(events)
	s.signalChanged()
	return state, data, code
}

func (s *Store) refuse(tx *sqlite.Tx, in AcquireInput, sample Sample, refusal string, state, code *string, data *map[string]any) error {
	at := timestamp(sample.Wall)
	var waitTimeout any
	if in.WaitTimeoutNS != nil {
		waitTimeout = *in.WaitTimeoutNS
	}
	if _, err := tx.Exec("INSERT INTO requests(request_id,tag,duration_ns,wait_mode,wait_timeout_ns,queue_sequence,queued_at,wait_deadline_at,queue_boot_session_id,queue_started_monotonic_ns,wait_deadline_monotonic_ns,state,outcome_code,created_at,updated_at) VALUES(?,?,?,?,?,NULL,NULL,NULL,NULL,NULL,NULL,'refused',?,?,?)", in.RequestID, in.Tag, in.DurationNS, in.WaitMode, waitTimeout, refusal, at, at); err != nil {
		return err
	}
	if err := insertAudit(tx, sample, "lease.acquire", &in.RequestID, &in.RequestID, refusal, nil, stringPtr("refused")); err != nil {
		return err
	}
	var err error
	*data, err = requestResult(tx, in.RequestID)
	if err != nil {
		return err
	}
	*state, *code = "error", refusal
	return nil
}

func (s *Store) AddResource(tag, id, mode string) (string, map[string]any, string) {
	var state, code string
	var data map[string]any
	var events []TerminalEvent
	err := s.withClockTransaction(func(tx *sqlite.Tx, sample Sample) error {
		if err := s.scheduleTx(tx, sample, "scheduler", &events); err != nil {
			return err
		}
		resources, err := tx.Query("SELECT id FROM resources WHERE id=?", id)
		if err != nil {
			return err
		}
		if len(resources) != 0 {
			state, code = "error", "duplicate_resource_id"
			return nil
		}
		pools, err := tx.Query("SELECT mode FROM pools WHERE tag=?", tag)
		if err != nil {
			return err
		}
		if len(pools) != 0 && rowString(pools[0], "mode") != mode {
			state, code = "error", "pool_mode_mismatch"
			return nil
		}
		at := timestamp(sample.Wall)
		if len(pools) == 0 {
			if _, err := tx.Exec("INSERT INTO pools(tag,mode,next_queue_sequence,created_at) VALUES(?,?,1,?)", tag, mode, at); err != nil {
				return err
			}
		}
		if _, err := tx.Exec("INSERT INTO resources(id,tag,state,fencing_token,created_at,updated_at) VALUES(?,?,'available',0,?,?)", id, tag, at, at); err != nil {
			return err
		}
		if err := insertAudit(tx, sample, "resource.add", nil, &id, "ok", nil, stringPtr("available")); err != nil {
			return err
		}
		resource, err := loadResource(tx, id)
		if err != nil {
			return err
		}
		state = "added"
		if err := s.scheduleQueued(tx, sample, &events); err != nil {
			return err
		}
		resource, err = loadResource(tx, id)
		if err != nil {
			return err
		}
		data = map[string]any{"pool": PoolRecord{Tag: tag, Mode: mode}, "resource": resource}
		return nil
	})
	if err != nil {
		return "error", emptyData(), storageCode(err)
	}
	s.publishEvents(events)
	s.signalChanged()
	return state, data, code
}

func (s *Store) SetResource(operation, id string, confirmStopped bool) (string, map[string]any, string) {
	var state, code string
	var data map[string]any
	var events []TerminalEvent
	err := s.withClockTransaction(func(tx *sqlite.Tx, sample Sample) error {
		if err := s.scheduleTx(tx, sample, "scheduler", &events); err != nil {
			return err
		}
		resource, err := loadResource(tx, id)
		if err != nil {
			if errors.Is(err, errNotFound) {
				state, code = "error", "unknown_resource"
				return nil
			}
			return err
		}
		target := ""
		switch operation {
		case "resource.disable":
			if resource.State == "disabled" {
				state, data = "disabled", map[string]any{"resource": resource}
				return nil
			}
			if resource.State != "available" {
				state, code = "error", "invalid_transition"
				return nil
			}
			target, state = "disabled", "disabled"
		case "resource.enable":
			if resource.State == "available" && resource.FencingToken < math.MaxInt64 {
				state, data = "enabled", map[string]any{"resource": resource}
				return nil
			}
			if resource.State != "disabled" || resource.FencingToken == math.MaxInt64 {
				state, code = "error", "invalid_transition"
				return nil
			}
			target, state = "available", "enabled"
		case "resource.recover":
			if !confirmStopped {
				state, code = "error", "invalid_request"
				return nil
			}
			if resource.Mode != "command" || resource.State != "quarantined" {
				state, code = "error", "invalid_transition"
				return nil
			}
			target, state = "available", "recovered"
			if resource.FencingToken == math.MaxInt64 {
				target = "disabled"
			}
		default:
			state, code = "error", "unknown_operation"
			return nil
		}
		at := timestamp(sample.Wall)
		if _, err := tx.Exec("UPDATE resources SET state=?,updated_at=? WHERE id=?", target, at, id); err != nil {
			return err
		}
		if err := insertAudit(tx, sample, operation, nil, &id, "ok", stringPtr(resource.State), stringPtr(target)); err != nil {
			return err
		}
		if err := s.scheduleQueued(tx, sample, &events); err != nil {
			return err
		}
		resource, err = loadResource(tx, id)
		if err != nil {
			return err
		}
		data = map[string]any{"resource": resource}
		return nil
	})
	if err != nil {
		return "error", emptyData(), storageCode(err)
	}
	s.publishEvents(events)
	s.signalChanged()
	return state, data, code
}

func (s *Store) Release(leaseID string) (string, map[string]any, string) {
	var state, code string
	var data map[string]any
	var events []TerminalEvent
	err := s.withClockTransaction(func(tx *sqlite.Tx, sample Sample) error {
		if err := s.scheduleTx(tx, sample, "scheduler", &events); err != nil {
			return err
		}
		rows, err := tx.Query("SELECT request_id FROM leases WHERE id=?", leaseID)
		if err != nil {
			return err
		}
		if len(rows) == 0 {
			state, code = "error", "unknown_lease"
			return nil
		}
		requestID := rowString(rows[0], "request_id")
		lease, err := leaseByID(tx, leaseID)
		if err != nil {
			return err
		}
		if lease.State == "active" {
			resource, err := loadResource(tx, lease.ResourceID)
			if err != nil {
				return err
			}
			if err := endLease(tx, sample, lease, resource, "released", "lease.release"); err != nil {
				return err
			}
		}
		data, err = requestResult(tx, requestID)
		if err != nil {
			return err
		}
		lease, err = leaseByID(tx, leaseID)
		if err != nil {
			return err
		}
		state = lease.State
		return s.scheduleQueued(tx, sample, &events)
	})
	if err != nil {
		return "error", emptyData(), storageCode(err)
	}
	s.publishEvents(events)
	s.signalChanged()
	return state, data, code
}

func (s *Store) Status(requestID, leaseID string) (string, map[string]any, string) {
	var state, code string
	var result map[string]any
	err := s.db.View(func(tx *sqlite.Tx) error {
		if requestID != "" {
			request, err := requestByID(tx, requestID)
			if err != nil {
				return err
			}
			if request == nil {
				code = "unknown_request"
				return nil
			}
			state = request.State
			result, err = requestResult(tx, requestID)
			return err
		}
		rows, err := tx.Query("SELECT request_id FROM leases WHERE id=?", leaseID)
		if err != nil {
			return err
		}
		if len(rows) == 0 {
			code = "unknown_lease"
			return nil
		}
		result, err = requestResult(tx, rowString(rows[0], "request_id"))
		if err != nil {
			return err
		}
		lease := result["lease"].(LeaseRecord)
		state = lease.State
		return nil
	})
	if err != nil {
		return "error", emptyData(), storageCode(err)
	}
	if code != "" {
		return "error", emptyData(), code
	}
	return state, result, ""
}

func (s *Store) ServiceStatus() (map[string]any, string) {
	var data map[string]any
	err := s.db.View(func(tx *sqlite.Tx) error {
		settings, err := tx.Query("SELECT schema_version,owner_uid,max_lease_ns,max_wait_ns FROM settings WHERE singleton_id=1")
		if err != nil || len(settings) != 1 {
			return sqlite.ErrStorage
		}
		pools, err := tx.Query("SELECT tag,mode FROM pools ORDER BY tag COLLATE BINARY")
		if err != nil {
			return err
		}
		poolStatus := make([]PoolStatus, 0, len(pools))
		for _, pool := range pools {
			resources, err := tx.Query("SELECT r.id,r.tag,p.mode,r.state,r.fencing_token FROM resources r JOIN pools p ON p.tag=r.tag WHERE r.tag=? ORDER BY r.id COLLATE BINARY", rowString(pool, "tag"))
			if err != nil {
				return err
			}
			items := make([]ResourceRecord, 0, len(resources))
			for _, row := range resources {
				items = append(items, resourceFromRow(row))
			}
			poolStatus = append(poolStatus, PoolStatus{Tag: rowString(pool, "tag"), Mode: rowString(pool, "mode"), Resources: items})
		}
		countRows, err := tx.Query("SELECT SUM(CASE WHEN state='queued' THEN 1 ELSE 0 END) AS queued,SUM(CASE WHEN state='leased' THEN 1 ELSE 0 END) AS active, SUM(CASE WHEN state IN ('released','expired','cancelled','timed_out','refused') THEN 1 ELSE 0 END) AS terminal FROM requests")
		if err != nil || len(countRows) != 1 {
			return sqlite.ErrStorage
		}
		auditRows, err := tx.Query("SELECT COUNT(*) AS n FROM audit")
		if err != nil || len(auditRows) != 1 {
			return sqlite.ErrStorage
		}
		counts := Counts{QueuedRequests: rowInt(countRows[0], "queued"), ActiveLeases: rowInt(countRows[0], "active"), TerminalRequests: rowInt(countRows[0], "terminal"), AuditRows: rowInt(auditRows[0], "n")}
		data = map[string]any{
			"schema_version":        rowInt(settings[0], "schema_version"),
			"owner_uid":             rowInt(settings[0], "owner_uid"),
			"max_lease_duration_ns": rowInt(settings[0], "max_lease_ns"),
			"max_wait_timeout_ns":   rowInt(settings[0], "max_wait_ns"),
			"pools":                 poolStatus,
			"counts":                counts,
		}
		return nil
	})
	if err != nil {
		return emptyData(), storageCode(err)
	}
	return data, ""
}

func (s *Store) History(limit int64, before *int64) (map[string]any, string) {
	query := "SELECT sequence,at,operation,request_id,target_id,result_code,from_state,to_state FROM audit ORDER BY sequence DESC LIMIT ?"
	args := []any{limit}
	if before != nil {
		query = "SELECT sequence,at,operation,request_id,target_id,result_code,from_state,to_state FROM audit WHERE sequence<? ORDER BY sequence DESC LIMIT ?"
		args = []any{*before, limit}
	}
	rows, err := s.db.Query(query, args...)
	if err != nil {
		return emptyData(), storageCode(err)
	}
	entries := make([]AuditEntry, 0, len(rows))
	for i := len(rows) - 1; i >= 0; i-- {
		row := rows[i]
		entries = append(entries, AuditEntry{Sequence: rowInt(row, "sequence"), At: rowString(row, "at"), Operation: rowString(row, "operation"), RequestID: nullableString(row, "request_id"), TargetID: nullableString(row, "target_id"), ResultCode: rowString(row, "result_code"), FromState: nullableString(row, "from_state"), ToState: nullableString(row, "to_state")})
	}
	return map[string]any{"entries": entries}, ""
}

func (s *Store) applyDue(tx *sqlite.Tx, sample Sample, operation string, events *[]TerminalEvent) error {
	rows, err := tx.Query("SELECT l.id,l.request_id,l.resource_id,l.state,l.duration_ns,l.granted_at,l.hard_end_at,l.fencing_token,l.boot_session_id,l.granted_monotonic_ns,l.hard_end_monotonic_ns,r.id AS joined_resource_id,r.tag,r.state AS resource_state,r.fencing_token AS resource_fencing_token,p.mode FROM leases l JOIN resources r ON r.id=l.resource_id JOIN pools p ON p.tag=r.tag WHERE l.state='active' ORDER BY l.id COLLATE BINARY")
	if err != nil {
		return err
	}
	for _, row := range rows {
		lease := leaseFromRow(row)
		resource := ResourceRecord{ID: rowString(row, "joined_resource_id"), Tag: rowString(row, "tag"), Mode: rowString(row, "mode"), State: rowString(row, "resource_state"), FencingToken: rowInt(row, "resource_fencing_token")}
		if rowString(row, "boot_session_id") != sample.BootSessionID || sample.MonotonicNS >= rowInt(row, "hard_end_monotonic_ns") {
			if err := endLease(tx, sample, lease, resource, "expired", operation); err != nil {
				return err
			}
		}
	}
	queued, err := tx.Query("SELECT request_id,queue_boot_session_id,wait_deadline_monotonic_ns FROM requests WHERE state='queued' ORDER BY tag,queue_sequence")
	if err != nil {
		return err
	}
	for _, row := range queued {
		bootID := stringValue(nullableString(row, "queue_boot_session_id"))
		deadline := rowInt(row, "wait_deadline_monotonic_ns")
		if bootID != sample.BootSessionID || sample.MonotonicNS >= deadline {
			id := rowString(row, "request_id")
			if _, err := tx.Exec("UPDATE requests SET state='timed_out',outcome_code='request_timed_out',updated_at=? WHERE request_id=? AND state='queued'", timestamp(sample.Wall), id); err != nil {
				return err
			}
			if err := insertAudit(tx, sample, operation, &id, &id, "request_timed_out", stringPtr("queued"), stringPtr("timed_out")); err != nil {
				return err
			}
			event, _, err := terminalEvent(tx, id)
			if err != nil || event == nil {
				return sqlite.ErrStorage
			}
			*events = append(*events, *event)
		}
	}
	return nil
}

func storageCode(err error) string {
	if errors.Is(err, errClockUnavailable) {
		return "clock_unavailable"
	}
	if errors.Is(err, sqlite.ErrOutcomeUnknown) {
		return "result_unknown"
	}
	return "storage_unavailable"
}

var errNotFound = errors.New("not found")

func disableExhaustedResources(tx *sqlite.Tx, tag string, sample Sample) error {
	rows, err := tx.Query("SELECT id,fencing_token FROM resources WHERE tag=? AND state='available' AND fencing_token=? ORDER BY id", tag, int64(math.MaxInt64))
	if err != nil {
		return err
	}
	for _, row := range rows {
		id := rowString(row, "id")
		if _, err := tx.Exec("UPDATE resources SET state='disabled',updated_at=? WHERE id=? AND state='available'", timestamp(sample.Wall), id); err != nil {
			return err
		}
		if err := insertAudit(tx, sample, "scheduler", nil, &id, "ok", stringPtr("available"), stringPtr("disabled")); err != nil {
			return err
		}
	}
	return nil
}

func endLease(tx *sqlite.Tx, sample Sample, lease LeaseRecord, resource ResourceRecord, terminal, operation string) error {
	if resource.State != "leased" || lease.State != "active" {
		return sqlite.ErrStorage
	}
	resourceState := "available"
	if resource.Mode == "command" {
		resourceState = "cleanup_pending"
	} else if resource.FencingToken == math.MaxInt64 {
		resourceState = "disabled"
	}
	at := timestamp(sample.Wall)
	requestState := terminal
	resultCode := "lease_released"
	if terminal == "expired" {
		requestState, resultCode = "expired", "lease_expired"
	}
	if _, err := tx.Exec("UPDATE leases SET state=?,ended_at=?,ended_resource_state=?,ended_fencing_token=? WHERE id=? AND state='active'", terminal, at, resourceState, lease.FencingToken, lease.ID); err != nil {
		return err
	}
	if _, err := tx.Exec("UPDATE requests SET state=?,outcome_code=?,updated_at=? WHERE request_id=? AND state='leased'", requestState, resultCode, at, lease.RequestID); err != nil {
		return err
	}
	if _, err := tx.Exec("UPDATE resources SET state=?,updated_at=? WHERE id=? AND state='leased'", resourceState, at, resource.ID); err != nil {
		return err
	}
	if err := insertAudit(tx, sample, operation, &lease.RequestID, &lease.ID, resultCode, stringPtr("active"), stringPtr(terminal)); err != nil {
		return err
	}
	if err := insertAudit(tx, sample, operation, &lease.RequestID, &lease.RequestID, resultCode, stringPtr("leased"), stringPtr(requestState)); err != nil {
		return err
	}
	return insertAudit(tx, sample, operation, &lease.RequestID, &resource.ID, resultCode, stringPtr("leased"), stringPtr(resourceState))
}

func insertAudit(tx *sqlite.Tx, sample Sample, operation string, requestID, targetID *string, resultCode string, from, to *string) error {
	_, err := tx.Exec("INSERT INTO audit(at,operation,request_id,target_id,result_code,from_state,to_state) VALUES(?,?,?,?,?,?,?)", timestamp(sample.Wall), operation, nullableSQLString(requestID), nullableSQLString(targetID), resultCode, nullableSQLString(from), nullableSQLString(to))
	return err
}

func nullableSQLString(value *string) any {
	if value == nil {
		return nil
	}
	return *value
}

func requestByID(tx *sqlite.Tx, id string) (*RequestRecord, error) {
	rows, err := tx.Query("SELECT request_id,tag,duration_ns,wait_mode,wait_timeout_ns,queue_sequence,queued_at,wait_deadline_at,state,outcome_code FROM requests WHERE request_id=?", id)
	if err != nil {
		return nil, err
	}
	if len(rows) == 0 {
		return nil, nil
	}
	if len(rows) != 1 {
		return nil, sqlite.ErrStorage
	}
	return requestFromRow(rows[0]), nil
}

func requestResult(tx *sqlite.Tx, id string) (map[string]any, error) {
	request, err := requestByID(tx, id)
	if err != nil || request == nil {
		return nil, sqlite.ErrStorage
	}
	leases, err := tx.Query("SELECT id FROM leases WHERE request_id=?", id)
	if err != nil {
		return nil, err
	}
	if len(leases) == 0 {
		return map[string]any{"request": request, "lease": nil, "resource": nil}, nil
	}
	lease, err := leaseByID(tx, rowString(leases[0], "id"))
	if err != nil {
		return nil, err
	}
	resource, err := leaseResource(tx, lease)
	if err != nil {
		return nil, err
	}
	return map[string]any{"request": request, "lease": lease, "resource": resource}, nil
}

func leaseByID(tx *sqlite.Tx, id string) (LeaseRecord, error) {
	rows, err := tx.Query("SELECT l.id,l.request_id,r.tag,l.resource_id,l.state,l.duration_ns,l.granted_at,l.hard_end_at,l.fencing_token,l.ended_at,l.ended_resource_state,l.ended_fencing_token,l.boot_session_id,l.granted_monotonic_ns,l.hard_end_monotonic_ns FROM leases l JOIN resources r ON r.id=l.resource_id WHERE l.id=?", id)
	if err != nil || len(rows) != 1 {
		return LeaseRecord{}, sqlite.ErrStorage
	}
	return leaseFromRow(rows[0]), nil
}

func leaseResource(tx *sqlite.Tx, lease LeaseRecord) (ResourceRecord, error) {
	if lease.State != "active" {
		rows, err := tx.Query("SELECT r.id,r.tag,p.mode FROM resources r JOIN pools p ON p.tag=r.tag WHERE r.id=?", lease.ResourceID)
		if err != nil || len(rows) != 1 {
			return ResourceRecord{}, sqlite.ErrStorage
		}
		return ResourceRecord{ID: rowString(rows[0], "id"), Tag: rowString(rows[0], "tag"), Mode: rowString(rows[0], "mode"), State: stringValue(lease.EndedResourceState), FencingToken: intValue(lease.EndedFencingToken)}, nil
	}
	return loadResource(tx, lease.ResourceID)
}

func loadResource(tx *sqlite.Tx, id string) (ResourceRecord, error) {
	rows, err := tx.Query("SELECT r.id,r.tag,p.mode,r.state,r.fencing_token FROM resources r JOIN pools p ON p.tag=r.tag WHERE r.id=?", id)
	if err != nil {
		return ResourceRecord{}, err
	}
	if len(rows) == 0 {
		return ResourceRecord{}, errNotFound
	}
	if len(rows) != 1 {
		return ResourceRecord{}, sqlite.ErrStorage
	}
	return resourceFromRow(rows[0]), nil
}

func resourceFromRow(row sqlite.Row) ResourceRecord {
	return ResourceRecord{ID: rowString(row, "id"), Tag: rowString(row, "tag"), Mode: rowString(row, "mode"), State: rowString(row, "state"), FencingToken: rowInt(row, "fencing_token")}
}

func requestFromRow(row sqlite.Row) *RequestRecord {
	return &RequestRecord{RequestID: rowString(row, "request_id"), Tag: rowString(row, "tag"), DurationNS: rowInt(row, "duration_ns"), WaitMode: rowString(row, "wait_mode"), WaitTimeout: nullableInt(row, "wait_timeout_ns"), QueueSequence: nullableInt(row, "queue_sequence"), QueuedAt: nullableString(row, "queued_at"), WaitDeadline: nullableString(row, "wait_deadline_at"), State: rowString(row, "state"), OutcomeCode: nullableString(row, "outcome_code")}
}

func leaseFromRow(row sqlite.Row) LeaseRecord {
	return LeaseRecord{ID: rowString(row, "id"), RequestID: rowString(row, "request_id"), Tag: rowString(row, "tag"), ResourceID: rowString(row, "resource_id"), State: rowString(row, "state"), DurationNS: rowInt(row, "duration_ns"), GrantedAt: rowString(row, "granted_at"), HardEndAt: rowString(row, "hard_end_at"), FencingToken: rowInt(row, "fencing_token"), EndedAt: nullableString(row, "ended_at"), EndedResourceState: nullableString(row, "ended_resource_state"), EndedFencingToken: nullableInt(row, "ended_fencing_token")}
}

func equalIntPointer(a, b *int64) bool {
	if a == nil || b == nil {
		return a == nil && b == nil
	}
	return *a == *b
}

func newID() (string, error) {
	var raw [16]byte
	if _, err := rand.Read(raw[:]); err != nil {
		return "", err
	}
	return hex.EncodeToString(raw[:]), nil
}

func timestamp(t time.Time) string { return t.UTC().Format(dateLayout) }

func stringPtr(value string) *string { return &value }

func stringValue(value *string) string {
	if value == nil {
		return ""
	}
	return *value
}

func intValue(value *int64) int64 {
	if value == nil {
		return 0
	}
	return *value
}
