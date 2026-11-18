package lease

import (
	"math"
	"time"

	"github.com/relux-works/swarma-lease/internal/sqlite"
)

type leaseClassPolicy struct {
	priority   int64
	maxActive  int64
	maxLeaseNS int64
}

func loadLeaseClassPolicy(tx *sqlite.Tx, class string) (leaseClassPolicy, error) {
	rows, err := tx.Query("SELECT priority,max_active,max_lease_ns FROM lease_classes WHERE class=?", class)
	if err != nil || len(rows) != 1 {
		return leaseClassPolicy{}, sqlite.ErrStorage
	}
	policy := leaseClassPolicy{priority: rowInt(rows[0], "priority"), maxActive: rowInt(rows[0], "max_active"), maxLeaseNS: rowInt(rows[0], "max_lease_ns")}
	if policy.priority < 0 || policy.maxActive < 1 || policy.maxLeaseNS <= 0 {
		return leaseClassPolicy{}, sqlite.ErrStorage
	}
	return policy, nil
}

func classCanGrant(tx *sqlite.Tx, class string) (bool, error) {
	policy, err := loadLeaseClassPolicy(tx, class)
	if err != nil {
		return false, err
	}
	active, err := activeClassLeaseCount(tx, class)
	if err != nil {
		return false, err
	}
	if active >= policy.maxActive {
		return false, nil
	}
	queued, err := hasClassQueue(tx, class)
	if err != nil {
		return false, err
	}
	if queued {
		return false, nil
	}
	blocked, err := hasHigherPriorityQueue(tx, policy.priority)
	if err != nil {
		return false, err
	}
	return !blocked, nil
}

func hasClassQueue(tx *sqlite.Tx, class string) (bool, error) {
	rows, err := tx.Query("SELECT request_id FROM requests WHERE class=? AND state='queued' LIMIT 1", class)
	if err != nil {
		return false, err
	}
	return len(rows) > 0, nil
}

func (s *Store) scheduleTx(tx *sqlite.Tx, sample Sample, operation string, events *[]TerminalEvent) error {
	if err := s.applyDue(tx, sample, operation, events); err != nil {
		return err
	}
	if err := disableAllExhaustedResources(tx, sample); err != nil {
		return err
	}
	return s.scheduleQueued(tx, sample, events)
}

func (s *Store) scheduleQueued(tx *sqlite.Tx, sample Sample, events *[]TerminalEvent) error {
	tags, err := tx.Query("SELECT DISTINCT r.tag FROM requests r JOIN pools p ON p.tag=r.tag WHERE r.state='queued' AND p.class IS NULL ORDER BY r.tag COLLATE BINARY")
	if err != nil {
		return err
	}
	for _, tagRow := range tags {
		tag := rowString(tagRow, "tag")
		for {
			requests, err := tx.Query("SELECT request_id FROM requests WHERE tag=? AND state='queued' ORDER BY queue_sequence LIMIT 1", tag)
			if err != nil {
				return err
			}
			if len(requests) == 0 {
				break
			}
			resources, err := tx.Query("SELECT id FROM resources WHERE tag=? AND state='available' ORDER BY id COLLATE BINARY LIMIT 1", tag)
			if err != nil {
				return err
			}
			if len(resources) == 0 {
				break
			}
			requestID := rowString(requests[0], "request_id")
			request, err := requestByID(tx, requestID)
			if err != nil || request == nil || request.Class != nil {
				return sqlite.ErrStorage
			}
			resource, err := loadResource(tx, rowString(resources[0], "id"))
			if err != nil {
				return err
			}
			if err := grantRequest(tx, sample, *request, resource, "scheduler", stringPtr("queued")); err != nil {
				return err
			}
			event, _, err := terminalEvent(tx, requestID)
			if err != nil || event == nil {
				return sqlite.ErrStorage
			}
			*events = append(*events, *event)
		}
	}
	classes, err := tx.Query("SELECT class,priority,max_active FROM lease_classes ORDER BY priority DESC,class COLLATE BINARY")
	if err != nil {
		return err
	}
	for _, classRow := range classes {
		class := rowString(classRow, "class")
		priority := rowInt(classRow, "priority")
		maxActive := rowInt(classRow, "max_active")
		if !ValidLeaseClass(class) || priority < 0 || maxActive < 1 {
			return sqlite.ErrStorage
		}
		for {
			blocked, err := hasHigherPriorityQueue(tx, priority)
			if err != nil {
				return err
			}
			if blocked {
				break
			}
			active, err := activeClassLeaseCount(tx, class)
			if err != nil {
				return err
			}
			if active >= maxActive {
				break
			}
			requests, err := tx.Query("SELECT request_id,tag,class_queue_sequence FROM requests WHERE class=? AND state='queued' ORDER BY class_queue_sequence,request_id COLLATE BINARY LIMIT 1", class)
			if err != nil {
				return err
			}
			if len(requests) == 0 {
				break
			}
			classSequence := nullableInt(requests[0], "class_queue_sequence")
			if classSequence == nil || *classSequence < 1 {
				return sqlite.ErrStorage
			}
			requestID := rowString(requests[0], "request_id")
			request, err := requestByID(tx, requestID)
			if err != nil || request == nil || request.Class == nil || *request.Class != class || request.Tag != rowString(requests[0], "tag") || request.ClassQueueSequence == nil || *request.ClassQueueSequence != *classSequence {
				return sqlite.ErrStorage
			}
			resources, err := tx.Query("SELECT r.id FROM resources r JOIN pools p ON p.tag=r.tag WHERE p.class=? AND r.tag=? AND r.state='available' ORDER BY r.id COLLATE BINARY LIMIT 1", class, request.Tag)
			if err != nil {
				return err
			}
			if len(resources) == 0 {
				break
			}
			resource, err := loadResource(tx, rowString(resources[0], "id"))
			if err != nil {
				return err
			}
			if err := grantRequest(tx, sample, *request, resource, "scheduler", stringPtr("queued")); err != nil {
				return err
			}
			event, _, err := terminalEvent(tx, requestID)
			if err != nil || event == nil {
				return sqlite.ErrStorage
			}
			*events = append(*events, *event)
		}
	}
	return nil
}

func activeClassLeaseCount(tx *sqlite.Tx, class string) (int64, error) {
	rows, err := tx.Query("SELECT COUNT(*) AS n FROM leases l JOIN requests r ON r.request_id=l.request_id WHERE r.class=? AND l.state='active'", class)
	if err != nil || len(rows) != 1 {
		return 0, sqlite.ErrStorage
	}
	return rowInt(rows[0], "n"), nil
}

func hasHigherPriorityQueue(tx *sqlite.Tx, priority int64) (bool, error) {
	rows, err := tx.Query("SELECT r.request_id FROM requests r JOIN lease_classes c ON c.class=r.class WHERE r.state='queued' AND c.priority>? LIMIT 1", priority)
	if err != nil {
		return false, err
	}
	return len(rows) > 0, nil
}

func disableAllExhaustedResources(tx *sqlite.Tx, sample Sample) error {
	tags, err := tx.Query("SELECT DISTINCT tag FROM resources WHERE state='available' AND fencing_token=? ORDER BY tag COLLATE BINARY", int64(math.MaxInt64))
	if err != nil {
		return err
	}
	for _, row := range tags {
		if err := disableExhaustedResources(tx, rowString(row, "tag"), sample); err != nil {
			return err
		}
	}
	return nil
}

func (s *Store) RunScheduler() (bool, string) {
	var events []TerminalEvent
	var changed bool
	err := s.withClockTransaction(func(tx *sqlite.Tx, sample Sample) error {
		before, err := auditSequence(tx)
		if err != nil {
			return err
		}
		if err := s.scheduleTx(tx, sample, "scheduler", &events); err != nil {
			return err
		}
		after, err := auditSequence(tx)
		if err != nil {
			return err
		}
		changed = after != before
		return nil
	})
	if err != nil {
		return false, storageCode(err)
	}
	s.publishEvents(events)
	if changed {
		s.signalChanged()
	}
	return changed, ""
}

func (s *Store) Wait(requestID string) (string, map[string]any, string) {
	var state, code string
	var data map[string]any
	var events []TerminalEvent
	err := s.withClockTransaction(func(tx *sqlite.Tx, sample Sample) error {
		if err := s.scheduleTx(tx, sample, "scheduler", &events); err != nil {
			return err
		}
		request, err := requestByID(tx, requestID)
		if err != nil {
			return err
		}
		if request == nil {
			state, code = "error", "unknown_request"
			data = emptyData()
			return nil
		}
		data, err = requestResult(tx, requestID)
		if err != nil {
			return err
		}
		switch request.State {
		case "queued":
			state = "queued"
		case "leased":
			state = "granted"
		case "refused", "released", "expired", "cancelled", "timed_out":
			state, code = "error", stringValue(request.OutcomeCode)
		default:
			return sqlite.ErrStorage
		}
		return nil
	})
	if err != nil {
		return "error", emptyData(), storageCode(err)
	}
	s.publishEvents(events)
	s.signalChanged()
	return state, data, code
}

func (s *Store) Cancel(requestID string) (string, map[string]any, string) {
	var state, code string
	var data map[string]any
	var events []TerminalEvent
	err := s.withClockTransaction(func(tx *sqlite.Tx, sample Sample) error {
		if err := s.scheduleTx(tx, sample, "scheduler", &events); err != nil {
			return err
		}
		request, err := requestByID(tx, requestID)
		if err != nil {
			return err
		}
		if request == nil {
			state, code, data = "error", "unknown_request", emptyData()
			return nil
		}
		if request.State != "queued" {
			data, err = requestResult(tx, requestID)
			if err != nil {
				return err
			}
			switch request.State {
			case "cancelled":
				state, code = "error", "request_cancelled"
			case "timed_out", "refused":
				state, code = "error", stringValue(request.OutcomeCode)
			case "leased", "released", "expired":
				state, code = "error", "request_not_cancellable"
			default:
				return sqlite.ErrStorage
			}
			return nil
		}
		at := timestamp(sample.Wall)
		if _, err := tx.Exec("UPDATE requests SET state='cancelled',outcome_code='request_cancelled',updated_at=? WHERE request_id=? AND state='queued'", at, requestID); err != nil {
			return err
		}
		if err := insertAudit(tx, sample, "lease.cancel", &requestID, &requestID, "request_cancelled", stringPtr("queued"), stringPtr("cancelled")); err != nil {
			return err
		}
		event, _, err := terminalEvent(tx, requestID)
		if err != nil || event == nil {
			return sqlite.ErrStorage
		}
		events = append(events, *event)
		data, err = requestResult(tx, requestID)
		if err != nil {
			return err
		}
		state = "cancelled"
		return s.scheduleQueued(tx, sample, &events)
	})
	if err != nil {
		return "error", emptyData(), storageCode(err)
	}
	s.publishEvents(events)
	s.signalChanged()
	return state, data, code
}

func (s *Store) enqueueRequest(tx *sqlite.Tx, in AcquireInput, sample Sample, state, code *string, data *map[string]any) error {
	pool, err := tx.Query("SELECT class,next_queue_sequence FROM pools WHERE tag=?", in.Tag)
	if err != nil || len(pool) != 1 {
		return sqlite.ErrStorage
	}
	sequence := rowInt(pool[0], "next_queue_sequence")
	if sequence <= 0 {
		return sqlite.ErrStorage
	}
	class := nullableString(pool[0], "class")
	var classSequence any
	var nextClassSequence int64
	if class != nil {
		classRows, err := tx.Query("SELECT next_queue_sequence FROM lease_classes WHERE class=?", *class)
		if err != nil || len(classRows) != 1 {
			return sqlite.ErrStorage
		}
		nextClassSequence = rowInt(classRows[0], "next_queue_sequence")
		if nextClassSequence <= 0 {
			return sqlite.ErrStorage
		}
		if nextClassSequence != math.MaxInt64 {
			classSequence = nextClassSequence
		}
	}
	if sequence == math.MaxInt64 || class != nil && nextClassSequence == math.MaxInt64 {
		return s.refuse(tx, in, sample, "queue_sequence_exhausted", state, code, data)
	}
	if in.WaitTimeoutNS == nil || *in.WaitTimeoutNS <= 0 || sample.MonotonicNS < 0 || *in.WaitTimeoutNS > math.MaxInt64-sample.MonotonicNS {
		return sqlite.ErrStorage
	}
	deadline := sample.MonotonicNS + *in.WaitTimeoutNS
	queuedAt := timestamp(sample.Wall)
	waitDeadlineAt := timestamp(sample.Wall.Add(time.Duration(*in.WaitTimeoutNS)))
	if _, err := tx.Exec("UPDATE pools SET next_queue_sequence=? WHERE tag=? AND next_queue_sequence=?", sequence+1, in.Tag, sequence); err != nil {
		return err
	}
	if class != nil {
		if _, err := tx.Exec("UPDATE lease_classes SET next_queue_sequence=? WHERE class=? AND next_queue_sequence=?", nextClassSequence+1, *class, nextClassSequence); err != nil {
			return err
		}
	}
	if _, err := tx.Exec(`INSERT INTO requests(request_id,requester_id,legacy_unattributed,tag,class,duration_ns,wait_mode,wait_timeout_ns,queue_sequence,class_queue_sequence,queued_at,wait_deadline_at,queue_boot_session_id,queue_started_monotonic_ns,wait_deadline_monotonic_ns,state,outcome_code,created_at,updated_at)
VALUES(?, ?, 0, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'queued', NULL, ?, ?)`, in.RequestID, in.RequesterID, in.Tag, nullableSQLString(class), in.DurationNS, in.WaitMode, *in.WaitTimeoutNS, sequence, classSequence, queuedAt, waitDeadlineAt, sample.BootSessionID, sample.MonotonicNS, deadline, queuedAt, queuedAt); err != nil {
		return err
	}
	if err := insertAudit(tx, sample, "lease.acquire", &in.RequestID, &in.RequestID, "ok", nil, stringPtr("queued")); err != nil {
		return err
	}
	var resultErr error
	*data, resultErr = requestResult(tx, in.RequestID)
	if resultErr != nil {
		return resultErr
	}
	*state, *code = "queued", ""
	return nil
}

func grantRequest(tx *sqlite.Tx, sample Sample, request RequestRecord, resource ResourceRecord, operation string, requestFrom *string) error {
	if (operation == "scheduler" && requestFrom == nil) || (operation == "lease.acquire" && requestFrom != nil) || (operation != "scheduler" && operation != "lease.acquire") {
		return sqlite.ErrStorage
	}
	if resource.State != "available" || resource.FencingToken == math.MaxInt64 || sample.MonotonicNS < 0 || request.DurationNS > math.MaxInt64-sample.MonotonicNS {
		return sqlite.ErrStorage
	}
	leaseID, err := newID()
	if err != nil {
		return sqlite.ErrStorage
	}
	grantedAt := timestamp(sample.Wall)
	hardEndAt := timestamp(sample.Wall.Add(time.Duration(request.DurationNS)))
	hardEndMono := sample.MonotonicNS + request.DurationNS
	fencingToken := resource.FencingToken + 1
	if requestFrom == nil {
		var waitTimeout any
		if request.WaitTimeout != nil {
			waitTimeout = *request.WaitTimeout
		}
		if _, err := tx.Exec("INSERT INTO requests(request_id,requester_id,legacy_unattributed,tag,class,duration_ns,wait_mode,wait_timeout_ns,queue_sequence,class_queue_sequence,queued_at,wait_deadline_at,queue_boot_session_id,queue_started_monotonic_ns,wait_deadline_monotonic_ns,state,outcome_code,created_at,updated_at) VALUES(?,?,?, ?,?,?,?, ?,NULL,NULL,NULL,NULL,NULL,NULL,NULL,'leased',NULL,?,?)", request.RequestID, nullableSQLString(request.RequesterID), boolInt(request.LegacyUnattributed), request.Tag, nullableSQLString(request.Class), request.DurationNS, request.WaitMode, waitTimeout, grantedAt, grantedAt); err != nil {
			return err
		}
	} else {
		if _, err := tx.Exec("UPDATE requests SET state='leased',outcome_code=NULL,updated_at=? WHERE request_id=? AND state='queued'", grantedAt, request.RequestID); err != nil {
			return err
		}
	}
	if _, err := tx.Exec("UPDATE resources SET state='leased',fencing_token=?,updated_at=? WHERE id=? AND state='available'", fencingToken, grantedAt, resource.ID); err != nil {
		return err
	}
	if _, err := tx.Exec("INSERT INTO leases(id,request_id,holder_id,legacy_unattributed,resource_id,state,duration_ns,granted_at,hard_end_at,boot_session_id,granted_monotonic_ns,hard_end_monotonic_ns,fencing_token,ended_at,ended_resource_state,ended_fencing_token) VALUES(?,?,?,?,?,'active',?,?,?,?,?,?,?,NULL,NULL,NULL)", leaseID, request.RequestID, nullableSQLString(request.RequesterID), boolInt(request.LegacyUnattributed), resource.ID, request.DurationNS, grantedAt, hardEndAt, sample.BootSessionID, sample.MonotonicNS, hardEndMono, fencingToken); err != nil {
		return err
	}
	requestID := request.RequestID
	if operation == "scheduler" {
		if err := insertAuditSystem(tx, sample, operation, "promotion", stringPtr("request"), &requestID, &requestID, &requestID, "ok", requestFrom, stringPtr("leased")); err != nil {
			return err
		}
	} else if err := insertAudit(tx, sample, operation, &requestID, &requestID, "ok", nil, stringPtr("leased")); err != nil {
		return err
	}
	grantCause := "grant"
	if operation == "scheduler" {
		grantCause = "promotion"
	}
	if err := insertAuditSystem(tx, sample, "scheduler", grantCause, stringPtr("request"), &requestID, &requestID, &leaseID, "ok", nil, stringPtr("active")); err != nil {
		return err
	}
	if err := insertAuditSystem(tx, sample, "scheduler", grantCause, stringPtr("request"), &requestID, &requestID, &resource.ID, "ok", stringPtr("available"), stringPtr("leased")); err != nil {
		return err
	}
	return nil
}

func auditSequence(tx *sqlite.Tx) (int64, error) {
	rows, err := tx.Query("SELECT COALESCE(MAX(sequence),0) AS sequence FROM audit")
	if err != nil || len(rows) != 1 {
		return 0, sqlite.ErrStorage
	}
	return rowInt(rows[0], "sequence"), nil
}

func boolInt(value bool) int64 {
	if value {
		return 1
	}
	return 0
}

func (s *Store) nextWakeDelay(sample Sample) (time.Duration, bool, error) {
	var deadline int64
	found := false
	err := s.db.View(func(tx *sqlite.Tx) error {
		queued, err := tx.Query("SELECT queue_boot_session_id,wait_deadline_monotonic_ns FROM requests WHERE state='queued'")
		if err != nil {
			return err
		}
		for _, row := range queued {
			value := rowInt(row, "wait_deadline_monotonic_ns")
			if stringValue(nullableString(row, "queue_boot_session_id")) != sample.BootSessionID {
				value = sample.MonotonicNS
			}
			if !found || value < deadline {
				deadline, found = value, true
			}
		}
		leases, err := tx.Query("SELECT boot_session_id,hard_end_monotonic_ns FROM leases WHERE state='active'")
		if err != nil {
			return err
		}
		for _, row := range leases {
			value := rowInt(row, "hard_end_monotonic_ns")
			if rowString(row, "boot_session_id") != sample.BootSessionID {
				value = sample.MonotonicNS
			}
			if !found || value < deadline {
				deadline, found = value, true
			}
		}
		return nil
	})
	if err != nil || !found {
		return 0, found, err
	}
	if deadline <= sample.MonotonicNS {
		return 0, true, nil
	}
	return time.Duration(deadline - sample.MonotonicNS), true, nil
}
