package lease

import (
	"sync"

	"github.com/relux-works/swarma-lease/internal/sqlite"
)

type TerminalEvent struct {
	Version   int            `json:"version"`
	Type      string         `json:"type"`
	Name      string         `json:"name"`
	RequestID string         `json:"request_id"`
	Sequence  int64          `json:"sequence"`
	Data      map[string]any `json:"data"`
}

type changeNotifier struct {
	mu       sync.Mutex
	changed  chan struct{}
	requests map[string]*requestWatch
}

type requestWatch struct {
	event       *TerminalEvent
	subscribers map[*requestSubscription]struct{}
}

type requestSubscription struct {
	notifier    *changeNotifier
	request     string
	ch          chan TerminalEvent
	deliverOnce sync.Once
	cancelOnce  sync.Once
}

func newChangeNotifier() *changeNotifier {
	return &changeNotifier{changed: make(chan struct{}), requests: make(map[string]*requestWatch)}
}

func (s *Store) changes() <-chan struct{} {
	return s.changesRef().current()
}

func (s *Store) changesRef() *changeNotifier {
	return s.notifier
}

func (n *changeNotifier) current() <-chan struct{} {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.changed
}

func (n *changeNotifier) signal() {
	n.mu.Lock()
	close(n.changed)
	n.changed = make(chan struct{})
	n.mu.Unlock()
}

func (s *Store) signalChanged() {
	s.changesRef().signal()
}

func (s *Store) publishEvents(events []TerminalEvent) {
	n := s.changesRef()
	n.mu.Lock()
	for _, event := range events {
		watch := n.requests[event.RequestID]
		if watch == nil || watch.event != nil {
			continue
		}
		copy := event
		watch.event = &copy
		for subscriber := range watch.subscribers {
			subscriber.deliver(*watch.event)
		}
	}
	n.mu.Unlock()
}

func (s *Store) subscribeRequest(requestID string) (*requestSubscription, error) {
	n := s.changesRef()
	subscription := &requestSubscription{notifier: n, request: requestID, ch: make(chan TerminalEvent, 1)}
	n.mu.Lock()
	watch := n.requests[requestID]
	if watch == nil {
		watch = &requestWatch{subscribers: make(map[*requestSubscription]struct{})}
		n.requests[requestID] = watch
	}
	watch.subscribers[subscription] = struct{}{}
	var cached *TerminalEvent
	if watch.event != nil {
		copy := *watch.event
		cached = &copy
	}
	n.mu.Unlock()
	if cached != nil {
		subscription.deliver(*cached)
		return subscription, nil
	}

	event, queued, err := s.eventSnapshot(requestID)
	if err != nil {
		subscription.cancel()
		return nil, err
	}
	if event != nil {
		s.publishEvents([]TerminalEvent{*event})
	} else if !queued {
		subscription.cancel()
		return nil, sqlite.ErrStorage
	}
	return subscription, nil
}

func (s *requestSubscription) deliver(event TerminalEvent) {
	s.deliverOnce.Do(func() { s.ch <- event })
}

func (s *Store) eventSnapshot(requestID string) (*TerminalEvent, bool, error) {
	var event *TerminalEvent
	var queued bool
	err := s.db.View(func(tx *sqlite.Tx) error {
		var err error
		event, queued, err = terminalEvent(tx, requestID)
		return err
	})
	return event, queued, err
}

func terminalEvent(tx *sqlite.Tx, requestID string) (*TerminalEvent, bool, error) {
	request, err := requestByID(tx, requestID)
	if err != nil {
		return nil, false, err
	}
	if request == nil {
		return nil, false, sqlite.ErrStorage
	}
	if request.State == "queued" {
		return nil, true, nil
	}
	name, toState := "", ""
	switch request.State {
	case "leased", "released", "expired":
		name, toState = "lease.granted", "leased"
	case "timed_out":
		name, toState = "request.timed_out", "timed_out"
	case "cancelled":
		name, toState = "request.cancelled", "cancelled"
	default:
		return nil, false, nil
	}
	rows, err := tx.Query("SELECT sequence FROM audit WHERE request_id=? AND target_id=? AND to_state=? ORDER BY sequence DESC LIMIT 1", requestID, requestID, toState)
	if err != nil {
		return nil, false, err
	}
	if len(rows) == 0 {
		return nil, false, sqlite.ErrStorage
	}
	data, err := requestResult(tx, requestID)
	if err != nil {
		return nil, false, err
	}
	return &TerminalEvent{Version: 1, Type: "event", Name: name, RequestID: requestID, Sequence: rowInt(rows[0], "sequence"), Data: data}, false, nil
}

func (s *requestSubscription) wait(done <-chan struct{}) (TerminalEvent, bool) {
	select {
	case event := <-s.ch:
		return event, true
	case <-done:
		return TerminalEvent{}, false
	}
}

func (s *requestSubscription) cancel() {
	s.cancelOnce.Do(func() {
		n := s.notifier
		n.mu.Lock()
		watch := n.requests[s.request]
		if watch != nil {
			delete(watch.subscribers, s)
			if len(watch.subscribers) == 0 {
				delete(n.requests, s.request)
			}
		}
		n.mu.Unlock()
	})
}
