package turnstate

import "time"

type Event struct {
	At      time.Time `json:"at"`
	Kind    string    `json:"kind"`
	StateID string    `json:"state_id"`
	Route   int       `json:"-"`
}

func acquired(v Snapshot) time.Time {
	if !v.AcquiredAt.IsZero() {
		return v.AcquiredAt
	}
	return v.Token.Issued
}
func (s *Store) record(kind string, v Snapshot, now time.Time) {
	if v.Token.Value == "" {
		return
	}
	s.events = append(s.events, Event{now, kind, v.Token.Fingerprint, v.Route})
	if len(s.events) > 40 {
		s.events = append([]Event(nil), s.events[len(s.events)-40:]...)
	}
}
func (s *Store) Events() []Event {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]Event(nil), s.events...)
}

// SuspendRoutes parks cards without changing their source or treating them as
// invalid. Only explicit disabled/non-network failed exits are parked; a simple
// network failure continues to preserve and admit the user's bound main card.
func (s *Store) SuspendRoutes(blocked map[int]bool, now time.Time) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.suspended = blocked
	changed := false
	if s.active.Token.Value != "" && blocked[s.active.Route] {
		s.record("source_suspended", s.active, now)
		s.parked = append(s.parked, s.active)
		s.active = Snapshot{}
		changed = true
	}
	kept := s.standby[:0]
	for _, v := range s.standby {
		if blocked[v.Route] {
			s.record("source_suspended", v, now)
			s.parked = append(s.parked, v)
			changed = true
		} else {
			kept = append(kept, v)
		}
	}
	s.standby = kept
	capacity := s.standbyLimit
	if s.active.Token.Value == "" {
		capacity++
	}
	parked := s.parked[:0]
	for _, v := range s.parked {
		if !s.policy.Accept(v.Token, now) {
			s.record("expired", v, now)
			changed = true
			continue
		}
		if !blocked[v.Route] && len(s.standby) < capacity {
			s.standby = append(s.standby, v)
			s.record("source_resumed", v, now)
			changed = true
		} else {
			parked = append(parked, v)
		}
	}
	s.parked = parked
	// Bound archived metadata independently of the active FIFO queue.
	if len(s.parked) > maxStandby {
		s.parked = s.parked[:maxStandby]
	}
	s.sortStandby()
	s.promote(now)
	return changed
}

func (s *Store) UpdatePolicy(p Policy, limit int, now time.Time) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.policy = p
	s.standbyLimit = max(0, min(maxStandby, limit))
	s.sortStandby()
	s.promote(now)
}
