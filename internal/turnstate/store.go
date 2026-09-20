package turnstate

import (
	"sort"
	"sync"
	"time"
)

type Snapshot struct {
	AcquiredAt time.Time
	Token      Token
	Route      int
	Version    uint64
}

// PersistedSnapshot is the private, opaque form used by the local backup.
// RouteID is stable across reloads; the in-memory route index is not.
type PersistedSnapshot struct {
	AcquiredAt time.Time `json:"acquired_at,omitempty"`
	Token      string    `json:"token"`
	RouteID    string    `json:"route_id"`
	Issued     time.Time `json:"issued"`
}

// Persisted is intentionally limited to state candidates. Credentials and
// request bodies never enter the backup file.
type Persisted struct {
	Active    *PersistedSnapshot  `json:"active,omitempty"`
	Standby   []PersistedSnapshot `json:"standby,omitempty"`
	Parked    []PersistedSnapshot `json:"parked,omitempty"`
	SavedAt   time.Time           `json:"saved_at"`
	Discarded []DiscardedState    `json:"discarded,omitempty"`
}

// Store publishes one active state and keeps a bounded set of standby states.
// A response observation can invalidate only the snapshot used by that
// request; the next valid standby is promoted immediately.
type Store struct {
	standbyLimit  int
	holdActive    bool
	mu            sync.Mutex
	policy        Policy
	active, ready Snapshot
	standby       []Snapshot
	parked        []Snapshot
	discarded     []DiscardedState
	suspended     map[int]bool
	events        []Event
	version       uint64
	strikes       int
	candidates    uint64
}

const maxStandby = 16

func New(p Policy) *Store { return &Store{policy: p, standbyLimit: maxStandby} }

func (s *Store) SetStandbyLimit(n int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.standbyLimit = max(0, min(maxStandby, n))
	s.sortStandby()
}

func (s *Store) Acquire(now time.Time) (Snapshot, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.promote(now)
	return s.active, s.policy.Accept(s.active.Token, now)
}

func (s *Store) promote(now time.Time) {
	s.prune(now)
	a := s.active
	activeOK := s.policy.Accept(a.Token, now)
	if len(s.standby) == 0 {
		s.ready = Snapshot{}
		return
	}
	candidate := s.standby[0]
	if activeOK && candidate.Token.Fingerprint == a.Token.Fingerprint {
		s.standby = s.standby[1:]
		s.promote(now)
		return
	}
	shouldPromote := !activeOK || s.strikes >= 2
	if shouldPromote {
		s.version++
		candidate.Version = s.version
		s.active = candidate
		s.record("promoted", candidate, now)
		s.standby = s.standby[1:]
		s.strikes = 0
	}
	if len(s.standby) > 0 {
		s.ready = s.standby[0]
	} else {
		s.ready = Snapshot{}
	}
}

func (s *Store) prune(now time.Time) {
	keptDiscards := s.discarded[:0]
	for _, v := range s.discarded {
		if now.Before(v.ExpiresAt) {
			keptDiscards = append(keptDiscards, v)
		}
	}
	s.discarded = keptDiscards
	if s.active.Token.Value != "" && !s.policy.Accept(s.active.Token, now) {
		s.record("expired", s.active, now)
		s.active = Snapshot{}
	}
	kept := s.standby[:0]
	seen := map[string]bool{}
	for _, candidate := range s.standby {
		if candidate.Token.Value == "" || !s.policy.Accept(candidate.Token, now) || seen[candidate.Token.Fingerprint] || candidate.Token.Fingerprint == s.active.Token.Fingerprint {
			continue
		}
		seen[candidate.Token.Fingerprint] = true
		kept = append(kept, candidate)
	}
	s.standby = kept
	s.ready = Snapshot{}
	if len(s.standby) > 0 {
		s.ready = s.standby[0]
	}
}

func (s *Store) Offer(t Token, route int, now time.Time) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.candidates++
	s.promote(now)
	if !s.policy.Accept(t, now) || s.suspended[route] || s.isDiscarded(t.Fingerprint, now) {
		return false
	}
	if t.Fingerprint == s.active.Token.Fingerprint {
		return true
	}
	for _, candidate := range s.standby {
		if candidate.Token.Fingerprint == t.Fingerprint {
			return true
		}
	}
	candidate := Snapshot{Token: t, Route: route, AcquiredAt: now}
	if !s.policy.Accept(s.active.Token, now) {
		s.version++
		candidate.Version = s.version
		s.active = candidate
		s.record("acquired_main", candidate, now)
		s.strikes = 0
		s.promote(now)
		return true
	}
	if len(s.standby) >= s.standbyLimit {
		return false
	}
	s.standby = append(s.standby, candidate)
	s.record("acquired_standby", candidate, now)
	s.sortStandby()
	s.promote(now)
	return true
}

// Bootstrap fills an empty usable pool from a completed ordinary response.
// It never replaces a concurrently acquired active state or adds a standby.
func (s *Store) Bootstrap(t Token, route int, now time.Time) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.promote(now)
	if s.policy.Accept(s.active.Token, now) || !s.policy.Accept(t, now) || s.suspended[route] || s.isDiscarded(t.Fingerprint, now) {
		return false
	}
	s.candidates++
	s.version++
	s.active = Snapshot{Token: t, Route: route, AcquiredAt: now, Version: s.version}
	s.record("acquired_main", s.active, now)
	s.strikes = 0
	return true
}

func (s *Store) sortStandby() {
	sort.SliceStable(s.standby, func(i, j int) bool { return acquired(s.standby[i]).Before(acquired(s.standby[j])) })
	limit := s.standbyLimit
	if s.active.Token.Value == "" {
		limit++
	}
	if len(s.standby) > limit {
		s.standby = s.standby[:limit]
	}
}

func (s *Store) Observe(value string, used Snapshot, now time.Time) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if value == "" {
		return false
	}
	s.candidates++
	t, err := Parse(value)
	suspect := err != nil || !s.policy.Accept(t, now)
	if used.Token.Value != "" && used.Version == s.active.Version && used.Token.Fingerprint == s.active.Token.Fingerprint {
		if suspect {
			s.strikes++
		} else {
			s.strikes = 0
		}
	}
	return suspect
}

func (s *Store) NeedsRefresh(now time.Time) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.promote(now)
	return !s.policy.Accept(s.active.Token, now) || s.strikes >= 2 || now.Add(s.policy.Refresh).After(s.active.Token.Issued.Add(s.policy.TTL))
}

type Status struct {
	Usable           bool   `json:"usable"`
	Version          uint64 `json:"version"`
	RemainingSeconds int    `json:"remaining_seconds"`
	Ready            bool   `json:"ready"`
	Standby          int    `json:"standby"`
	Strikes          int    `json:"strikes"`
	Candidates       uint64 `json:"observations"`
}

func (s *Store) Status(now time.Time) Status {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.promote(now)
	left := int(s.active.Token.Issued.Add(s.policy.TTL).Sub(now).Seconds())
	if left < 0 {
		left = 0
	}
	return Status{s.policy.Accept(s.active.Token, now), s.active.Version, left, s.policy.Accept(s.ready.Token, now), len(s.standby), s.strikes, s.candidates}
}

// Export returns only currently acceptable candidates, suitable for the
// permission-0600 local backup. The route ID is supplied by the gateway.
func (s *Store) Export(now time.Time, routeID func(int) string) Persisted {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.exportLocked(now, routeID)
}

func (s *Store) exportLocked(now time.Time, routeID func(int) string) Persisted {
	s.promote(now)
	result := Persisted{SavedAt: now.UTC(), Discarded: append([]DiscardedState(nil), s.discarded...)}
	if s.policy.Accept(s.active.Token, now) {
		result.Active = &PersistedSnapshot{Token: s.active.Token.Value, RouteID: routeID(s.active.Route), Issued: s.active.Token.Issued, AcquiredAt: s.active.AcquiredAt}
	}
	for _, candidate := range s.standby {
		if s.policy.Accept(candidate.Token, now) {
			result.Standby = append(result.Standby, PersistedSnapshot{Token: candidate.Token.Value, RouteID: routeID(candidate.Route), Issued: candidate.Token.Issued, AcquiredAt: candidate.AcquiredAt})
		}
	}
	for _, candidate := range s.parked {
		if s.policy.Accept(candidate.Token, now) {
			result.Parked = append(result.Parked, PersistedSnapshot{Token: candidate.Token.Value, RouteID: routeID(candidate.Route), Issued: candidate.Token.Issued, AcquiredAt: candidate.AcquiredAt})
		}
	}
	return result
}

// Restore imports a previously accepted backup. Unknown routes are skipped;
// no state is attached to a different egress by accident.
func (s *Store) Restore(p Persisted, now time.Time, routeIndex func(string) (int, bool)) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.active, s.ready, s.standby, s.parked = Snapshot{}, Snapshot{}, nil, nil
	s.discarded = append([]DiscardedState(nil), p.Discarded...)
	add := func(raw PersistedSnapshot) (Snapshot, bool) {
		route, ok := routeIndex(raw.RouteID)
		if !ok {
			return Snapshot{}, false
		}
		t, err := Parse(raw.Token)
		if err != nil || !s.policy.Accept(t, now) || s.isDiscarded(t.Fingerprint, now) {
			return Snapshot{}, false
		}
		return Snapshot{Token: t, Route: route, AcquiredAt: raw.AcquiredAt}, true
	}
	if p.Active != nil {
		if active, ok := add(*p.Active); ok {
			s.version++
			active.Version = s.version
			s.active = active
		}
	}
	for _, raw := range p.Standby {
		if candidate, ok := add(raw); ok && candidate.Token.Fingerprint != s.active.Token.Fingerprint {
			s.standby = append(s.standby, candidate)
		}
	}
	for _, raw := range p.Parked {
		if candidate, ok := add(raw); ok {
			s.parked = append(s.parked, candidate)
		}
	}
	s.sortStandby()
	s.promote(now)
	return len(s.standby) + func() int {
		if s.active.Token.Value != "" {
			return 1
		}
		return 0
	}()
}

// RejectAndPromote changes only future admissions. In-flight snapshots remain
// immutable and a stale failure cannot invalidate a newer active state.
func (s *Store) RejectAndPromote(used Snapshot, now time.Time) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if used.Version != s.active.Version || used.Token.Fingerprint != s.active.Token.Fingerprint {
		return false
	}
	s.active = Snapshot{}
	s.strikes = 0
	s.promote(now)
	return s.policy.Accept(s.active.Token, now)
}

func (s *Store) HoldActive(hold bool) { s.mu.Lock(); defer s.mu.Unlock(); s.holdActive = hold }

func (s *Store) DropRoute(route int, now time.Time) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.active.Route == route {
		s.active = Snapshot{}
	}
	kept := s.standby[:0]
	for _, card := range s.standby {
		if card.Route != route {
			kept = append(kept, card)
		}
	}
	s.standby = kept
	s.promote(now)
}

// Invalidate only the snapshot actually used by a failing request. Late
// responses must not invalidate a newer active state.
func (s *Store) Invalidate(used Snapshot, now time.Time) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if used.Token.Value == "" || used.Version != s.active.Version || used.Token.Fingerprint != s.active.Token.Fingerprint {
		return false
	}
	s.record("invalidated", s.active, now)
	s.active = Snapshot{}
	s.strikes = 0
	s.promote(now)
	return true
}
