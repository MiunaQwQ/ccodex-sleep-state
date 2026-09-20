package turnstate

import (
	"errors"
	"time"
)

var ErrCardNotFound = errors.New("这张票已不存在或已到期，请刷新后重试")

// Only the fingerprint is retained, so a late probe or fallback response
// cannot reintroduce a discarded ticket, including after a restart.
type DiscardedState struct {
	ID        string    `json:"id"`
	ExpiresAt time.Time `json:"expires_at"`
}

func (s *Store) isDiscarded(id string, now time.Time) bool {
	for _, v := range s.discarded {
		if v.ID == id && now.Before(v.ExpiresAt) {
			return true
		}
	}
	return false
}

// Discard commits an exact card deletion before publishing the new pool.
// Caller serializes this write with other backup saves. A failed save leaves
// the original pool untouched; in-flight request snapshots remain immutable.
func (s *Store) Discard(id string, now time.Time, routeID func(int) string, save func(Persisted) error) (string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if id == "" {
		return "", ErrCardNotFound
	}
	next := &Store{
		policy: s.policy, standbyLimit: s.standbyLimit, holdActive: s.holdActive,
		active: s.active, ready: s.ready, version: s.version, strikes: s.strikes,
		standby: append([]Snapshot(nil), s.standby...), parked: append([]Snapshot(nil), s.parked...),
		discarded: append([]DiscardedState(nil), s.discarded...), events: append([]Event(nil), s.events...),
	}
	next.promote(now)
	var removed Snapshot
	role := ""
	if next.active.Token.Fingerprint == id {
		removed, role = next.active, "active"
		next.active, next.strikes = Snapshot{}, 0
	}
	remove := func(cards []Snapshot, candidateRole string) []Snapshot {
		kept := cards[:0]
		for _, card := range cards {
			if card.Token.Fingerprint == id {
				if role == "" {
					removed, role = card, candidateRole
				}
			} else {
				kept = append(kept, card)
			}
		}
		return kept
	}
	next.standby = remove(next.standby, "standby")
	next.parked = remove(next.parked, "parked")
	if role == "" || !s.policy.Accept(removed.Token, now) {
		return "", ErrCardNotFound
	}
	// Settings permit at most one hour of validity. Keep the deletion through
	// that window even if the user later increases a shorter local TTL.
	next.discarded = append(next.discarded, DiscardedState{id, removed.Token.Issued.Add(max(time.Hour, s.policy.TTL))})
	next.record("discarded", removed, now)
	next.promote(now)
	if save != nil {
		if err := save(next.exportLocked(now, routeID)); err != nil {
			return "", err
		}
	}
	s.active, s.ready, s.strikes, s.version = next.active, next.ready, next.strikes, next.version
	s.standby, s.parked, s.discarded, s.events = next.standby, next.parked, next.discarded, next.events
	return role, nil
}
