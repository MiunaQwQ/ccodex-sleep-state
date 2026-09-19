package gateway

import (
	"github.com/gylive/ccodex-sleep-state/internal/turnstate"
	"time"
)

func (e *Engine) SetCollectionBudget(b *CollectionBudget) {
	if b != nil {
		e.collection = b
	}
}

func (e *Engine) cards(s *session, now time.Time) []turnstate.Card {
	return s.state.Cards(now, func(i int) (string, string) {
		if i < 0 || i >= len(e.routes) {
			return "", ""
		}
		r := e.routes[i]
		return r.ID, r.DisplayName
	})
}

func (e *Engine) standbyTarget() int {
	if !e.config.PoolEnabled {
		return 0
	}
	return e.config.Collection.StandbyTarget
}

// The first reserve is immediate. Additional/replacement reserves are spaced
// by acquisition time. At capacity only the oldest reserve is replaced.
func (e *Engine) collectionAt(s *session, now time.Time) (time.Time, string) {
	cards := e.cards(s, now)
	if len(cards) == 0 {
		return now, "missing_active"
	}
	target := e.standbyTarget()
	if target == 0 {
		if !e.onDemand.Load() {
			return cards[0].ExpiresAt.Add(-time.Duration(e.config.RefreshSeconds) * time.Second), "refresh_active"
		}
		return time.Time{}, "pool_ready"
	}
	if len(cards) == 1 {
		return now, "missing_standby"
	}
	var latest time.Time
	oldest := cards[1].ExpiresAt
	for _, card := range cards[1:] {
		acquired := card.AcquiredAt
		if acquired.IsZero() {
			acquired = card.IssuedAt
		}
		if acquired.After(latest) {
			latest = acquired
		}
		if card.ExpiresAt.Before(oldest) {
			oldest = card.ExpiresAt
		}
	}
	at := latest.Add(time.Duration(e.config.Collection.StandbySpacingSeconds) * time.Second)
	if len(cards)-1 >= target {
		refresh := oldest.Add(-time.Duration(e.config.RefreshSeconds) * time.Second)
		if refresh.After(at) {
			at = refresh
		}
		return at, "standby_refresh"
	}
	return at, "standby_spacing"
}

// Caller holds s.mu. Backoff and budgets are independent of success cooldown;
// rejecting a state can break the latter without resetting either budget.
func (e *Engine) collectionStatus(s *session, now time.Time) CollectionStatus {
	status := e.collection.Status(s.backupKey, now, e.config.Collection)
	at, reason := e.collectionAt(s, now)
	if at.IsZero() {
		status.Reason = "pool_ready"
		status.NextAt = time.Time{}
	} else {
		if s.nextProbe.After(at) {
			at, reason = s.nextProbe, "success_cooldown"
		}
		if !status.NextAt.IsZero() && !status.NextAt.Before(at) {
			at, reason = status.NextAt, status.Reason
		}
		status.NextAt, status.Reason = at, reason
	}
	status.Idle = now.Sub(s.lastUsed) >= time.Duration(e.config.Collection.IdleSeconds)*time.Second
	if status.Idle {
		status.Reason = "idle"
	}
	status.WaitSeconds = max(0, int(status.NextAt.Sub(now).Seconds())+1)
	return status
}
