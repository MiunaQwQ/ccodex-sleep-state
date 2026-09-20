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
	e.syncRoutes(s, now)
	return s.state.Cards(now, func(i int) (string, string) {
		if i < 0 || i >= len(e.routes) {
			return "", ""
		}
		r := e.routes[i]
		return r.ID, r.DisplayName
	})
}

func (e *Engine) standbyTarget() int {
	if !e.settings().PoolEnabled {
		return 0
	}
	return e.settings().Collection.StandbyTarget
}

// Fill only vacancies. A full pool never harvests replacement cards early.
func (e *Engine) collectionAt(s *session, now time.Time) (time.Time, string) {
	cards := e.cards(s, now)
	active := false
	usable := cards[:0]
	for _, card := range cards {
		if card.Role != "parked" {
			usable = append(usable, card)
			active = active || card.Role == "active"
		}
	}
	cards = usable
	if !active {
		return now, "missing_active"
	}
	target := e.standbyTarget()
	if len(cards) >= target+1 {
		return time.Time{}, "pool_ready"
	}
	if len(cards) == 1 {
		return now, "missing_standby"
	}
	latest := cards[len(cards)-1].AcquiredAt
	if latest.IsZero() {
		latest = cards[len(cards)-1].IssuedAt
	}
	return latest.Add(time.Duration(e.settings().Collection.StandbySpacingSeconds) * time.Second), "standby_spacing"
}

// No usable active state includes natural expiration and invalidation without
// a standby to promote. This is a priority hint, not a cooldown bypass.
func (e *Engine) seekingActive(s *session, now time.Time) bool {
	return e.settings().Collection.Cadence == "round" && !s.state.Status(now).Usable
}

// Caller holds s.mu. Compute one effective gate for scheduling, manual retry,
// HTTP Retry-After and the panel. An explicit manual random attempt may skip
// local pacing without resetting persisted history or upstream restrictions.
func (e *Engine) probeSchedule(s *session, now time.Time, manualRandom ...bool) CollectionStatus {
	urgent := len(manualRandom) > 0 && manualRandom[0]
	policy := e.settings().Collection
	if urgent {
		policy.Cadence = "round"
	}
	status := e.collection.Status(s.backupKey, now, policy, urgent)
	if !urgent && s.nextProbe.After(now) && s.nextProbe.After(status.NextAt) {
		status.NextAt, status.Reason = s.nextProbe, "success_cooldown"
	}
	if s.upstreamPause.After(now) && s.upstreamPause.After(status.NextAt) {
		status.NextAt, status.Reason = s.upstreamPause, "upstream_pause"
	}
	status.WaitSeconds = 0
	if status.NextAt.After(now) {
		status.WaitSeconds = int(status.NextAt.Sub(now).Seconds()) + 1
	}
	return status
}

// Caller holds s.mu. Automatic collection always respects local pacing.
// Only an explicit one-shot manual action may skip it; hard gates still apply.
func (e *Engine) collectionStatus(s *session, now time.Time) CollectionStatus {
	status := e.probeSchedule(s, now)
	at, reason := e.collectionAt(s, now)
	if at.IsZero() {
		status.Reason = "pool_ready"
		status.NextAt = time.Time{}
	} else {
		if !status.NextAt.IsZero() && !status.NextAt.Before(at) {
			at, reason = status.NextAt, status.Reason
		}
		status.NextAt, status.Reason = at, reason
	}
	status.Idle = e.collectionIdleLocked(s, now)
	if status.Idle && status.Reason != "pool_ready" {
		status.Reason = "idle"
	}
	status.WaitSeconds = 0
	if status.NextAt.After(now) {
		status.WaitSeconds = int(status.NextAt.Sub(now).Seconds()) + 1
	}
	if s.probing != nil {
		status.Reason, status.WaitSeconds = "collecting", 0
	}
	return status
}

// Caller holds s.mu. Metadata requests and probes cannot refresh AI activity.
func (e *Engine) collectionIdleLocked(s *session, now time.Time) bool {
	return s.aiBusy == 0 && now.Sub(s.lastAI) >= time.Duration(e.settings().Collection.IdleSeconds)*time.Second
}
