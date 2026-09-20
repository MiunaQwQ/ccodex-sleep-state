package gateway

import (
	"github.com/gylive/ccodex-sleep-state/internal/settings"
	"github.com/gylive/ccodex-sleep-state/internal/turnstate"
	"time"
)

func (e *Engine) settings() settings.Config {
	if c := e.liveConfig.Load(); c != nil {
		return *c
	}
	return e.config
}

// Only timing fields change here; source transports and account identity stay
// immutable. Readers load a whole settings value rather than racing field writes.
func (e *Engine) UpdateTiming(c settings.Config) {
	next := e.settings()
	next.Collection = c.Collection
	next.ProbeSeconds, next.CooldownSeconds, next.MaxProbes = c.ProbeSeconds, c.CooldownSeconds, c.MaxProbes
	next.TTLSeconds, next.RefreshSeconds = c.TTLSeconds, c.RefreshSeconds
	e.liveConfig.Store(&next)
	e.mu.Lock()
	for _, s := range e.sessions {
		limit := e.standbyTarget()
		s.state.UpdatePolicy(turnstate.Policy{Blocks: s.policy.Blocks, TTL: time.Duration(c.TTLSeconds) * time.Second, Refresh: time.Duration(c.RefreshSeconds) * time.Second}, limit, time.Now())
		e.persistState(s)
	}
	e.mu.Unlock()
	e.signal()
}
