package turnstate

import (
	"errors"
	"time"
)

func persistedSnapshot(v Snapshot, routeID func(int) string) PersistedSnapshot {
	p := PersistedSnapshot{Token: v.Token.Value, RouteID: routeID(v.Route), Issued: v.Token.Issued, AcquiredAt: v.AcquiredAt}
	if v.ManualRoute {
		p.SourceRouteID = v.SourceRouteID
		if p.SourceRouteID == "" {
			p.SourceRouteID = routeID(v.SourceRoute)
		}
	}
	return p
}

// SwitchRoute changes only the exact active version displayed by the panel.
// Persist before publishing; a failed save leaves the pool and version intact.
// The new version prevents late replies from the old egress invalidating it.
func (s *Store) SwitchRoute(id string, version uint64, route int, now time.Time, routeID func(int) string, save func(Persisted) error) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if id == "" || s.active.Token.Fingerprint != id || s.active.Version != version || !s.policy.Accept(s.active.Token, now) {
		return errors.New("主票或节点已变化，请刷新后重新选择")
	}
	if s.active.Route == route {
		return nil
	}
	candidate := s.active
	if !candidate.ManualRoute {
		candidate.SourceRoute = candidate.Route
		candidate.SourceRouteID = routeID(candidate.Route)
	}
	candidate.Route, candidate.ManualRoute, candidate.Version = route, true, s.version+1
	// Construct an independent export so even expiry/pruning stays private until
	// the save succeeds. Other cards and their original bindings are preserved.
	next := &Store{policy: s.policy, standbyLimit: s.standbyLimit, active: candidate,
		standby: append([]Snapshot(nil), s.standby...), parked: append([]Snapshot(nil), s.parked...),
		discarded: append([]DiscardedState(nil), s.discarded...), version: candidate.Version}
	if save != nil {
		if err := save(next.exportLocked(now, routeID)); err != nil {
			return err
		}
	}
	s.active, s.version, s.strikes = candidate, candidate.Version, 0
	s.record("route_switched", candidate, now)
	return nil
}
