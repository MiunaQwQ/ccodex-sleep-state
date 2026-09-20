package gateway

import (
	"errors"
	"time"
)

// Caller holds s.mu. Pauses belong to one credential/model, not the shared pool.
func (e *Engine) otherEligibleRouteLocked(s *session, current int, now time.Time) int {
	pinned := e.effectivePinnedRoute()
	for offset := 1; offset < len(e.routes); offset++ {
		i := (current + offset) % len(e.routes)
		if pinned != "" && e.routes[i].ID != pinned {
			continue
		}
		state := e.pool.Get(e.routes[i].ID).State
		if (state == "disabled" || state == "failed") || now.Before(s.nodePauses[i]) {
			continue
		}
		return i
	}
	return -1
}

func (e *Engine) effectivePinnedRoute() string {
	if e.pool.Get(e.settings().PinnedRoute).State == "failed" {
		return ""
	}
	return e.settings().PinnedRoute
}

func (e *Engine) pauseMismatchedRoute(s *session, route, blocks int, now time.Time, requestStarted ...time.Time) {
	if blocks != 11 || blocks == s.policy.Blocks {
		return
	}
	s.mu.Lock()
	// A late response must not discard a newer main acquired while that
	// request was in flight through this route.
	if len(requestStarted) > 0 {
		if current, ok := s.state.Acquire(now); ok && current.Route == route && current.AcquiredAt.After(requestStarted[0]) {
			s.mu.Unlock()
			return
		}
	}
	if s.nodePauses == nil {
		s.nodePauses = map[int]time.Time{}
	}
	next := e.otherEligibleRouteLocked(s, route, now)
	if next < 0 {
		// Keep one usable route for passthrough; a 312 is never a valid state.
		delete(s.nodePauses, route)
		s.retainedRoute, s.retainedLast, s.fallbackRoute = route, true, route
		s.cursor = (route + 1) % len(e.routes)
	} else {
		s.nodePauses[route] = now.Add(time.Duration(e.settings().CooldownSeconds) * time.Second)
		s.fallbackRoute, s.cursor = next, next
		s.retainedLast = false
		// Pausing new collection on an exit says nothing about already held
		// cards. Only a response to a request using a card can invalidate it.
	}
	s.mu.Unlock()
}

// Transport failures disable an exit across model sessions until manually
// restored for automatic collection. Existing cards stay bound to this route;
// network failure is not state invalidation. A completed 11-block probe never
// calls this function.
func (e *Engine) failRoute(route int) {
	_ = e.pool.FinishProbe(e.routes[route].ID, "failed", "network_failed")
	e.mu.Lock()
	for _, s := range e.sessions {
		s.mu.Lock()
		if next := e.otherEligibleRouteLocked(s, route, time.Now()); next >= 0 {
			s.fallbackRoute, s.cursor = next, next
		}
		s.mu.Unlock()
		e.persistState(s)
	}
	e.mu.Unlock()
	e.log.Info("node_failed_until_manual_resume", "route", e.routes[route].ID, "reason", "network_failed")
	e.signal()
}

func (e *Engine) fallbackRouteFor(s *session, now time.Time) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	current := s.fallbackRoute
	state := e.pool.Get(e.routes[current].ID).State
	if now.Before(s.nodePauses[current]) || state == "disabled" || state == "failed" {
		if next := e.otherEligibleRouteLocked(s, current, now); next >= 0 {
			s.fallbackRoute = next
		}
	}
	return s.fallbackRoute
}

func (e *Engine) advanceUnavailableRoute(s *session, route int, now time.Time) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if next := e.otherEligibleRouteLocked(s, route, now); next >= 0 {
		s.fallbackRoute, s.cursor = next, next
	}
}

// ResumeNode restores eligibility only; it does not reset probe/account budgets.
func (e *Engine) ResumeNode(sessionID, routeID string) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	for _, s := range e.sessions {
		if s.id != sessionID {
			continue
		}
		for i, route := range e.routes {
			if route.ID != routeID {
				continue
			}
			if e.pool.Get(routeID).State == "failed" {
				if err := e.pool.Change([]string{routeID}, "available", "manual", false); err != nil {
					return err
				}
			}
			s.mu.Lock()
			delete(s.nodePauses, i)
			s.mu.Unlock()
			e.signal()
			return nil
		}
		return errors.New("节点已不存在，请刷新节点列表")
	}
	return errors.New("会话已不存在，请刷新节点列表")
}

func (e *Engine) syncRoutes(s *session, now time.Time) {
	blocked := map[int]bool{}
	for i, r := range e.routes {
		p := e.pool.Get(r.ID)
		blocked[i] = p.State == "disabled" || (p.State == "failed" && p.Reason != "network_failed")
	}
	if s.state.SuspendRoutes(blocked, now) {
		e.persistState(s)
	}
}
