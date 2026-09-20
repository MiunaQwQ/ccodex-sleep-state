package gateway

import (
	"github.com/gylive/ccodex-sleep-state/internal/turnstate"
	"time"
)

// ResponseStateCheck describes evidence from a real response, independently
// from the collector. Missing metadata is unknown, never a successful check.
type ResponseStateCheck struct {
	At         time.Time `json:"at"`
	StateID    string    `json:"state_id"`
	RouteID    string    `json:"route_id"`
	RouteLabel string    `json:"route_label"`
	Result     string    `json:"result"`
	HTTPStatus int       `json:"http_status"`
	Bootstrap  string    `json:"bootstrap,omitempty"`
}

func (e *Engine) recordStateCheck(s *session, used turnstate.Snapshot, result string, status int) {
	e.recordResponseCheck(s, used, result, status, "")
}

func (e *Engine) recordResponseCheck(s *session, used turnstate.Snapshot, result string, status int, bootstrap string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	route := e.routes[used.Route]
	s.lastStateCheck = ResponseStateCheck{At: time.Now(), StateID: used.Token.Fingerprint, RouteID: route.ID, RouteLabel: route.DisplayName, Result: result, HTTPStatus: status, Bootstrap: bootstrap}
}

// Node results belong to one account/credential/model session. They are
// observations of individual requests, not permanent model-quality labels.
type NodeObservation struct {
	Route          string    `json:"route"`
	Result         string    `json:"result"`
	HTTPStatus     int       `json:"http_status"`
	Blocks         int       `json:"blocks"`
	Length         int       `json:"length"`
	ExpectedLength int       `json:"expected_length"`
	Source         string    `json:"source"`
	At             time.Time `json:"at"`
	DurationMS     int64     `json:"duration_ms"`
	Attempts       uint64    `json:"attempts"`
	Matches        uint64    `json:"matches"`
	PauseUntil     time.Time `json:"pause_until"`
	PauseSeconds   int       `json:"pause_seconds"`
	RetainedLast   bool      `json:"retained_last"`
	PoolState      string    `json:"pool_state,omitempty"`
}

func (e *Engine) recordNode(s *session, route int, result string, status, blocks int, duration time.Duration, source string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.nodes == nil {
		s.nodes = make(map[string]NodeObservation)
	}
	id := e.routes[route].ID
	old := s.nodes[id]
	length := 0
	if blocks > 0 {
		length = ((57 + 16*blocks + 2) / 3) * 4
	}
	matches := old.Matches
	if result == "accepted" {
		matches++
		if s.retainedRoute == route {
			s.retainedLast = false
		}
	}
	s.nodes[id] = NodeObservation{Route: id, Result: result, HTTPStatus: status, Blocks: blocks, Length: length, ExpectedLength: s.policy.Length, Source: source, At: time.Now().UTC(), DurationMS: duration.Milliseconds(), Attempts: old.Attempts + 1, Matches: matches}
	if source == "probe" {
		// Keep the probe's source separate from later replies on this route.
		observation := s.nodes[id]
		s.lastProbeResult = &observation
	}
}

func (e *Engine) invalidate(s *session, used turnstate.Snapshot, route int, result string, blocks int) bool {
	invalidated := false
	s.mu.Lock()
	if s.state.Invalidate(used, time.Now()) {
		s.nextProbe = time.Time{}
		s.cursor = (route + 1) % len(e.routes)
		s.diagnostic, s.observedBlocks = result, blocks
		invalidated = true
	}
	s.mu.Unlock()
	if invalidated {
		next, _ := s.state.Acquire(time.Now())
		e.log.Info("state_invalidated", "model", s.model, "state_id", used.Token.Fingerprint,
			"state_version", used.Version, "reason", result, "route", e.routes[route].ID,
			"promoted_state_id", next.Token.Fingerprint, "standby", s.state.Status(time.Now()).Standby)
		e.collection.RecordUseFailure(s.backupKey, UseFailure{At: time.Now(), Reason: result, Route: e.routes[route].ID, StateID: used.Token.Fingerprint})
		e.persistState(s)
	}
	e.signal()
	return invalidated
}
