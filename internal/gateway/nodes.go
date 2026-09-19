package gateway

import (
	"github.com/gylive/ccodex-sleep-state/internal/turnstate"
	"time"
)

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
	}
	s.nodes[id] = NodeObservation{Route: id, Result: result, HTTPStatus: status, Blocks: blocks, Length: length, ExpectedLength: s.policy.Length, Source: source, At: time.Now().UTC(), DurationMS: duration.Milliseconds(), Attempts: old.Attempts + 1, Matches: matches}
}

func (e *Engine) invalidate(s *session, used turnstate.Snapshot, route int, result string, blocks int) {
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
		e.persistState(s)
	}
	e.signal()
}
