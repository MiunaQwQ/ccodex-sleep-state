package gateway

import (
	"math/rand/v2"
	"time"
)

// A cycle spans collection batches, successful acquisitions and restarts.
// Route IDs are stable across source reordering; credentials remain hashed in
// the existing budget key. Explicit fixed/manual probes do not move this deck.
type probeRotation struct {
	Cycle     uint64          `json:"cycle"`
	Order     []string        `json:"order"`
	Visited   map[string]bool `json:"visited"`
	UpdatedAt time.Time       `json:"updated_at"`
}

type RotationStatus struct {
	Cycle     uint64 `json:"cycle"`
	Completed int    `json:"completed"`
	Total     int    `json:"total"`
}

func rotationStatus(r probeRotation) RotationStatus {
	status := RotationStatus{Cycle: r.Cycle, Total: len(r.Order)}
	for _, id := range r.Order {
		if r.Visited[id] {
			status.Completed++
		}
	}
	return status
}

// Peek without consuming: cooldown, budget or claim failures must not skip an
// untested node. A paused, unvisited node remains pending until it recovers;
// nodes already tried cannot start another cycle ahead of it.
func (b *CollectionBudget) rotationCandidate(key string, members []string, ready map[string]bool, allowNext bool, now time.Time) string {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.err != nil || len(members) == 0 {
		return ""
	}
	if b.doc.Rotations == nil {
		b.doc.Rotations = map[string]probeRotation{}
	}
	r := b.doc.Rotations[key]
	allowed := map[string]bool{}
	for _, id := range members {
		allowed[id] = true
	}
	order := make([]string, 0, len(members))
	for _, id := range r.Order {
		if allowed[id] {
			order = append(order, id)
			delete(allowed, id)
		}
	}
	var added []string
	for _, id := range members {
		if allowed[id] {
			added = append(added, id)
			delete(allowed, id)
		}
	}
	rand.Shuffle(len(added), func(i, j int) { added[i], added[j] = added[j], added[i] })
	r.Order = append(order, added...)
	// Bound old IDs retained after repeated source replacements. Current-cycle
	// members keep their visit markers; removed nodes do not grow the file forever.
	if len(r.Visited) > 256 {
		current := map[string]bool{}
		for _, id := range r.Order {
			current[id] = true
		}
		for id := range r.Visited {
			if !current[id] {
				delete(r.Visited, id)
			}
		}
	}
	pending := false
	for _, id := range r.Order {
		if !r.Visited[id] {
			pending = true
			break
		}
	}
	if r.Cycle == 0 || !pending {
		if !allowNext {
			return ""
		}
		r.Cycle++
		r.Visited = map[string]bool{}
		rand.Shuffle(len(r.Order), func(i, j int) { r.Order[i], r.Order[j] = r.Order[j], r.Order[i] })
	}
	r.UpdatedAt = now
	b.doc.Rotations[key] = r
	for _, id := range r.Order {
		if !r.Visited[id] && ready[id] {
			return id
		}
	}
	return ""
}

// Persist before sending the probe. The same node cannot be picked again after
// a service restart even if its previous request was still in flight.
func (b *CollectionBudget) recordRotationAttempt(key, id string, now time.Time) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	r := b.doc.Rotations[key]
	if r.Visited == nil {
		r.Visited = map[string]bool{}
	}
	r.Visited[id] = true
	r.UpdatedAt = now
	b.doc.Rotations[key] = r
	return b.write()
}

func (e *Engine) nextProbeInRotation(s *session, tried map[int]bool, allowNext bool, now time.Time) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	var members []string
	ready := map[string]bool{}
	indices := map[string]int{}
	for i, route := range e.routes {
		entry := e.pool.Get(route.ID)
		if entry.State == "disabled" || entry.State == "failed" || (e.settings().PoolEnabled && entry.State != "available") {
			continue
		}
		members = append(members, route.ID)
		indices[route.ID] = i
		ready[route.ID] = !tried[i] && !now.Before(s.nodePauses[i])
	}
	id := e.collection.rotationCandidate(s.backupKey, members, ready, allowNext, now)
	if id == "" {
		return -1
	}
	return indices[id]
}
