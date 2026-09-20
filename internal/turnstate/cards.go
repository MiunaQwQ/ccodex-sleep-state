package turnstate

import "time"

// Card is public metadata. It deliberately contains no usable state token.
type Card struct {
	ID               string    `json:"id"`
	Role             string    `json:"role"`
	RouteID          string    `json:"route_id"`
	RouteLabel       string    `json:"route_label"`
	AcquiredAt       time.Time `json:"acquired_at"`
	IssuedAt         time.Time `json:"issued_at"`
	ExpiresAt        time.Time `json:"expires_at"`
	UsableUntil      time.Time `json:"usable_until"`
	RemainingSeconds int       `json:"remaining_seconds"`
	AgeSeconds       int       `json:"age_seconds"`
	Version          uint64    `json:"version"`
	SourceRouteID    string    `json:"source_route_id"`
	SourceRouteLabel string    `json:"source_route_label"`
	ManualRoute      bool      `json:"manual_route"`
}

func (s *Store) Cards(now time.Time, route func(int) (string, string)) []Card {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.promote(now)
	result := []Card{}
	add := func(v Snapshot, role string) {
		if !s.policy.Accept(v.Token, now) {
			return
		}
		id, label := route(v.Route)
		expires := v.Token.Issued.Add(s.policy.TTL)
		age := 0
		if !v.AcquiredAt.IsZero() {
			age = max(0, int(now.Sub(v.AcquiredAt).Seconds()))
		}
		sourceID, sourceLabel := id, label
		if v.ManualRoute {
			sourceID, sourceLabel = route(v.SourceRoute)
		}
		result = append(result, Card{ID: v.Token.Fingerprint, Role: role, RouteID: id, RouteLabel: label, AcquiredAt: v.AcquiredAt, IssuedAt: v.Token.Issued, ExpiresAt: expires, UsableUntil: expires.Add(-30 * time.Second), RemainingSeconds: max(0, int(expires.Sub(now).Seconds())), AgeSeconds: age, Version: v.Version, SourceRouteID: sourceID, SourceRouteLabel: sourceLabel, ManualRoute: v.ManualRoute})
	}
	add(s.active, "active")
	for _, candidate := range s.standby {
		add(candidate, "standby")
	}
	for _, candidate := range s.parked {
		add(candidate, "parked")
	}
	return result
}
