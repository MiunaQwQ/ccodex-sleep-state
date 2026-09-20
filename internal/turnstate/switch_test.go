package turnstate

import (
	"errors"
	"testing"
	"time"
)

func TestSwitchRetainsTicketRejectsStaleResponseAndPersists(t *testing.T) {
	now := time.Now()
	p := Policy{10, time.Hour, 20 * time.Minute}
	s := New(p)
	a, _ := Parse(synthetic(10, now, 7))
	b, _ := Parse(synthetic(10, now, 8))
	s.Offer(a, 0, now)
	s.Offer(b, 2, now)
	before, _ := s.Acquire(now)
	routes := []string{"source", "chosen", "backup"}
	rid := func(i int) string { return routes[i] }
	saveFailure := errors.New("disk unavailable")
	if err := s.SwitchRoute(a.Fingerprint, before.Version, 1, now, rid, func(Persisted) error { return saveFailure }); !errors.Is(err, saveFailure) {
		t.Fatal(err)
	}
	if got, _ := s.Acquire(now); got != before {
		t.Fatal("failed save mutated active")
	}
	var saved Persisted
	if err := s.SwitchRoute(a.Fingerprint, before.Version, 1, now, rid, func(p Persisted) error { saved = p; return nil }); err != nil {
		t.Fatal(err)
	}
	after, _ := s.Acquire(now)
	if after.Token != before.Token || after.AcquiredAt != before.AcquiredAt || after.Route != 1 || after.SourceRoute != 0 || !after.ManualRoute || after.Version == before.Version {
		t.Fatal("ticket/snapshot changed incorrectly")
	}
	if s.Invalidate(before, now) {
		t.Fatal("old request invalidated newly selected route")
	}
	if err := s.SwitchRoute(a.Fingerprint, before.Version, 2, now, rid, nil); err == nil {
		t.Fatal("stale panel accepted")
	}
	if err := s.SwitchRoute(b.Fingerprint, after.Version, 1, now, rid, nil); err == nil {
		t.Fatal("standby switch accepted")
	}
	restored := New(p)
	reordered := map[string]int{"source": 2, "chosen": 0, "backup": 1}
	restored.Restore(saved, now, func(id string) (int, bool) { i, ok := reordered[id]; return i, ok })
	got, _ := restored.Acquire(now)
	if got.Route != 0 || got.SourceRoute != 2 || !got.ManualRoute || got.Token != a || restored.Status(now).Standby != 1 {
		t.Fatal("restore lost route, source or standby")
	}
	delete(reordered, "source")
	restored.Restore(saved, now, func(id string) (int, bool) { i, ok := reordered[id]; return i, ok })
	got, ok := restored.Acquire(now)
	if !ok || got.Token != a || got.Route != 0 || got.SourceRouteID != "source" || got.SourceRoute != -1 {
		t.Fatal("removing old source discarded switched main")
	}
	if restored.Export(now, func(i int) string { return []string{"chosen", "backup"}[i] }).Active.SourceRouteID != "source" {
		t.Fatal("missing source metadata was lost on save")
	}
	// Disabling the old source must not disable the user's explicit new egress.
	s.SuspendRoutes(map[int]bool{0: true}, now)
	if got, ok := s.Acquire(now); !ok || got.Token != a || got.Route != 1 {
		t.Fatal("old source parked switched main")
	}
	if !s.Invalidate(after, now) {
		t.Fatal("current failing snapshot cannot invalidate")
	}
	if got, _ := s.Acquire(now); got.Token != b || got.Route != 2 || got.ManualRoute {
		t.Fatal("override leaked to successor")
	}
}
