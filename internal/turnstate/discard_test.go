package turnstate

import (
	"errors"
	"path/filepath"
	"reflect"
	"testing"
	"time"
)

func TestDiscardSpecificCardsPreservesFIFOAndLateRequest(t *testing.T) {
	now := time.Now().Truncate(time.Second)
	s := New(Policy{10, time.Hour, 20 * time.Minute})
	tokens := make([]Token, 3)
	for i := range tokens {
		tokens[i], _ = Parse(synthetic(10, now, byte(i+40)))
		s.Offer(tokens[i], i, now.Add(time.Duration(i)*time.Second))
	}
	old, _ := s.Acquire(now)
	route := func(i int) string { return []string{"a", "b", "c"}[i] }
	if role, err := s.Discard(tokens[1].Fingerprint, now, route, nil); err != nil || role != "standby" {
		t.Fatal(role, err)
	}
	if active, _ := s.Acquire(now); active != old || s.Status(now).Standby != 1 {
		t.Fatal("standby deletion changed main")
	}
	if role, err := s.Discard(old.Token.Fingerprint, now, route, nil); err != nil || role != "active" {
		t.Fatal(role, err)
	}
	next, _ := s.Acquire(now)
	if next.Token != tokens[2] || next.Route != 2 || !next.AcquiredAt.Equal(now.Add(2*time.Second)) {
		t.Fatal("wrong FIFO successor or source")
	}
	if s.Invalidate(old, now) {
		t.Fatal("late response invalidated successor")
	}
	if _, err := s.Discard(old.Token.Fingerprint, now, route, nil); !errors.Is(err, ErrCardNotFound) {
		t.Fatal("stale click accepted")
	}
	if s.Offer(old.Token, 0, now) {
		t.Fatal("late probe resurrected discarded ticket")
	}
	if active, _ := s.Acquire(now); active != next {
		t.Fatal("stale click changed successor")
	}
}

func TestDiscardParkedAndPersistenceFailure(t *testing.T) {
	now := time.Now().Truncate(time.Second)
	s := New(Policy{10, time.Hour, 20 * time.Minute})
	a, _ := Parse(synthetic(10, now, 51))
	b, _ := Parse(synthetic(10, now, 52))
	s.Offer(a, 0, now)
	s.Offer(b, 1, now)
	s.SuspendRoutes(map[int]bool{0: true}, now)
	route := func(i int) string { return []string{"a", "b"}[i] }
	before := s.Export(now, route)
	events := s.Events()
	if _, err := s.Discard(a.Fingerprint, now, route, func(Persisted) error { return errors.New("disk unavailable") }); err == nil {
		t.Fatal("save failure hidden")
	}
	if !reflect.DeepEqual(before, s.Export(now, route)) || !reflect.DeepEqual(events, s.Events()) {
		t.Fatal("failed transaction changed cards or events")
	}
	if role, err := s.Discard(a.Fingerprint, now, route, nil); err != nil || role != "parked" {
		t.Fatal(role, err)
	}
	s.SuspendRoutes(nil, now)
	if active, _ := s.Acquire(now); active.Token != b || s.Status(now).Standby != 0 {
		t.Fatal("restoring source revived parked ticket")
	}
}

func TestDiscardLastCardSurvivesReloadAndRejectsOldResponse(t *testing.T) {
	now := time.Now().Truncate(time.Second)
	s := New(Policy{10, 5 * time.Minute, time.Minute})
	a, _ := Parse(synthetic(10, now, 53))
	s.Offer(a, 0, now)
	path := filepath.Join(t.TempDir(), "state-backup.json")
	backup, err := OpenBackup(path)
	if err != nil {
		t.Fatal(err)
	}
	route := func(int) string { return "source" }
	if _, err = s.Discard(a.Fingerprint, now, route, func(p Persisted) error { return backup.Save("session", p) }); err != nil {
		t.Fatal(err)
	}
	reloaded, err := OpenBackup(path)
	if err != nil {
		t.Fatal(err)
	}
	saved, ok := reloaded.Load("session")
	if !ok || saved.Active != nil || len(saved.Discarded) != 1 {
		t.Fatal("last-card deletion was not persisted")
	}
	restored := New(Policy{10, time.Hour, 20 * time.Minute})
	restored.Restore(saved, now, func(string) (int, bool) { return 0, true })
	if restored.Offer(a, 0, now.Add(6*time.Minute)) || restored.Bootstrap(a, 0, now) {
		t.Fatal("reload or longer TTL resurrected discarded ticket")
	}
	saved.Active = &PersistedSnapshot{Token: a.Value, RouteID: "source", Issued: a.Issued}
	restored.Restore(saved, now, func(string) (int, bool) { return 0, true })
	if restored.Status(now).Usable {
		t.Fatal("discard metadata did not win over old backup card")
	}
	if len(restored.Export(now.Add(time.Hour), route).Discarded) != 0 {
		t.Fatal("expired deletion markers not pruned")
	}
}
