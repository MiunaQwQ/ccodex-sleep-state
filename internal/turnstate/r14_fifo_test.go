package turnstate

import (
	"testing"
	"time"
)

func TestR14RestoredFIFOUsesAcquisitionNotIssueOrder(t *testing.T) {
	now := time.Now().Truncate(time.Second)
	s := New(Policy{10, time.Hour, 20 * time.Minute})
	s.SetStandbyLimit(2)
	a := PersistedSnapshot{Token: synthetic(10, now.Add(-30*time.Minute), 1), RouteID: "a", AcquiredAt: now.Add(-29 * time.Minute)}
	b := PersistedSnapshot{Token: synthetic(10, now.Add(-10*time.Minute), 2), RouteID: "b", AcquiredAt: now.Add(-9 * time.Minute)}
	c := PersistedSnapshot{Token: synthetic(10, now.Add(-12*time.Minute), 3), RouteID: "c", AcquiredAt: now.Add(-5 * time.Minute)}
	s.Restore(Persisted{Active: &a, Standby: []PersistedSnapshot{c, b}}, now, func(id string) (int, bool) { return int(id[0] - 'a'), true })
	cards := s.Cards(now, func(i int) (string, string) { return "", "" })
	if len(cards) != 3 || cards[1].AcquiredAt != b.AcquiredAt || cards[2].AcquiredAt != c.AcquiredAt {
		t.Fatal("cards out of acquisition order")
	}
	d, _ := Parse(synthetic(10, now, 4))
	if s.Offer(d, 3, now) {
		t.Fatal("full pool accepted fourth card")
	}
	for _, want := range []int{0, 1, 2} {
		a, ok := s.Acquire(now)
		if !ok || a.Route != want {
			t.Fatalf("wrong FIFO route %d", a.Route)
		}
		s.Invalidate(a, now)
	}
}
func TestR14SingleParkedMainCanResumeWithZeroStandby(t *testing.T) {
	now := time.Now()
	s := New(Policy{10, time.Hour, 20 * time.Minute})
	s.SetStandbyLimit(0)
	token, _ := Parse(synthetic(10, now, 6))
	s.Offer(token, 0, now)
	s.SuspendRoutes(map[int]bool{0: true}, now)
	if _, ok := s.Acquire(now); ok {
		t.Fatal("disabled source used")
	}
	s.SuspendRoutes(map[int]bool{}, now)
	if a, ok := s.Acquire(now); !ok || a.Token.Value != token.Value {
		t.Fatal("parked main lost")
	}
}
