package gateway

import (
	"context"
	"fmt"
	"net/http"
	"path/filepath"
	"testing"
	"time"

	"github.com/gylive/ccodex-sleep-state/internal/settings"
)

func TestRoundsBoundRequestsAndPauseEvenWithoutActiveState(t *testing.T) {
	for _, tc := range []struct {
		name                                       string
		acceptedAt, rejectedAt, hourlyBudget, want int
	}{
		{"six failures", 0, 0, 30, 6},
		{"success ends bootstrap", 2, 0, 30, 2},
		{"429 stops rotation", 0, 2, 30, 2},
		{"hourly budget stops inside round", 0, 0, 3, 3},
	} {
		t.Run(tc.name, func(t *testing.T) {
			calls := 0
			e, _ := testEngine(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				calls++
				if calls == tc.rejectedAt {
					w.Header().Set("Retry-After", "300")
					w.WriteHeader(429)
					return
				}
				blocks := 11
				if calls == tc.acceptedAt {
					blocks = 10
				}
				complete(w, fakeToken(blocks, byte(calls)))
			}))
			e.config.Collection = settings.DefaultCollection()
			e.config.Collection.HourlyBudget = tc.hourlyBudget
			for i := 1; i < 8; i++ {
				r := e.routes[0]
				r.ID = fmt.Sprintf("node-%d", i)
				e.routes = append(e.routes, r)
			}
			s, _ := e.borrow(request(generation, "round-test").Header)
			defer release(s)
			before := time.Now()
			e.refresh(context.Background(), s, true)
			if calls != tc.want {
				t.Fatalf("requests=%d want=%d", calls, tc.want)
			}
			entry := e.collection.doc.Searches[s.backupKey]
			if entry.Reason != "round_cooldown" || entry.Until.Before(before.Add(180*time.Second)) || entry.Until.After(time.Now().Add(180*time.Second)) {
				t.Fatalf("missing completion pause: %+v", entry)
			}
			// Simulate the stale in-memory timestamp from an earlier batch.
			s.nextProbe = entry.Until
			e.refresh(context.Background(), s, true)
			want := tc.want
			if calls != want {
				t.Fatalf("next batch requests=%d want=%d", calls, want)
			}
			if tc.rejectedAt > 0 && time.Until(s.upstreamPause) < 299*time.Second {
				t.Fatal("Retry-After shortened")
			}
		})
	}
}

func TestRoundPauseAndHourlyBudgetSurviveRestart(t *testing.T) {
	p := settings.DefaultCollection()
	p.HourlyBudget = 6
	path := filepath.Join(t.TempDir(), "budget.json")
	b, err := OpenCollectionBudget(path)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Date(2026, 9, 19, 12, 0, 0, 0, time.UTC)
	for i := 0; i < 6; i++ {
		if until, why := b.ReserveRound("sol", now, p, i > 0, 180*time.Second); !until.IsZero() {
			t.Fatalf("round stopped early: %s", why)
		}
		b.Complete("sol", now, false, p, "shape_mismatch", "node")
	}
	b.EndRound("sol", now.Add(20*time.Second), 180*time.Second)
	b, err = OpenCollectionBudget(path)
	if err != nil {
		t.Fatal(err)
	}
	if got := b.doc.Searches["sol"]; got.Failures != 6 || !got.Until.Equal(now.Add(200*time.Second)) {
		t.Fatal("restart lost round record")
	}
	if until, why := b.ReserveRound("sol", now.Add(200*time.Second), p, false, 180*time.Second); until.IsZero() || why != "hourly_budget" {
		t.Fatal("hourly budget bypassed")
	}
	if until, _ := b.ReserveRound("sol", now.Add(time.Hour), p, false, 180*time.Second); !until.IsZero() {
		t.Fatal("round did not resume")
	}
}

func TestUnfinishedRoundAlsoPausesAfterRestart(t *testing.T) {
	p := settings.DefaultCollection()
	path := filepath.Join(t.TempDir(), "budget.json")
	b, _ := OpenCollectionBudget(path)
	now := time.Now()
	b.ReserveRound("sol", now, p, false, 180*time.Second)
	b, _ = OpenCollectionBudget(path)
	if until, why := b.ReserveRound("sol", now.Add(time.Second), p, false, 180*time.Second); !until.Equal(now.Add(180*time.Second)) || why != "round_cooldown" {
		t.Fatal("unfinished probe bypassed pause")
	}
}
