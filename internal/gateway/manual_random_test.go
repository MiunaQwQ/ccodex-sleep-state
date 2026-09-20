package gateway

import (
	"context"
	"encoding/json"
	"net/http"
	"path/filepath"
	"testing"
	"time"

	"github.com/gylive/ccodex-sleep-state/internal/settings"
	"github.com/gylive/ccodex-sleep-state/internal/turnstate"
)

func TestManualTicketIgnoresEveryTimedGateWithoutChangingAutomaticBudget(t *testing.T) {
	for _, cadence := range []string{"round", "backoff"} {
		t.Run(cadence, func(t *testing.T) {
			calls := 0
			e, _ := testEngine(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				calls++
				complete(w, fakeToken(12, byte(170+calls))) // Rejected shape keeps a vacancy.
			}))
			e.config.Collection = settings.DefaultCollection()
			e.config.Collection.Cadence = cadence
			e.config.Collection.HourlyBudget = 1
			e.config.PoolEnabled = true
			other := e.routes[0]
			other.ID = "manual-alternative"
			e.routes = append(e.routes, other)
			e.config.PinnedRoute = e.routes[0].ID
			s, _ := e.borrow(request(generation, "all-manual-timers").Header)
			defer release(s)
			s.activated = true
			for _, marker := range []byte{168, 169} {
				token, _ := turnstate.Parse(fakeToken(10, marker))
				s.state.Offer(token, 0, time.Now())
			}
			main, _ := s.state.Acquire(time.Now())
			until := time.Now().Add(time.Hour)
			s.nextProbe, s.upstreamPause = until, until
			s.nodePauses = map[int]time.Time{1: until}
			e.reject(s, 429, time.Hour, 0)
			accountUntil := s.limit.retryUntil
			s.lastAI = time.Now().Add(-time.Hour)
			e.collection, _ = OpenCollectionBudget(filepath.Join(t.TempDir(), "budget.json"))
			e.collection.doc.History = []time.Time{time.Now()}
			e.collection.doc.Searches[s.backupKey] = collectionSearch{Until: until, Reason: "search_budget", Failures: 99, LastAttemptAt: time.Now(), LastFailureAt: time.Now()}
			e.collection.write()
			before, _ := json.Marshal(e.collection.doc)
			e.refresh(context.Background(), s, false)
			if calls != 0 {
				t.Fatal("automatic collection ignored its pause")
			}
			for i := 1; i <= 2; i++ {
				// The result is an explicit shape failure, not a timed rejection.
				if err := e.RetryRandomState(context.Background(), s.id); err == nil || s.diagnostic != "shape_mismatch" || calls != i {
					t.Fatalf("manual attempt %d did not dispatch once: %v, calls=%d", i, err, calls)
				}
			}
			after, _ := json.Marshal(e.collection.doc)
			if string(before) != string(after) || !s.nextProbe.Equal(until) || !s.upstreamPause.Equal(until) || !s.nodePauses[1].Equal(until) || !s.limit.retryUntil.Equal(accountUntil) {
				t.Fatal("manual ticket changed automatic timers or budget")
			}
			restored, err := OpenCollectionBudget(e.collection.path)
			if err != nil || restored.Status(s.backupKey, time.Now(), e.config.Collection).HourlyUsed != 1 {
				t.Fatal("automatic budget was not preserved across reload")
			}
			active, _ := s.state.Acquire(time.Now())
			if active != main || s.state.Status(time.Now()).Standby != 1 || e.config.PinnedRoute != e.routes[0].ID {
				t.Fatal("manual ticket changed existing cards, binding or pin")
			}
			e.refresh(context.Background(), s, false)
			if calls != 2 {
				t.Fatal("manual click started an automatic retry")
			}
		})
	}
}

func TestManualTicketUpstreamRefusalIsOneAttemptAndKeepsAutomaticPause(t *testing.T) {
	calls := 0
	e, _ := testEngine(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		w.Header().Set("Retry-After", "3600")
		w.WriteHeader(429)
	}))
	s, _ := e.borrow(request(generation, "manual-upstream-pause").Header)
	defer release(s)
	s.activated = true
	for i := 1; i <= 2; i++ {
		if err := e.RetryRandomState(context.Background(), s.id); err == nil || calls != i {
			t.Fatal("manual refusal retried automatically or next click was delayed")
		}
	}
	e.refresh(context.Background(), s, false)
	if calls != 2 || time.Until(s.upstreamPause) < 59*time.Minute {
		t.Fatal("automatic upstream pause was erased")
	}
}

func TestManualTicketStillRejectsAuthenticationAndBusyWork(t *testing.T) {
	for _, status := range []int{401, 403, 0} {
		calls := 0
		e, _ := testEngine(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { calls++ }))
		s, _ := e.borrow(request(generation, "manual-nontimed-gates").Header)
		s.activated = true
		if status == 0 {
			s.queued = true
		} else {
			e.reject(s, status, 0, 0)
		}
		if err := e.RetryRandomState(context.Background(), s.id); err == nil || calls != 0 {
			t.Fatal("auth/busy gate was bypassed")
		}
		release(s)
	}
}
