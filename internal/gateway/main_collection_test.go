package gateway

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/gylive/ccodex-sleep-state/internal/settings"
)

func TestMissingMainNeverSkipsHardGatesOrBackoff(t *testing.T) {
	for _, gate := range []string{"401", "429", "hourly", "storage", "backoff", "idle", "node-paused"} {
		t.Run(gate, func(t *testing.T) {
			calls := 0
			e, _ := testEngine(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { calls++ }))
			e.config.Collection = settings.DefaultCollection()
			s, _ := e.borrow(request(generation, "guarded-main-state").Header)
			defer release(s)
			now := time.Now()
			e.collection.EndRound(s.backupKey, now, 180*time.Second)
			s.nextProbe = now.Add(180 * time.Second)
			switch gate {
			case "401":
				e.reject(s, 401, 0, 0)
			case "429":
				s.upstreamPause = now.Add(300 * time.Second)
			case "hourly":
				e.config.Collection.HourlyBudget = 1
				e.collection.doc.History = []time.Time{now}
			case "storage":
				e.collection.err = errors.New("synthetic storage failure")
			case "backoff":
				e.config.Collection.Cadence = "backoff"
			case "idle":
				s.lastAI = now.Add(-time.Hour)
			case "node-paused":
				s.nodePauses = map[int]time.Time{0: now.Add(time.Minute)}
			}
			e.refresh(context.Background(), s, false)
			if calls != 0 {
				t.Fatalf("missing active bypassed %s", gate)
			}
		})
	}
}

func TestOrdinaryReplyAcquiresMainThenWaitsWithoutSpendingBudget(t *testing.T) {
	calls := 0
	e, _ := testEngine(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		complete(w, fakeToken(10, 246))
	}))
	e.config.Collection = settings.DefaultCollection()
	e.config.PoolEnabled = true
	e.config.StateFallback = "passthrough"
	// Queue the collector without starting it: only the ordinary reply runs.
	e.running.Store(true)
	w := httptest.NewRecorder()
	e.ServeHTTP(w, request(generation, "ordinary-first-main"))
	e.running.Store(false)
	s, _ := e.borrow(request(generation, "ordinary-first-main").Header)
	defer release(s)
	if w.Code != 200 || calls != 1 || !s.state.Status(time.Now()).Usable {
		t.Fatal("ordinary reply did not acquire the main state")
	}
	status := e.collectionStatus(s, time.Now())
	if status.Reason != "round_cooldown" || status.WaitSeconds < 178 || status.HourlyUsed != 0 {
		t.Fatalf("ordinary acquisition changed budget or skipped standby wait: %+v", status)
	}
	e.refresh(context.Background(), s, false)
	if calls != 1 {
		t.Fatal("standby probe ran immediately after ordinary acquisition")
	}
}
