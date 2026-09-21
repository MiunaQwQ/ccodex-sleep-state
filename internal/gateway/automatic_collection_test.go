package gateway

import (
	"context"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gylive/ccodex-sleep-state/internal/settings"
	"github.com/gylive/ccodex-sleep-state/internal/turnstate"
)

func TestAutomaticCollectionDisabledStopsSchedulerAndKeepsManualTickets(t *testing.T) {
	var calls atomic.Int32
	e, _ := testEngine(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		complete(w, fakeToken(10, byte(20+calls.Load())))
	}))
	e.config.Collection = settings.DefaultCollection()
	e.config.Collection.AutomaticDisabled = true
	e.config.PoolEnabled = true
	e.config.Collection.StandbyTarget = 1
	e.routes = append(e.routes, e.routes[0])
	e.routes[1].ID = "manual-alternative"

	s, _ := e.borrow(request(generation, "automatic-disabled").Header)
	defer release(s)
	s.activated = true
	s.lastAI, s.lastUsed = time.Now(), time.Now()
	original, err := turnstate.Parse(fakeToken(10, 1))
	if err != nil || !s.state.Offer(original, 0, time.Now()) {
		t.Fatal("failed to seed the existing main ticket")
	}
	activeBefore, usable := s.state.Acquire(time.Now())
	if !usable {
		t.Fatal("seeded ticket is not usable")
	}

	if work := e.backgroundWork(time.Now()); len(work) != 0 {
		t.Fatalf("disabled scheduler queued %d sessions", len(work))
	}
	e.refreshAutomatic(context.Background(), s, false)
	if calls.Load() != 0 {
		t.Fatalf("disabled scheduler dispatched %d probes", calls.Load())
	}
	if status := e.collectionStatus(s, time.Now()); status.Reason != "automatic_disabled" || status.WaitSeconds != 0 {
		t.Fatalf("unexpected disabled status: %+v", status)
	}

	if err := e.RetryRandomState(context.Background(), s.id); err != nil {
		t.Fatalf("manual ticket was blocked by automatic switch: %v", err)
	}
	if calls.Load() != 1 {
		t.Fatalf("manual action dispatched %d probes, want 1", calls.Load())
	}
	activeAfter, usable := s.state.Acquire(time.Now())
	if !usable || activeAfter.Token.Fingerprint != activeBefore.Token.Fingerprint {
		t.Fatal("manual ticket changed the existing main ticket")
	}
	if got := len(e.cards(s, time.Now())); got != 2 {
		t.Fatalf("manual ticket did not add one standby while disabled: got %d cards", got)
	}
	if status := e.collectionStatus(s, time.Now()); status.Reason != "automatic_disabled" {
		t.Fatalf("manual action unexpectedly re-enabled automatic status: %+v", status)
	}
}

func TestAutomaticCollectionDisabledSkipsRequestBootstrapProbe(t *testing.T) {
	var requests atomic.Int32
	e, _ := testEngine(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests.Add(1)
		complete(w, fakeToken(10, byte(40+requests.Load())))
	}))
	e.config.Collection.AutomaticDisabled = true
	e.config.StateFallback = "passthrough"
	w := httptest.NewRecorder()
	e.ServeHTTP(w, request(generation, "bootstrap-disabled"))
	if w.Code != http.StatusOK || requests.Load() != 1 {
		t.Fatalf("disabled request bootstrap: status=%d requests=%d", w.Code, requests.Load())
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	for _, s := range e.sessions {
		if len(e.cards(s, time.Now())) != 0 {
			t.Fatal("disabled fallback response was silently added to the ticket pool")
		}
	}
}
