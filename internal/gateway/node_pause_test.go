package gateway

import (
	"context"
	"fmt"
	"net/http"
	"testing"
	"time"

	"github.com/gylive/ccodex-sleep-state/internal/settings"
)

func TestConfirmed11PausesByModelAndRetainsLastThenRecovers(t *testing.T) {
	e, _ := testEngine(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { complete(w, fakeToken(11, 151)) }))
	e.config.Collection = settings.DefaultCollection()
	for i := 1; i < 3; i++ {
		r := e.routes[0]
		r.ID = fmt.Sprintf("node-%d", i)
		e.routes = append(e.routes, r)
	}
	s, _ := e.borrow(request(generation, "pause-test").Header)
	setTestProbeOrder(e, s, 0, 1, 2)
	defer release(s)
	e.refresh(context.Background(), s, true)
	now := time.Now()
	if len(s.nodePauses) != 2 || !now.Before(s.nodePauses[0]) || !now.Before(s.nodePauses[1]) || !s.retainedLast || s.retainedRoute != 2 || e.fallbackRouteFor(s, now) != 2 {
		t.Fatalf("incorrect paused/retained state: %+v", s.nodePauses)
	}
	if _, ok := s.state.Acquire(now); ok {
		t.Fatal("last 312 was accepted")
	}
	other, _ := e.borrow(request(generation, "pause-test").Header, "gpt-5.6-sol")
	defer release(other)
	if len(other.nodePauses) != 0 || e.fallbackRouteFor(other, now) != 0 {
		t.Fatal("pause leaked to another model")
	}
	s.mu.Lock()
	recovered := e.otherEligibleRouteLocked(s, 2, now.Add(181*time.Second))
	s.mu.Unlock()
	if recovered != 0 {
		t.Fatal("nodes did not automatically return to rotation")
	}
	before := e.collection.Status(s.backupKey, now, e.config.Collection)
	if err := e.ResumeNode(s.id, e.routes[0].ID); err != nil {
		t.Fatal(err)
	}
	after := e.collection.Status(s.backupKey, now, e.config.Collection)
	if _, ok := s.nodePauses[0]; ok || !before.NextAt.Equal(after.NextAt) || before.HourlyUsed != after.HourlyUsed {
		t.Fatal("manual resume failed or reset budget")
	}
	if err := e.ResumeNode("unknown", e.routes[0].ID); err == nil {
		t.Fatal("unknown session accepted")
	}
}

func TestOtherShapeDoesNotPauseOrRepeatCandidate(t *testing.T) {
	calls := 0
	e, _ := testEngine(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { calls++; complete(w, fakeToken(12, 152)) }))
	e.config.Collection = settings.DefaultCollection()
	r := e.routes[0]
	r.ID = "other"
	e.routes = append(e.routes, r)
	s, _ := e.borrow(request(generation, "other-shape").Header)
	defer release(s)
	e.refresh(context.Background(), s, true)
	if calls != 2 || len(s.nodePauses) != 0 {
		t.Fatal("another shape must not pause nodes or repeat a candidate")
	}
}

func TestForegroundDoesNotDeferBackgroundCollection(t *testing.T) {
	e, _ := testEngine(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {}))
	s, _ := e.borrow(request(generation, "foreground").Header)
	s.activated = true
	release(s)
	e.foreground.Add(1)
	work := e.backgroundWork(time.Now())
	if len(work) != 1 {
		t.Fatal("background collection was deferred during a reply")
	}
	releaseWork(work[0])
	e.foreground.Add(-1)
	work = e.backgroundWork(time.Now())
	if len(work) != 1 {
		t.Fatal("background did not remain eligible after the reply")
	}
	releaseWork(work[0])
	calls := 0
	other, _ := testEngine(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		e.foreground.Store(1)
		complete(w, fakeToken(11, 153))
	}))
	other.config.Collection = settings.DefaultCollection()
	r := other.routes[0]
	r.ID = "other"
	other.routes = append(other.routes, r)
	// The hook models a foreground arrival after the first probe dispatch.
	e = other
	x, _ := other.borrow(request(generation, "between-probes").Header)
	defer release(x)
	other.refresh(context.Background(), x, false)
	if calls != 2 {
		t.Fatal("foreground activity interrupted the probe round")
	}
}
