package gateway

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gylive/ccodex-sleep-state/internal/settings"
	"github.com/gylive/ccodex-sleep-state/internal/turnstate"
)

func TestSameExitProbe11PreservesMainAndStandby(t *testing.T) {
	e, _ := testEngine(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { complete(w, fakeToken(11, 223)) }))
	other := e.routes[0]
	other.ID = "other"
	e.routes = append(e.routes, other)
	s, _ := e.borrow(request(generation, "same-exit-probe").Header)
	defer release(s)
	for i := 0; i < 2; i++ {
		token, _ := turnstate.Parse(fakeToken(10, byte(221+i)))
		s.state.Offer(token, 0, time.Now())
	}
	before, _ := s.state.Acquire(time.Now())
	e.refresh(context.Background(), s, false, 0)
	after, ok := s.state.Acquire(time.Now())
	if !ok || before != after || s.state.Status(time.Now()).Standby != 1 {
		t.Fatal("fresh probe 11 removed existing main or standby")
	}
	if !time.Now().Before(s.nodePauses[0]) {
		t.Fatal("probe exit not paused for new collection")
	}
}

func TestTwoInFlight11ResponsesCannotDeletePromotedSameExitStandby(t *testing.T) {
	arrived := make(chan struct{}, 2)
	finish := make(chan struct{}, 2)
	e, _ := testEngine(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		arrived <- struct{}{}
		<-finish
		complete(w, fakeToken(11, 227))
	}))
	other := e.routes[0]
	other.ID = "other"
	e.routes = append(e.routes, other)
	e.log = slog.New(slog.NewTextHandler(io.Discard, nil))
	e.running.Store(true)
	s, _ := e.borrow(request(generation, "late-response-state").Header)
	a, _ := turnstate.Parse(fakeToken(10, 225))
	b, _ := turnstate.Parse(fakeToken(10, 226))
	s.state.Offer(a, 0, time.Now())
	s.state.Offer(b, 0, time.Now())
	release(s)
	done := make(chan int, 2)
	for i := 0; i < 2; i++ {
		go func() {
			w := httptest.NewRecorder()
			e.ServeHTTP(w, request(generation, "late-response-state"))
			done <- w.Code
		}()
	}
	for i := 0; i < 2; i++ {
		<-arrived
	}
	for i := 0; i < 2; i++ {
		finish <- struct{}{}
		if <-done != 200 {
			t.Fatal("lost original answer")
		}
		current, ok := s.state.Acquire(time.Now())
		if !ok || current.Token.Fingerprint != b.Fingerprint {
			t.Fatal("old response deleted same-exit standby")
		}
	}
}

func TestMetadataDoesNotKeepCollectorAwakeAndAIResumesIt(t *testing.T) {
	e, _ := testEngine(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { io.WriteString(w, "ok") }))
	e.log = slog.New(slog.NewTextHandler(io.Discard, nil))
	e.running.Store(true)
	e.config.StateFallback = "passthrough"
	s, _ := e.borrow(request(generation, "idle-collector-account").Header)
	s.activated = true
	s.lastAI = time.Now().Add(-301 * time.Second)
	release(s)
	r := request("", "idle-collector-account")
	r.Method = http.MethodGet
	r.URL.Path = "/backend-api/codex/models"
	e.ServeHTTP(httptest.NewRecorder(), r)
	if work := e.backgroundWork(time.Now()); len(work) != 0 {
		for _, v := range work {
			releaseWork(v)
		}
		t.Fatal("models request extended AI activity")
	}
	s.mu.Lock()
	status := e.collectionStatus(s, time.Now())
	s.mu.Unlock()
	if !status.Idle {
		t.Fatal("idle UI did not match scheduler")
	}
	e.ServeHTTP(httptest.NewRecorder(), request(generation, "idle-collector-account"))
	work := e.backgroundWork(time.Now())
	defer func() {
		for _, v := range work {
			releaseWork(v)
		}
	}()
	if len(work) != 1 {
		t.Fatal("new AI request did not resume collection")
	}
}

func TestRoundStopsAtIdleBoundaryBeforeNextNode(t *testing.T) {
	var s *session
	var calls int
	e, _ := testEngine(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		s.mu.Lock()
		s.lastAI = time.Now().Add(-301 * time.Second)
		s.mu.Unlock()
		complete(w, fakeToken(11, 230))
	}))
	e.config.Collection = settings.DefaultCollection()
	other := e.routes[0]
	other.ID = "other"
	e.routes = append(e.routes, other)
	s, _ = e.borrow(request(generation, "idle-round-account").Header)
	defer release(s)
	e.refresh(context.Background(), s, false)
	if calls != 1 {
		t.Fatalf("idle round kept dispatching: %d", calls)
	}
}

func TestTimeoutKeepsFixedNodeAndDoesNotSwitchToAnotherRoute(t *testing.T) {
	e, _ := testEngine(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { complete(w, fakeToken(10, 231)) }))
	e.config.PoolEnabled = true
	e.config.Collection = settings.DefaultCollection()
	e.config.Collection.StandbyTarget = 0
	other := e.routes[0]
	other.ID = "healthy"
	e.routes = append(e.routes, other)
	var brokenCalls atomic.Int32
	e.routes[0].Transport = &http.Transport{DialContext: func(context.Context, string, string) (net.Conn, error) {
		brokenCalls.Add(1)
		return nil, context.DeadlineExceeded
	}}
	e.config.PinnedRoute = e.routes[0].ID
	s, _ := e.borrow(request(generation, "dead-exit-account").Header)
	defer release(s)
	e.refresh(context.Background(), s, false)
	active, ok := s.state.Acquire(time.Now())
	if ok || active.Token.Value != "" || brokenCalls.Load() != 1 {
		t.Fatal("failed fixed node switched to a healthy exit")
	}
	if e.pool.Get(e.routes[0].ID).State != "available" || e.effectivePinnedRoute() != e.routes[0].ID {
		t.Fatal("fixed node was disabled or unpinned after a transient failure")
	}
	// Clear only the local pacing record to model a later retry after the
	// external proxy machine has had time to change its own upstream node.
	e.collection.doc.Searches[s.backupKey] = collectionSearch{}
	s.nextProbe = time.Time{}
	s.cursor = 0
	e.refresh(context.Background(), s, false)
	if brokenCalls.Load() != 2 {
		t.Fatal("fixed node was not retried in place")
	}
}

func TestFixedRouteRequestFailureDoesNotFallBackToHealthyLocalRoute(t *testing.T) {
	var upstreamCalls atomic.Int32
	e, _ := testEngine(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		upstreamCalls.Add(1)
		complete(w, fakeToken(10, 240))
	}))
	e.config.PoolEnabled = true
	e.config.PinnedRoute = e.routes[0].ID
	e.config.Collection.StandbyTarget = 0
	other := e.routes[0]
	other.ID = "healthy-local-route"
	e.routes = append(e.routes, other)
	e.routes[0].Transport = &http.Transport{DialContext: func(context.Context, string, string) (net.Conn, error) {
		return nil, context.DeadlineExceeded
	}}
	s, _ := e.borrow(request(generation, "fixed-request-failure").Header)
	defer release(s)
	token, _ := turnstate.Parse(fakeToken(10, 239))
	if !s.state.Offer(token, 0, time.Now()) {
		t.Fatal("failed to seed fixed-route ticket")
	}
	w := httptest.NewRecorder()
	e.ServeHTTP(w, request(generation, "fixed-request-failure"))
	if w.Code != http.StatusBadGateway || upstreamCalls.Load() != 0 {
		t.Fatalf("fixed request fell back to another local route: status=%d upstream=%d", w.Code, upstreamCalls.Load())
	}
	if e.pool.Get(e.routes[0].ID).State != "available" || e.effectivePinnedRoute() != e.routes[0].ID {
		t.Fatal("fixed route was disabled or unpinned after request failure")
	}
	if active, ok := s.state.Acquire(time.Now()); !ok || active.Token.Fingerprint != token.Fingerprint {
		t.Fatal("fixed request failure removed the existing ticket")
	}
}

func TestCapacityFailureContinuesToSuccessfulNextNode(t *testing.T) {
	var calls int
	e, _ := testEngine(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		if calls == 1 {
			fmt.Fprint(w, "data: {\"type\":\"response.failed\",\"response\":{\"error\":{\"code\":\"server_is_overloaded\"}}}\n\n")
			return
		}
		complete(w, fakeToken(10, 232))
	}))
	e.config.Collection = settings.DefaultCollection()
	other := e.routes[0]
	other.ID = "other"
	e.routes = append(e.routes, other)
	s, _ := e.borrow(request(generation, "capacity-next-success").Header)
	setTestProbeOrder(e, s, 0, 1)
	defer release(s)
	e.refresh(context.Background(), s, false)
	active, ok := s.state.Acquire(time.Now())
	if !ok || active.Route != 1 || calls != 2 || !s.upstreamPause.IsZero() {
		t.Fatal("capacity ended round or blocked good candidate")
	}
}

func TestProbeStreamTimeoutClassifiedAsConnectionFailure(t *testing.T) {
	e, _ := testEngine(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		fmt.Fprint(w, "data: {\"type\":\"response.created\"}\n\n")
		w.(http.Flusher).Flush()
		<-r.Context().Done()
	}))
	e.config.ProbeSeconds = 1
	_, _, _, err := e.probe(context.Background(), request(generation, "stream-timeout").Header, e.routes[0])
	if probeReason(err) != "network_failed" {
		t.Fatalf("timeout was not classified as connection failure: %v", err)
	}
}
