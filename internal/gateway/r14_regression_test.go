package gateway

import (
	"context"
	"fmt"
	"github.com/gylive/ccodex-sleep-state/internal/turnstate"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestR14CapacityStreamPreservesBothCardsAndReportsFailure(t *testing.T) {
	e, _ := testEngine(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set(turnstate.Header, fakeToken(11, 213))
		w.Header().Set("Content-Type", "text/event-stream")
		fmt.Fprint(w, "data: {\"type\":\"response.failed\",\"response\":{\"error\":{\"code\":\"server_is_overloaded\"}}}\n\n")
	}))
	e.config.PoolEnabled = true
	e.running.Store(true)
	s, _ := e.borrow(request(generation, "r14-capacity").Header)
	defer release(s)
	for _, marker := range []byte{211, 212} {
		token, _ := turnstate.Parse(fakeToken(10, marker))
		s.state.Offer(token, 0, time.Now())
	}
	before, _ := s.state.Acquire(time.Now())
	for i := 0; i < 2; i++ {
		outcome := &RequestOutcome{}
		w := httptest.NewRecorder()
		e.ServeHTTP(w, WithOutcome(request(generation, "r14-capacity"), outcome))
		if w.Code != 200 || !strings.Contains(w.Body.String(), "server_is_overloaded") || outcome.Result != "failed" || outcome.ErrorCode != "model_capacity" {
			t.Fatalf("forwarded result missing: %+v", outcome)
		}
	}
	after, ok := s.state.Acquire(time.Now())
	if !ok || before != after || s.state.Status(time.Now()).Standby != 1 || s.lastStateCheck.Result != "upstream_error" {
		t.Fatal("capacity error consumed cards")
	}
}
func TestR14DisabledSourceParksMainAndUsesHealthyStandby(t *testing.T) {
	calls := 0
	e, _ := testEngine(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		if r.Header.Get(turnstate.Header) != fakeToken(10, 202) {
			t.Error("wrong bound card")
		}
		complete(w, fakeToken(10, 202))
	}))
	e.config.PoolEnabled = true
	other := e.routes[0]
	other.ID = "healthy"
	e.routes = append(e.routes, other)
	s, _ := e.borrow(request(generation, "r14-disabled").Header)
	defer release(s)
	for i, m := range []byte{201, 202} {
		token, _ := turnstate.Parse(fakeToken(10, m))
		s.state.Offer(token, i, time.Now())
	}
	e.pool.Change([]string{e.routes[0].ID}, "disabled", "manual", false)
	w := httptest.NewRecorder()
	e.ServeHTTP(w, request(generation, "r14-disabled"))
	active, ok := s.state.Acquire(time.Now())
	cards := e.cards(s, time.Now())
	if w.Code != 200 || calls != 1 || !ok || active.Route != 1 || len(cards) != 2 || cards[1].Role != "parked" {
		t.Fatal("disabled main stranded standby or lost held card")
	}
	e.pool.Change([]string{e.routes[0].ID}, "available", "manual", false)
	cards = e.cards(s, time.Now())
	if len(cards) != 2 || cards[1].Role != "standby" {
		t.Fatal("manual resume did not restore parked card")
	}
}
func TestR14FeatureFailureAdvancesNextIndependentRequest(t *testing.T) {
	healthy, failed := 0, 0
	e, _ := testEngine(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { healthy++; io.WriteString(w, `{"data":[]}`) }))
	other := e.routes[0]
	other.ID = "healthy-feature"
	e.routes = append(e.routes, other)
	e.routes[0].Transport = &http.Transport{DialContext: func(context.Context, string, string) (net.Conn, error) {
		failed++
		return nil, context.DeadlineExceeded
	}}
	for i, want := range []int{502, 200} {
		r := request("", "r14-feature")
		r.Method = "GET"
		r.URL.Path = "/backend-api/codex/models"
		w := httptest.NewRecorder()
		e.ServeHTTP(w, r)
		if w.Code != want {
			t.Fatalf("request %d=%d", i, w.Code)
		}
	}
	if failed != 1 || healthy != 1 || e.pool.Get(e.routes[0].ID).State != "failed" {
		t.Fatal("feature fault not isolated")
	}
}
func TestR14RandomModeCompactAllowsOnlyHealthySource(t *testing.T) {
	calls := 0
	e, _ := testEngine(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		if r.Header.Get(turnstate.Header) != "" {
			t.Error("compact injected card")
		}
		complete(w, "")
	}))
	e.config.PoolEnabled = true
	e.config.EgressMode = "random"
	s, _ := e.borrow(request(generation, "r14-compact").Header)
	defer release(s)
	token, _ := turnstate.Parse(fakeToken(10, 221))
	s.state.Offer(token, 0, time.Now())
	outcome := &RequestOutcome{}
	w := httptest.NewRecorder()
	e.ServeHTTP(w, WithOutcome(request(`{"model":"gpt-6-astra","input":[{"type":"compaction_trigger"}],"stream":true}`, "r14-compact"), outcome))
	if w.Code != 200 || calls != 1 || outcome.Kind != "compact" || outcome.Result != "completed" {
		t.Fatalf("compact locally blocked: %+v", outcome)
	}
}
func TestR14FullPoolStopsAutomaticAndManualRequests(t *testing.T) {
	calls := 0
	e, _ := testEngine(t, http.HandlerFunc(func(http.ResponseWriter, *http.Request) { calls++ }))
	e.config.PoolEnabled = true
	e.config.Collection.StandbyTarget = 2
	s, _ := e.borrow(request(generation, "r14-full").Header)
	defer release(s)
	s.activated = true
	for _, m := range []byte{41, 42, 43} {
		token, _ := turnstate.Parse(fakeToken(10, m))
		s.state.Offer(token, 0, time.Now())
	}
	now := time.Now().Add(45 * time.Minute)
	if at, reason := e.collectionAt(s, now); !at.IsZero() || reason != "pool_ready" {
		t.Fatal("full pool scheduled early replacement")
	}
	e.refresh(context.Background(), s, false)
	if err := e.RetryRandomState(context.Background(), s.id); err == nil {
		t.Fatal("manual action allowed full pool")
	}
	if calls != 0 || e.collection.Status(s.backupKey, time.Now(), e.config.Collection).HourlyUsed != 0 {
		t.Fatal("full pool consumed requests or budget")
	}
	active, _ := s.state.Acquire(time.Now())
	s.state.Invalidate(active, time.Now())
	if at, _ := e.collectionAt(s, time.Now().Add(20*time.Minute)); at.IsZero() {
		t.Fatal("vacancy did not resume collection")
	}
}

func TestR14OrdinaryStreamRateLimitPausesAccountWithoutRemovingCards(t *testing.T) {
	calls := 0
	e, _ := testEngine(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		w.Header().Set("Content-Type", "text/event-stream")
		w.Header().Set(turnstate.Header, fakeToken(11, 88))
		io.WriteString(w, "data: {\"type\":\"response.failed\",\"error\":{\"code\":\"rate_limit_exceeded\"}}\n\n")
	}))
	s, _ := e.borrow(request(generation, "ordinary-rate-limit").Header)
	defer release(s)
	token, _ := turnstate.Parse(fakeToken(10, 89))
	s.state.Offer(token, 0, time.Now())
	first := httptest.NewRecorder()
	e.ServeHTTP(first, request(generation, "ordinary-rate-limit"))
	second := httptest.NewRecorder()
	e.ServeHTTP(second, request(generation, "ordinary-rate-limit"))
	if first.Code != 200 || second.Code != 429 || calls != 1 {
		t.Fatal("ordinary rate-limit not respected")
	}
	if held, ok := s.state.Acquire(time.Now()); !ok || held.Token.Value != token.Value {
		t.Fatal("rate limit removed card")
	}
}
