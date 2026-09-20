package gateway

import (
	"context"
	"github.com/gylive/ccodex-sleep-state/internal/turnstate"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestSwitchMainWhileInflightUsesExactNodeAndProtectsNewVersion(t *testing.T) {
	entered, finish := make(chan struct{}), make(chan struct{})
	var once sync.Once
	releaseReq := func() { once.Do(func() { close(finish) }) }
	defer releaseReq()
	e, _ := testEngine(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { close(entered); <-finish; complete(w, fakeToken(11, 43)) }))
	second := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get(turnstate.Header) != fakeToken(10, 41) {
			t.Error("main ticket changed")
		}
		complete(w, fakeToken(10, 41))
	}))
	defer second.Close()
	route := e.routes[0]
	route.ID = "other"
	route.DisplayName = "Other node"
	route.Transport = &http.Transport{DialContext: func(ctx context.Context, network, address string) (net.Conn, error) {
		return (&net.Dialer{}).DialContext(ctx, network, strings.TrimPrefix(second.URL, "http://"))
	}}
	e.routes = append(e.routes, route)
	s, _ := e.borrow(request(generation, "switch-main").Header)
	defer release(s)
	other, _ := e.borrow(request(generation, "other-account").Header)
	defer release(other)
	a := mustSwitchToken(t, 41)
	b := mustSwitchToken(t, 42)
	for _, v := range []*session{s, other} {
		v.state.Offer(a, 0, time.Now())
		v.state.Offer(b, 0, time.Now())
	}
	before, _ := s.state.Acquire(time.Now())
	done := make(chan struct{})
	go func() { defer close(done); e.ServeHTTP(httptest.NewRecorder(), request(generation, "switch-main")) }()
	<-entered
	e.failRoute(0)
	if err := e.SwitchStateRoute(s.id, a.Fingerprint, before.Version, "other"); err != nil {
		t.Fatal(err)
	}
	releaseReq()
	<-done
	if got, ok := s.state.Acquire(time.Now()); !ok || got.Token != a || got.Route != 1 {
		t.Fatal("late failure destroyed switched main")
	}
	for _, body := range []string{generation, `{"model":"gpt-6-astra","input":[{"type":"compaction_trigger"}]}`} {
		outcome := &RequestOutcome{}
		w := httptest.NewRecorder()
		e.ServeHTTP(w, WithOutcome(request(body, "switch-main"), outcome))
		if w.Code != 200 || outcome.RouteID != "other" || !outcome.StateInjected || outcome.Result != "completed" {
			t.Fatal("wrong route/response", w.Code, outcome)
		}
	}
	if got, _ := other.state.Acquire(time.Now()); got.Route != 0 || got.ManualRoute {
		t.Fatal("switch crossed account")
	}
	after, _ := s.state.Acquire(time.Now())
	e.pool.Change([]string{"test-route"}, "disabled", "manual", false)
	if err := e.SwitchStateRoute(s.id, a.Fingerprint, after.Version, "test-route"); err == nil {
		t.Fatal("disabled destination accepted")
	}
}

func mustSwitchToken(t *testing.T, marker byte) turnstate.Token {
	t.Helper()
	v, err := turnstate.Parse(fakeToken(10, marker))
	if err != nil {
		t.Fatal(err)
	}
	return v
}
