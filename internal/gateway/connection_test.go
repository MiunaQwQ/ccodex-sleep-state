package gateway

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gylive/ccodex-sleep-state/internal/proxyroute"
	"github.com/gylive/ccodex-sleep-state/internal/settings"
	"github.com/gylive/ccodex-sleep-state/internal/turnstate"
)

func TestConnectionRecoveryPreservesCardRouteAndDoesNotReplay(t *testing.T) {
	var calls atomic.Int32
	e, server := testEngine(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if calls.Add(1) <= 2 {
			c, _, _ := w.(http.Hijacker).Hijack()
			c.Close()
			return
		}
		w.Header().Set("Content-Type", "text/event-stream")
		io.WriteString(w, "data: {\"type\":\"response.completed\",\"response\":{\"model\":\"gpt-6-astra\"}}\n\n")
	}))
	_ = server
	cfg := settings.Default()
	cfg.Direct = true
	cfg.NodeNetworkMode = "system"
	cfg.Subscriptions = nil
	cfg.ProxyURLs = nil
	cfg.ProxyEnvs = nil
	routes, err := proxyroute.Load(context.Background(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	e.routes = routes
	e.config.PoolEnabled = true
	defer e.Close()
	s, _ := e.borrow(request(generation, "recover-card").Header)
	defer release(s)
	token, _ := turnstate.Parse(fakeToken(10, 210))
	s.state.Offer(token, 0, time.Now())
	old, _ := s.state.Acquire(time.Now())
	for i := int32(1); i <= 2; i++ {
		w := httptest.NewRecorder()
		e.ServeHTTP(w, request(generation, "recover-card"))
		if w.Code != 502 || calls.Load() != i {
			t.Fatal("failed request replayed", w.Code, calls.Load())
		}
	}
	if routes[0].ConnectionHealth().Recoveries != 1 {
		t.Fatal("connection not rebuilt")
	}
	w := httptest.NewRecorder()
	e.ServeHTTP(w, request(generation, "recover-card"))
	if w.Code != 200 || calls.Load() != 3 {
		t.Fatal(w.Code, calls.Load())
	}
	if current, _ := s.state.Acquire(time.Now()); current != old {
		t.Fatal("recovery replaced card or route")
	}
	if e.pool.Get(routes[0].ID).State != "failed" {
		t.Fatal("recovery bypassed manual node recovery policy")
	}
}
