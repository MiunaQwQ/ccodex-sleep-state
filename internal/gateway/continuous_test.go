package gateway

import (
	"context"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"net/url"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gylive/ccodex-sleep-state/internal/proxyroute"
	"github.com/gylive/ccodex-sleep-state/internal/settings"
	"github.com/gylive/ccodex-sleep-state/internal/turnstate"
)

func await(t *testing.T, f func() bool) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if f() {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal("condition not reached within 3 seconds")
}
func runCollector(t *testing.T, e *Engine) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { defer close(done); e.Run(ctx) }()
	t.Cleanup(func() { cancel(); <-done })
	await(t, func() bool { return e.running.Load() })
}
func TestContinuousSearchCrossesBatchesAndStopsOnSuccess(t *testing.T) {
	var probes atomic.Int32
	e, _ := testEngine(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		count := probes.Add(1)
		if count <= 4 {
			complete(w, fakeToken(11, byte(count)))
		} else {
			complete(w, fakeToken(10, 9))
		}
	}))
	e.log = slog.New(slog.NewTextHandler(io.Discard, nil))
	e.config.MaxProbes = 1
	e.config.Collection.FailureIntervalSeconds = 0
	e.config.Collection.FailureMaxIntervalSeconds = 0
	e.config.CooldownSeconds = 180
	runCollector(t, e)
	s, err := e.borrow(request(generation, "continuous-account").Header)
	if err != nil {
		t.Fatal(err)
	}
	s.mu.Lock()
	s.activated = true
	s.mu.Unlock()
	release(s)
	e.signal()
	await(t, func() bool { _, ok := s.state.Acquire(time.Now()); return ok })
	// Wake-ups do not launch another probe while the successful state is fresh.
	for i := 0; i < 20; i++ {
		e.signal()
	}
	time.Sleep(50 * time.Millisecond)
	if probes.Load() != 5 {
		t.Fatalf("probes=%d; failed batches must continue, success must stop", probes.Load())
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if !time.Now().Before(s.nextProbe) {
		t.Fatal("successful state did not set cooldown")
	}
	if len(s.nodes) != 1 || s.nodes[e.routes[0].ID].Attempts != 5 {
		t.Fatal("missing per-node observations")
	}
}

func Test312BreaksSuccessCooldownWithoutLosingAnswer(t *testing.T) {
	var probes, answers atomic.Int32
	e, _ := testEngine(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		b, _ := io.ReadAll(r.Body)
		if string(b) == generation {
			answers.Add(1)
			complete(w, fakeToken(11, 7))
			return
		}
		probes.Add(1)
		complete(w, fakeToken(10, 8))
	}))
	e.log = slog.New(slog.NewTextHandler(io.Discard, nil))
	s, err := e.borrow(request(generation, "changed-shape-account").Header)
	if err != nil {
		t.Fatal(err)
	}
	token, _ := turnstate.Parse(fakeToken(10, 1))
	s.state.Offer(token, 0, time.Now())
	s.mu.Lock()
	s.nextProbe = time.Now().Add(time.Hour)
	s.mu.Unlock()
	release(s)
	runCollector(t, e)
	w := httptest.NewRecorder()
	e.ServeHTTP(w, request(generation, "changed-shape-account"))
	if w.Code != 200 || !strings.Contains(w.Body.String(), "response.completed") || answers.Load() != 1 {
		t.Fatal("answer was dropped or replayed")
	}
	await(t, func() bool {
		snapshot, ok := s.state.Acquire(time.Now())
		return ok && snapshot.Token.Value == fakeToken(10, 8)
	})
	if probes.Load() != 1 {
		t.Fatal("312 did not trigger exactly one successful replacement")
	}
}

func TestConcurrentColdRequestsShareCollector(t *testing.T) {
	var count, inflight, peak atomic.Int32
	e, _ := testEngine(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		current := inflight.Add(1)
		defer inflight.Add(-1)
		for {
			p := peak.Load()
			if current <= p || peak.CompareAndSwap(p, current) {
				break
			}
		}
		time.Sleep(10 * time.Millisecond)
		n := count.Add(1)
		if n < 3 {
			complete(w, fakeToken(11, 2))
		} else {
			complete(w, fakeToken(10, 3))
		}
	}))
	e.log = slog.New(slog.NewTextHandler(io.Discard, nil))
	// Scheduling delays are covered with a controlled clock in collection_budget_test.go.
	e.config.Collection.FailureIntervalSeconds = 0
	e.config.Collection.FailureMaxIntervalSeconds = 0
	runCollector(t, e)
	var wg sync.WaitGroup
	for i := 0; i < 12; i++ {
		wg.Add(1)
		go func() { defer wg.Done(); e.ServeHTTP(httptest.NewRecorder(), request(generation, "same-cold-account")) }()
	}
	wg.Wait()
	await(t, func() bool { return count.Load() >= 3 })
	if peak.Load() != 1 || count.Load() != 3 {
		t.Fatalf("duplicate collectors peak=%d count=%d", peak.Load(), count.Load())
	}
}

func TestFailedRouteMovesToNextSelectedRoute(t *testing.T) {
	var hits atomic.Int32
	good := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { hits.Add(1); complete(w, fakeToken(10, 4)) }))
	defer good.Close()
	bad := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(502) }))
	defer bad.Close()
	route := func(id, target string) proxyroute.Route {
		return proxyroute.Route{ID: id, Transport: &http.Transport{Proxy: func(*http.Request) (*url.URL, error) { return url.Parse(target) }}}
	}
	cfg := settings.Default()
	cfg.Upstream = "http://127.0.0.1:12345/backend-api/codex"
	cfg.MaxProbes = 1
	cfg.Collection.FailureIntervalSeconds = 0
	cfg.Collection.FailureMaxIntervalSeconds = 0
	e := New(cfg, []proxyroute.Route{route("bad", bad.URL), route("good", good.URL)}, slog.New(slog.NewTextHandler(io.Discard, nil)))
	defer e.Close()
	runCollector(t, e)
	s, _ := e.borrow(request(generation, "route-failure-account").Header)
	s.mu.Lock()
	s.activated = true
	s.mu.Unlock()
	release(s)
	e.signal()
	await(t, func() bool { snapshot, ok := s.state.Acquire(time.Now()); return ok && snapshot.Route == 1 })
	if hits.Load() != 1 {
		t.Fatal("good node was not selected")
	}
}

func TestContinuousCollectorStopsForAccountLimit(t *testing.T) {
	var calls atomic.Int32
	e, _ := testEngine(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		w.Header().Set("Retry-After", "60")
		w.WriteHeader(429)
	}))
	e.log = slog.New(slog.NewTextHandler(io.Discard, nil))
	runCollector(t, e)
	s, _ := e.borrow(request(generation, "limited-account").Header)
	s.mu.Lock()
	s.activated = true
	s.mu.Unlock()
	release(s)
	e.signal()
	await(t, func() bool { s.mu.Lock(); defer s.mu.Unlock(); return time.Now().Before(s.upstreamPause) })
	for i := 0; i < 20; i++ {
		e.signal()
	}
	time.Sleep(40 * time.Millisecond)
	if calls.Load() != 1 {
		t.Fatal("collector bypassed account pause")
	}
}

func TestEngineRestoresPrivateStateBackup(t *testing.T) {
	var probes, generations atomic.Int32
	token := fakeToken(10, 91)
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get(turnstate.Header) == "" {
			probes.Add(1)
		} else {
			generations.Add(1)
		}
		complete(w, token)
	}))
	defer upstream.Close()
	backup, err := turnstate.OpenBackup(filepath.Join(t.TempDir(), "state-backup.json"))
	if err != nil {
		t.Fatal(err)
	}
	cfg := settings.Default()
	cfg.Upstream = upstream.URL + "/backend-api/codex"
	cfg.ProbeSeconds = 3
	route := proxyroute.Route{ID: "stable-route", Transport: &http.Transport{Proxy: nil}}
	first := New(cfg, []proxyroute.Route{route}, slog.New(slog.NewTextHandler(io.Discard, nil)))
	first.SetStateBackup(backup)
	req := request(generation, "backup-account")
	w := httptest.NewRecorder()
	first.ServeHTTP(w, req)
	first.Close()
	if w.Code != 200 || probes.Load() != 1 {
		t.Fatalf("initial request status=%d probes=%d", w.Code, probes.Load())
	}
	second := New(cfg, []proxyroute.Route{{ID: "stable-route", Transport: &http.Transport{Proxy: nil}}}, slog.New(slog.NewTextHandler(io.Discard, nil)))
	second.SetStateBackup(backup)
	defer second.Close()
	w = httptest.NewRecorder()
	second.ServeHTTP(w, request(generation, "backup-account"))
	if w.Code != 200 || probes.Load() != 1 || generations.Load() != 2 {
		t.Fatalf("restored request status=%d probes=%d generations=%d", w.Code, probes.Load(), generations.Load())
	}
}
