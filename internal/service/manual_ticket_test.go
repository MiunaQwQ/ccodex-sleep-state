package service

import (
	"encoding/json"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gylive/ccodex-sleep-state/internal/proxyroute"
	"github.com/gylive/ccodex-sleep-state/internal/settings"
)

func manualTicketControl(t *testing.T, listen string) (*control, http.Handler, *atomic.Int32, string) {
	t.Helper()
	attempts := &atomic.Int32{}
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var input struct {
			Instructions string `json:"instructions"`
		}
		json.NewDecoder(r.Body).Decode(&input)
		if input.Instructions == "Reply with OK." {
			attempts.Add(1)
			w.Header().Set("Retry-After", "3600")
			w.WriteHeader(429)
			return
		}
		w.Header().Set("Content-Type", "text/event-stream")
		io.WriteString(w, "data: {\"type\":\"response.completed\",\"response\":{\"model\":\"gpt-6-astra\"}}\n\n")
	}))
	t.Cleanup(upstream.Close)
	c, _ := panelControl(t, func(cfg *settings.Config) {
		cfg.Listen = listen
		cfg.Upstream = upstream.URL + "/backend-api/codex"
		cfg.PoolEnabled = true
		cfg.StateFallback = "passthrough"
		cfg.Collection.HourlyBudget = 1
	})
	c.stop()
	c.start([]proxyroute.Route{
		{ID: "manual-one", DisplayName: "模拟节点 A", Transport: &http.Transport{Proxy: nil}},
		{ID: "manual-two", DisplayName: "模拟节点 B", Transport: &http.Transport{Proxy: nil}},
	})
	c.pause()
	if until, _ := c.collection.Reserve("synthetic-budget", time.Now(), c.config.Collection); !until.IsZero() {
		t.Fatal("fixture could not exhaust automatic budget")
	}
	r := httptest.NewRequest("POST", "http://"+listen+"/backend-api/codex/responses", strings.NewReader(`{"model":"gpt-6-astra","input":"synthetic"}`))
	r.Header.Set("Authorization", "Bearer manual-ticket-fixture")
	w := httptest.NewRecorder()
	c.ServeHTTP(w, r)
	if w.Code != 200 || attempts.Load() != 0 {
		t.Fatal("fixture sent an automatic probe")
	}
	data, _ := json.Marshal(c.engine.Status())
	var status struct{ Sessions []struct{ ID string } }
	json.Unmarshal(data, &status)
	if len(status.Sessions) != 1 {
		t.Fatal("missing fixture session")
	}
	return c, controlHandler(listen, panelTestToken, c), attempts, status.Sessions[0].ID
}

func TestManualTicketHTTPDoesNotWaitOnBudgetOrPrevious429(t *testing.T) {
	c, h, attempts, id := manualTicketControl(t, "127.0.0.1:17841")
	for i := int32(1); i <= 2; i++ {
		w := panelPost(h, "state/retry", `{"id":"`+id+`","random_once":true}`)
		if w.Code != 400 || attempts.Load() != i || !strings.Contains(w.Body.String(), "upstream_rejected") {
			t.Fatalf("attempt %d blocked or replayed: %d %s", i, w.Code, w.Body.String())
		}
	}
	if status := c.collection.Status("synthetic-budget", time.Now(), c.config.Collection); status.HourlyUsed != 1 || status.WaitSeconds < 3500 {
		t.Fatal("manual ticket changed automatic budget")
	}
}

func TestBrowserManualTicketFixture(t *testing.T) {
	if os.Getenv("SLEEPSTATE_MANUAL_UI") != "1" {
		t.Skip("opt-in synthetic browser fixture")
	}
	c, _, attempts, _ := manualTicketControl(t, "127.0.0.1:17849")
	ticket := &browserTicket{value: "manual-ticket-synthetic-launch", expires: time.Now().Add(15 * time.Minute)}
	handler := controlHandler(c.config.Listen, panelTestToken, c, ticket)
	listener, err := net.Listen("tcp", c.config.Listen)
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan struct{})
	var once sync.Once
	server := &http.Server{Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/__finish":
			once.Do(func() { close(done) })
			w.WriteHeader(204)
		case "/__manual-check":
			json.NewEncoder(w).Encode(map[string]any{"attempts": attempts.Load(), "hourly_used": c.collection.Status("synthetic-budget", time.Now(), c.config.Collection).HourlyUsed})
		default:
			handler.ServeHTTP(w, r)
		}
	})}
	go server.Serve(listener)
	defer server.Close()
	t.Log("Manual ticket fixture http://127.0.0.1:17849/admin/#launch=manual-ticket-synthetic-launch")
	select {
	case <-done:
	case <-time.After(15 * time.Minute):
		t.Fatal("UI fixture expired")
	}
}
