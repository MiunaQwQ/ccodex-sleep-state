package service

import (
	"context"
	"encoding/json"
	"github.com/gylive/ccodex-sleep-state/internal/settings"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"
)

const trafficThreadA = "11111111-1111-4111-8111-111111111111"
const trafficThreadB = "22222222-2222-4222-8222-222222222222"

func trafficRequest(id, body string) *http.Request {
	r := httptest.NewRequest("POST", "http://127.0.0.1/backend-api/codex/responses", strings.NewReader(body))
	r.Header.Set("Authorization", "Bearer traffic-private-auth")
	r.Header.Set("session_id", id)
	return r
}
func TestTrafficVisibleBeforeHeadersAndDuringStreamSeparatedByConversation(t *testing.T) {
	entered, headers, finish := make(chan struct{}), make(chan struct{}), make(chan struct{})
	var once sync.Once
	end := func() { once.Do(func() { close(finish) }) }
	defer end()
	up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		close(entered)
		<-headers
		w.Header().Set("Content-Type", "text/event-stream")
		io.WriteString(w, "data: {\"type\":\"response.created\",\"response\":{\"model\":\"gpt-6-astra\"}}\n\n")
		w.(http.Flusher).Flush()
		<-finish
		io.WriteString(w, "data: {\"type\":\"response.completed\",\"response\":{\"model\":\"gpt-6-astra\"}}\n\n")
	}))
	defer up.Close()
	c, _ := panelControl(t, func(cfg *settings.Config) { cfg.InjectionDisabled = true; cfg.Upstream = up.URL })
	done := make(chan struct{})
	go func() {
		defer close(done)
		c.ServeHTTP(httptest.NewRecorder(), trafficRequest(trafficThreadA, `{"model":"gpt-6-astra","input":"never-store-this-prose"}`))
	}()
	<-entered
	snapshot := c.history.snapshot()
	groups := snapshot["conversations"].([]conversationTraffic)
	if snapshot["total"] != uint64(1) || snapshot["active"] != 1 || len(groups) != 1 || groups[0].Received != 1 || groups[0].Recent[0].Phase != "waiting_upstream" || groups[0].Recent[0].SessionID == "" {
		t.Fatal("request invisible before upstream", snapshot)
	}
	close(headers)
	deadline := time.Now().Add(2 * time.Second)
	for {
		groups = c.history.snapshot()["conversations"].([]conversationTraffic)
		if groups[0].Recent[0].Bytes > 0 && groups[0].Recent[0].Phase == "receiving" {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("stream progress invisible")
		}
		time.Sleep(time.Millisecond)
	}
	// Local rejection belongs to another real conversation even on same account.
	c.ServeHTTP(httptest.NewRecorder(), trafficRequest(trafficThreadB, `{"model":"unsupported"}`))
	end()
	<-done
	snapshot = c.history.snapshot()
	groups = snapshot["conversations"].([]conversationTraffic)
	if snapshot["total"] != uint64(2) || snapshot["active"] != 0 || len(groups) != 2 {
		t.Fatal(snapshot)
	}
	for _, g := range groups {
		if g.Received != 1 || g.Active != 0 {
			t.Fatal("wrong counts")
		}
		e := g.Recent[0]
		if g.ConversationID == trafficThreadA && (g.Completed != 1 || e.Result != "completed" || e.ResponseModel != "gpt-6-astra" || e.Bytes == 0 || e.RouteID != "direct") {
			t.Fatal("completion missing", g)
		}
		if g.ConversationID == trafficThreadB && (g.Failed != 1 || !e.DispatchedAt.IsZero() || e.Status != 400) {
			t.Fatal("local rejection reported as forwarded", g)
		}
	}
	raw, _ := json.Marshal(snapshot)
	for _, secret := range []string{"traffic-private-auth", "never-store-this-prose", "unsupported"} {
		if strings.Contains(string(raw), secret) {
			t.Fatal("private data stored", secret)
		}
	}
}
func TestTrafficIdentityFallbackAndCredentialIsolation(t *testing.T) {
	a := trafficRequest(trafficThreadA, "")
	b := trafficRequest(trafficThreadA, "")
	b.Header.Set("Authorization", "Bearer different-account")
	ka, ida, _ := conversationIdentity(a)
	kb, _, _ := conversationIdentity(b)
	if ka == kb || ida != trafficThreadA {
		t.Fatal("cross account grouping")
	}
	a.Header.Set("thread_id", trafficThreadB)
	_, id, source := conversationIdentity(a)
	if id != trafficThreadB || source != "thread_id" {
		t.Fatal("thread precedence")
	}
	a.Header.Del("thread_id")
	a.Header.Set("session_id", "sensitive-arbitrary-header")
	_, id, _ = conversationIdentity(a)
	if id != "" {
		t.Fatal("raw header exposed")
	}
	a.Header.Set("x-codex-turn-metadata", `{"thread_id":"`+trafficThreadA+`"}`)
	_, id, _ = conversationIdentity(a)
	if id != trafficThreadA {
		t.Fatal("metadata identity missing")
	}
}
func TestTrafficCancelledBeforeDispatchAndUnconfirmed200(t *testing.T) {
	c, _ := panelControl(t, func(cfg *settings.Config) { cfg.InjectionDisabled = true })
	r := trafficRequest(trafficThreadA, `{"model":"gpt-6-astra"}`)
	ctx, cancel := context.WithCancel(r.Context())
	cancel()
	c.ServeHTTP(httptest.NewRecorder(), r.WithContext(ctx))
	group := c.history.snapshot()["conversations"].([]conversationTraffic)[0]
	if group.Cancelled != 1 || group.Completed != 0 {
		t.Fatal("cancellation treated as success", group)
	}
	// A body without the completion marker cannot become successful merely
	// because the upstream returned HTTP 200.
	up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		io.WriteString(w, "data: {\"type\":\"response.created\"}\n\n")
	}))
	defer up.Close()
	c2, _ := panelControl(t, func(cfg *settings.Config) { cfg.InjectionDisabled = true; cfg.Upstream = up.URL })
	c2.ServeHTTP(httptest.NewRecorder(), trafficRequest(trafficThreadB, `{"model":"gpt-6-astra"}`))
	group = c2.history.snapshot()["conversations"].([]conversationTraffic)[0]
	if group.Completed != 0 || group.Failed+group.Unverified != 1 {
		t.Fatal("200 falsely successful", group)
	}
}
