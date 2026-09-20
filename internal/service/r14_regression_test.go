package service

import (
	"encoding/base64"
	"encoding/json"
	"github.com/gylive/ccodex-sleep-state/internal/settings"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestR14UnchangedAdvancedKeepsPin(t *testing.T) {
	c, h := panelControl(t, func(c *settings.Config) { c.PinnedRoute = "direct"; c.EgressMode = "state" })
	body, _ := json.Marshal(advancedFrom(c.config))
	w := panelPost(h, "advanced", string(body))
	if w.Code != 200 || c.config.PinnedRoute != "direct" {
		t.Fatal("saving advanced lost pin", w.Code)
	}
}
func TestR14TimingHotUpdateDuringChatAndFailureHistory(t *testing.T) {
	started, finish := make(chan struct{}), make(chan struct{})
	up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		close(started)
		<-finish
		w.Header().Set("Content-Type", "text/event-stream")
		w.Write([]byte("data: {\"type\":\"response.failed\",\"error\":{\"code\":\"server_is_overloaded\"}}\n\n"))
	}))
	defer up.Close()
	c, h := panelControl(t, func(c *settings.Config) { c.Upstream = up.URL + "/backend-api/codex"; c.InjectionDisabled = true })
	engine := c.engine
	done := make(chan struct{})
	go func() {
		defer close(done)
		panelRequest(h, "POST", "/backend-api/codex/responses", `{"model":"gpt-6-astra","input":[{"type":"compaction_trigger"}]}`, map[string]string{"Authorization": "Bearer synthetic-r14"})
	}()
	<-started
	timing := timingFrom(c.config)
	timing.CooldownSeconds = 240
	body, _ := json.Marshal(timing)
	w := panelPost(h, "timing", string(body))
	close(finish)
	<-done
	if w.Code != 200 || c.engine != engine || c.config.CooldownSeconds != 240 {
		t.Fatal("timing rebuilt or blocked active engine", w.Code, w.Body.String())
	}
	history := c.history.snapshot()
	if history["failed"] != uint64(1) {
		t.Fatal("200 business error not counted")
	}
	events := history["recent"].([]requestEvent)
	if len(events) != 1 || events[0].Kind != "远程压缩" || events[0].Result != "failed" {
		t.Fatal("wrong compact classification")
	}
}
func TestR14UnifiedImportWrappersAndMixedSources(t *testing.T) {
	c, _ := panelControl(t, nil)
	url := "https://example.invalid/subscribe?token=synthetic"
	wrapper := "sub://" + base64.RawStdEncoding.EncodeToString([]byte(url)) + "#label"
	for _, value := range []string{wrapper, `{"type":"Subscribe","host":"` + url + `"}`, "[link](" + url + ")"} {
		v, err := c.candidate(sourceRequest{Mode: "auto", Value: value})
		if err != nil || len(v.Subscriptions) != 1 || v.Subscriptions[0].URL != url {
			t.Fatal("wrapper not imported", err)
		}
	}
	v, err := c.candidate(sourceRequest{Mode: "auto", Value: url + "\nsocks5://127.0.0.1:1080\nhttp://127.0.0.1:8080"})
	if err != nil || len(v.Subscriptions) != 1 || len(v.ProxyURLs) != 2 {
		t.Fatal("mixed sources misclassified", err)
	}
	if _, err := normalizeSource(sourceRequest{Mode: "auto", Value: strings.Repeat("x", 65537)}); err == nil {
		t.Fatal("oversized TXT accepted")
	}
}
func TestR14RetestFailedRestoresOnlyPassed(t *testing.T) {
	up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "" {
			t.Error("test leaked model credential")
		}
		w.WriteHeader(401)
	}))
	defer up.Close()
	c, h := panelControl(t, func(c *settings.Config) { c.Upstream = up.URL })
	c.pool.Change([]string{"direct"}, "failed", "network_failed", false)
	w := panelPost(h, "pool/test", `{"target":"upstream","recover_failed":true,"timeout_seconds":1}`)
	if w.Code != 202 {
		t.Fatal(w.Code, w.Body.String())
	}
	deadline := time.Now().Add(time.Second)
	for c.tests.snapshot().Running && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	result := c.tests.snapshot().Results["direct"]
	if !result.Restored || !result.Selectable || len(result.Checks) != 1 || c.pool.Get("direct").State != "available" {
		t.Fatalf("retest did not restore: %+v", result)
	}
}
