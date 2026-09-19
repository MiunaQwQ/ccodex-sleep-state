package service

import (
	"context"
	"encoding/json"
	"github.com/gylive/ccodex-sleep-state/internal/settings"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestSubscriptionsAppendAndDedupe(t *testing.T) {
	c := &control{config: settings.Default()}
	c.config.Subscriptions = []settings.Source{{URL: "https://one.invalid/sub"}}
	next, err := c.candidate(sourceRequest{Mode: "subscription-list", Append: true, Value: "\ufeffhttps://one.invalid/sub\n# comment\nhttps://two.invalid/sub\r\nhttps://two.invalid/sub\n"})
	if err != nil || len(next.Subscriptions) != 2 || len(c.config.Subscriptions) != 1 {
		t.Fatal(err, next.Subscriptions)
	}
	_, err = c.candidate(sourceRequest{Mode: "subscription-list", Value: strings.Repeat("https://different.invalid/", 1) + strings.Join(func() []string {
		v := []string{}
		for i := 0; i < 17; i++ {
			v = append(v, "https://a.invalid/"+strings.Repeat("x", i))
		}
		return v
	}(), "\n")})
	if err == nil {
		t.Fatal("unbounded sources")
	}
}
func TestPolicySwitchPreservesEngine(t *testing.T) {
	c, h := panelControl(t, nil)
	engine := c.engine
	w := panelPost(h, "state-policy", `{"mode":"on_demand"}`)
	if w.Code != 200 || c.engine != engine || c.config.StateRefreshMode != "on_demand" {
		t.Fatal(w.Code, w.Body.String())
	}
}
func TestAdvancedInvalidDoesNotWrite(t *testing.T) {
	c, h := panelControl(t, nil)
	before := diskConfig(t, c)
	body := advancedFrom(c.config)
	body.RequestLimitMiB = 129
	b, _ := json.Marshal(body)
	w := panelPost(h, "advanced", string(b))
	if w.Code != 400 || diskConfig(t, c) != before {
		t.Fatal(w.Code)
	}
}
func TestRelayModelsFilterAndNoKeyEcho(t *testing.T) {
	calls := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		if r.URL.Path != "/v1/models" || r.Header.Get("Authorization") != "Bearer fixture-secret" {
			t.Error("wrong request")
		}
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte(`{"data":[{"id":"gpt-6-astra"},{"id":"gpt-5.6-sol"},{"id":"other"},{"id":"gpt-6-astra"}]}`))
	}))
	defer server.Close()
	result, err := relayModels(context.Background(), server.URL+"/v1/responses", "fixture-secret")
	if err != nil {
		t.Fatal(err)
	}
	b, _ := json.Marshal(result)
	if strings.Contains(string(b), "fixture-secret") || strings.Contains(string(b), "other") || calls != 1 {
		t.Fatal(string(b), calls)
	}
}
func TestRelayModelsRedirectDoesNotLeakKey(t *testing.T) {
	calls := 0
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { calls++ }))
	defer target.Close()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { http.Redirect(w, r, target.URL, 302) }))
	defer server.Close()
	r, err := relayModels(context.Background(), server.URL, "fixture-secret")
	if err != nil || calls != 0 || r["status"] != 302 {
		t.Fatal(err, calls)
	}
}

func TestLegacyPinClearsIndependentExit(t *testing.T) {
	c, h := panelControl(t, nil)
	next := c.config
	next.EgressMode = "random"
	next.EgressRoute = "old-independent"
	if err := c.persist(next); err != nil {
		t.Fatal(err)
	}
	c.config = next
	w := panelPost(h, "routes/pin", `{"id":""}`)
	if w.Code != 200 || c.config.EgressMode != "state" || c.config.EgressRoute != "" || c.engine == nil {
		t.Fatal(w.Code, w.Body.String())
	}
}

func TestSimplePoolImportActivatesExplicitPreset(t *testing.T) {
	c := &control{config: settings.Default()}
	next, err := c.candidate(sourceRequest{Mode: "subscription-list", Value: "https://fixture.invalid/sub", Append: true, EnablePool: true})
	if err != nil || !next.PoolEnabled || next.EgressMode != "random" {
		t.Fatal(next, err)
	}
	c.config.EgressMode = "fixed"
	c.config.EgressRoute = "fixed-id"
	next, err = c.candidate(sourceRequest{Mode: "subscription-list", Value: "https://fixture.invalid/sub", Append: true, EnablePool: true})
	if err != nil || next.EgressMode != "fixed" || next.EgressRoute != "fixed-id" {
		t.Fatal(next, err)
	}
}
