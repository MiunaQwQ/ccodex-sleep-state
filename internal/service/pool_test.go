package service

import (
	"encoding/json"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/gylive/ccodex-sleep-state/internal/proxyroute"
	"github.com/gylive/ccodex-sleep-state/internal/settings"
)

func TestAppendFullVLESSSelectAndRemoveSource(t *testing.T) {
	old := "http://user:private-password@127.0.0.1:18001"
	uri := "vless://00000000-0000-4000-8000-000000000001@127.0.0.1:443?security=tls&type=tcp#my-vless"
	c, h := panelControl(t, func(c *settings.Config) { c.Direct = false; c.ProxyURLs = []string{old} })
	body, _ := json.Marshal(map[string]any{"mode": "proxy", "value": uri, "append": true})
	w := panelPost(h, "sources/apply", string(body))
	if w.Code != 200 {
		t.Fatal(w.Code, w.Body.String())
	}
	if len(c.config.ProxyURLs) != 2 || c.config.ProxyURLs[0] != old {
		t.Fatal("append erased original source")
	}
	get := func(token string) *httptest.ResponseRecorder {
		return panelRequest(h, "GET", "/admin/api/pool", "", map[string]string{"Authorization": "Bearer " + token})
	}
	if w = get(""); w.Code != 401 || strings.Contains(w.Body.String(), uri) {
		t.Fatal("full links exposed without authentication")
	}
	w = get(panelTestToken)
	var pool struct {
		Routes  []struct{ ID, Connection string }
		Sources []struct{ ID, Value string }
	}
	if json.Unmarshal(w.Body.Bytes(), &pool) != nil || len(pool.Routes) != 2 {
		t.Fatal("missing node catalog")
	}
	selected := ""
	for _, route := range pool.Routes {
		if route.Connection == uri {
			selected = route.ID
		}
	}
	if selected == "" || !strings.Contains(w.Body.String(), "private-password") {
		t.Fatal("owner links were masked")
	}
	payload, _ := json.Marshal(map[string]any{"ids": []string{selected}})
	w = panelPost(h, "pool/select", string(payload))
	if w.Code != 200 {
		t.Fatal(w.Code, w.Body.String())
	}
	stored, err := settings.Load(c.path)
	if err != nil || len(stored.SelectedRoutes) != 1 || stored.SelectedRoutes[0] != selected || c.engine.Status()["routes"] != 1 {
		t.Fatal("selection was not enforced/persisted")
	}
	// Re-select on disk reload to verify persisted selection, not just UI state.
	c.mu.Lock()
	c.stop()
	routes, err := proxyroute.Load(c.ctx, stored)
	if err == nil {
		c.config = stored
		c.start(routes)
	}
	c.mu.Unlock()
	if err != nil || c.engine.Status()["routes"] != 1 {
		t.Fatal("selection did not survive reload")
	}
	for _, source := range pool.Sources {
		if source.Value == old {
			payload, _ = json.Marshal(map[string]string{"id": source.ID})
		}
	}
	w = panelPost(h, "pool/remove-source", string(payload))
	if w.Code != 200 {
		t.Fatal(w.Code, w.Body.String())
	}
	if len(c.config.ProxyURLs) != 1 || c.config.ProxyURLs[0] != uri {
		t.Fatal("wrong source removed")
	}
	before := diskConfig(t, c)
	if w = panelPost(h, "pool/select", `{"ids":[]}`); w.Code != 400 || diskConfig(t, c) != before {
		t.Fatal("empty selection mutated working config")
	}
}

func TestSelectionCannotAccidentallyEnableNewImportedNodes(t *testing.T) {
	c, h := panelControl(t, func(c *settings.Config) { c.Direct = false; c.ProxyURLs = []string{"http://127.0.0.1:18001"} })
	id := c.catalog[0]["id"].(string)
	payload, _ := json.Marshal(map[string]any{"ids": []string{id}})
	if w := panelPost(h, "pool/select", string(payload)); w.Code != 200 {
		t.Fatal(w.Body.String())
	}
	w := panelPost(h, "sources/apply", `{"append":true,"mode":"proxy","value":"socks5://127.0.0.1:18002"}`)
	if w.Code != 200 || len(c.catalog) != 2 || c.engine.Status()["routes"] != 1 {
		t.Fatal("new node bypassed saved selection", w.Body.String())
	}
}

func TestCandidateSelectionEnablesContinuousOnDemandWorkflow(t *testing.T) {
	c, h := panelControl(t, func(cfg *settings.Config) {
		cfg.PoolEnabled = true
		cfg.EgressMode = "random"
		cfg.StateRefreshMode = "standby"
	})
	w := panelPost(h, "pool/select", `{"ids":["direct"]}`)
	if w.Code != 200 || !c.config.PoolEnabled || c.config.EgressMode != "state" || c.config.StateRefreshMode != "on_demand" {
		t.Fatal(w.Code, w.Body.String())
	}
}

func TestPinnedNodeKeepsSelectedAlternativesLoaded(t *testing.T) {
	c, h := panelControl(t, func(c *settings.Config) {
		c.Direct = false
		c.ProxyURLs = []string{"socks5://127.0.0.1:10801", "socks5://127.0.0.1:10802"}
	})
	id := c.catalog[0]["id"].(string)
	payload, _ := json.Marshal(map[string]string{"id": id})
	w := panelPost(h, "routes/pin", string(payload))
	if w.Code != 200 || c.engine.Status()["routes"] != 2 || c.config.PinnedRoute != id {
		t.Fatal("pinning discarded failover candidates", w.Code)
	}
}
