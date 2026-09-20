package service

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/gylive/ccodex-sleep-state/internal/settings"
)

func TestNodeResumeAuthenticatedAndAvailableWhileRepliesHoldReadLock(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(200) }))
	defer upstream.Close()
	c, h := panelControl(t, func(cfg *settings.Config) { cfg.Upstream = upstream.URL; cfg.InjectionDisabled = true })
	r := httptest.NewRequest("POST", "http://127.0.0.1/backend-api/codex/responses", strings.NewReader(`{"model":"gpt-6-astra","input":"test"}`))
	r.Header.Set("Authorization", "Bearer synthetic-node-resume")
	c.engine.ServeHTTP(httptest.NewRecorder(), r)
	var status struct {
		Sessions []struct {
			ID string `json:"id"`
		} `json:"sessions"`
	}
	data, _ := json.Marshal(c.engine.Status())
	if err := json.Unmarshal(data, &status); err != nil || len(status.Sessions) != 1 {
		t.Fatal("missing fixture session")
	}
	body, _ := json.Marshal(map[string]string{"session_id": status.Sessions[0].ID, "route_id": c.engine.PoolStatus()[0].ID})
	if w := panelRequest(h, "POST", "/admin/api/nodes/resume", string(body), nil); w.Code != 401 {
		t.Fatal("resume accepted unauthenticated request")
	}
	before, engine := diskConfig(t, c), c.engine
	c.mu.RLock()
	w := panelPost(h, "nodes/resume", string(body))
	c.mu.RUnlock()
	if w.Code != 200 || c.engine != engine || diskConfig(t, c) != before {
		t.Fatal("resume blocked by reply, rebuilt engine or changed config", w.Code)
	}
}
