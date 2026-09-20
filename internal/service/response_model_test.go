package service

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gylive/ccodex-sleep-state/internal/gateway"
	"github.com/gylive/ccodex-sleep-state/internal/settings"
)

func TestResponseModelPanelAndPerRequestHistory(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte(`{"object":"response","status":"completed","model":"gpt-6-sol","output":[]}`))
	}))
	defer upstream.Close()
	c, h := panelControl(t, func(c *settings.Config) { c.Upstream = upstream.URL + "/backend-api/codex"; c.InjectionDisabled = true })
	for _, model := range []string{"gpt-5.6-sol", "gpt-5.6-terra"} {
		w := panelRequest(h, "POST", "/backend-api/codex/responses", `{"model":"`+model+`","input":"synthetic"}`, map[string]string{"Authorization": "Bearer response-model-test"})
		if w.Code != 200 {
			t.Fatal(w.Code)
		}
	}
	history := c.history.snapshot()["recent"].([]requestEvent)
	if len(history) != 2 || history[0].Model != "gpt-5.6-sol" || history[1].Model != "gpt-5.6-terra" || history[0].ResponseModel != "gpt-6-sol" || history[1].ResponseModel != "gpt-6-sol" {
		t.Fatal("request/response model history lost")
	}
	w := panelRequest(h, "GET", "/admin/api/status", "", map[string]string{"Authorization": "Bearer " + panelTestToken})
	var status struct {
		Sessions []struct {
			LastResponse *gateway.ResponseModelObservation `json:"last_response"`
		}
	}
	if w.Code != 200 || json.Unmarshal(w.Body.Bytes(), &status) != nil || len(status.Sessions) != 2 {
		t.Fatal("missing panel sessions")
	}
	for _, s := range status.Sessions {
		if s.LastResponse == nil || s.LastResponse.Model != "gpt-6-sol" {
			t.Fatal("panel omitted actual upstream model")
		}
	}
}
