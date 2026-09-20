package service

import (
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gylive/ccodex-sleep-state/internal/settings"
)

func TestApprovalReviewHistoryLabelAndModel(t *testing.T) {
	up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		io.WriteString(w, "data: {\"type\":\"response.completed\",\"response\":{\"model\":\"codex-auto-review\"}}\n\n")
	}))
	defer up.Close()
	c, h := panelControl(t, func(c *settings.Config) { c.Upstream = up.URL + "/backend-api/codex" })
	w := panelRequest(h, "POST", "/backend-api/codex/responses", `{"model":"codex-auto-review","input":"synthetic review"}`, map[string]string{"Authorization": "Bearer synthetic-review"})
	events := c.history.snapshot()["recent"].([]requestEvent)
	if w.Code != 200 || len(events) != 1 || events[0].Kind != "自动审批" || events[0].Model != "codex-auto-review" || events[0].StateInjected {
		t.Fatal("review history was misclassified", w.Code, events)
	}
}
