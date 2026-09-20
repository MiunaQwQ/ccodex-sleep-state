package service

import (
	"io"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"

	"github.com/gylive/ccodex-sleep-state/internal/settings"
)

func TestImageAndFutureFeatureThroughFullHandler(t *testing.T) {
	var calls atomic.Int32
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		if r.URL.Path != "/v1/images/generations" && r.URL.Path != "/v1/future-feature" {
			t.Errorf("wrong base URL mapping: %s", r.URL.Path)
		}
		body, _ := io.ReadAll(r.Body)
		if string(body) != `{"model":"gpt-image-2","prompt":"fixture"}` {
			t.Error("body changed")
		}
		_, _ = io.WriteString(w, `{"data":[]}`)
	}))
	defer upstream.Close()
	c, h := panelControl(t, func(cfg *settings.Config) { cfg.Upstream = upstream.URL + "/v1"; cfg.UpstreamKind = "relay" })
	for _, path := range []string{"images/generations", "future-feature"} {
		w := panelRequest(h, "POST", "/backend-api/codex/"+path, `{"model":"gpt-image-2","prompt":"fixture"}`, map[string]string{"Authorization": "Bearer fixture-feature-key"})
		if w.Code != 200 || w.Body.String() != `{"data":[]}` {
			t.Fatalf("feature blocked: %d %s", w.Code, w.Body.String())
		}
	}
	w := panelRequest(h, "GET", "/admin/api/status", "", nil)
	if w.Code != 401 || calls.Load() != 2 {
		t.Fatal("management isolation changed")
	}
	events := c.history.snapshot()["recent"].([]requestEvent)
	if len(events) != 2 || events[0].Kind != "图片生成" || events[1].Kind != "其他接口" {
		t.Fatal("feature diagnostics missing")
	}
}
