package service

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/gylive/ccodex-sleep-state/internal/settings"
)

func TestPanelSnapshotsAndLatencyDuringGeneration(t *testing.T) {
	started, finish := make(chan struct{}), make(chan struct{})
	var once sync.Once
	release := func() { once.Do(func() { close(finish) }) }
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.HasSuffix(r.URL.Path, "/models") {
			if r.Header.Get("Authorization") != "" {
				t.Error("latency test must not use the generation credential")
			}
			w.WriteHeader(401)
			return
		}
		close(started)
		select {
		case <-finish:
		case <-r.Context().Done():
		}
		w.Write([]byte("generation finished"))
	}))
	defer upstream.Close()
	defer release()
	c, h := panelControl(t, func(cfg *settings.Config) {
		cfg.Upstream = upstream.URL
		cfg.InjectionDisabled = true
	})
	before, engine := diskConfig(t, c), c.engine
	done := make(chan *httptest.ResponseRecorder, 1)
	go func() {
		done <- panelRequest(h, "POST", "/backend-api/codex/responses", `{"model":"gpt-6-astra","input":"test"}`, map[string]string{"Authorization": "Bearer synthetic-generation"})
	}()
	select {
	case <-started:
	case <-time.After(2 * time.Second):
		t.Fatal("generation did not reach upstream")
	}

	// Independent views and latency tools also work while another management
	// action owns its serialization lock (e.g. a manual state probe).
	c.action.Lock()
	for _, path := range []string{"status", "pool", "pool/status"} {
		t.Run(path, func(t *testing.T) {
			result := make(chan *httptest.ResponseRecorder, 1)
			go func() {
				result <- panelRequest(h, "GET", "/admin/api/"+path, "", map[string]string{"Authorization": "Bearer " + panelTestToken})
			}()
			select {
			case w := <-result:
				if w.Code != 200 {
					t.Fatalf("snapshot blocked: %d %s", w.Code, w.Body.String())
				}
			case <-time.After(time.Second):
				t.Error("snapshot waited for generation")
			}
		})
	}
	for _, tc := range []struct {
		path, body string
		status     int
	}{
		{"pool", `{}`, 200},
		{"pool/test", `{"ids":["direct"],"timeout_seconds":1}`, 202},
		{"pool/test-stop", `{}`, 200},
	} {
		w := panelPost(h, tc.path, tc.body)
		if w.Code != tc.status {
			t.Errorf("%s: %d %s", tc.path, w.Code, w.Body.String())
		}
	}
	c.action.Unlock()
	if w := panelPost(h, "pool/select", `{"ids":["direct"]}`); w.Code != 409 {
		t.Error("route rebuild must still protect the active generation", w.Code)
	}
	if w := panelRequest(h, "GET", "/admin/api/pool/status", "", nil); w.Code != 401 {
		t.Error("node snapshot requires authentication", w.Code)
	}
	if c.engine != engine || diskConfig(t, c) != before {
		t.Error("viewing or testing nodes rebuilt the active engine")
	}
	select {
	case <-done:
		t.Error("management operation interrupted generation")
	default:
	}
	release()
	select {
	case w := <-done:
		if w.Code != 200 || w.Body.String() != "generation finished" {
			t.Errorf("generation changed: %d %s", w.Code, w.Body.String())
		}
	case <-time.After(2 * time.Second):
		t.Error("generation failed to finish")
	}
}
