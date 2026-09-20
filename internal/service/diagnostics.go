package service

import (
	"github.com/gylive/ccodex-sleep-state/internal/gateway"
	"net/http"
	"sync"
	"time"
)

// Request history deliberately contains no URL, prompt, credential or state.
// It survives route reloads so an empty model session list never means no traffic.
type requestEvent struct {
	gateway.RequestOutcome
	Kind       string    `json:"kind"`
	Status     int       `json:"status"`
	DurationMS int64     `json:"duration_ms"`
	At         time.Time `json:"at"`
}
type requestHistory struct {
	mu            sync.Mutex
	total, failed uint64
	recent        []requestEvent
}

func (h *requestHistory) record(e requestEvent) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.total++
	if e.Status >= 400 || e.Result == "failed" {
		h.failed++
	}
	if len(h.recent) == 30 {
		copy(h.recent, h.recent[1:])
		h.recent = h.recent[:29]
	}
	h.recent = append(h.recent, e)
}
func (h *requestHistory) snapshot() map[string]any {
	h.mu.Lock()
	defer h.mu.Unlock()
	recent := append([]requestEvent{}, h.recent...)
	return map[string]any{"total": h.total, "failed": h.failed, "recent": recent}
}

type observedResponse struct {
	http.ResponseWriter
	status int
}

func (w *observedResponse) Unwrap() http.ResponseWriter { return w.ResponseWriter }
func (w *observedResponse) WriteHeader(code int) {
	// Informational responses do not close the final response headers.
	if code >= 100 && code < 200 {
		w.ResponseWriter.WriteHeader(code)
		return
	}
	if w.status != 0 {
		return
	}
	w.status = code
	w.ResponseWriter.WriteHeader(code)
}
func (w *observedResponse) Write(p []byte) (int, error) {
	if w.status == 0 {
		w.WriteHeader(200)
	}
	return w.ResponseWriter.Write(p)
}
func (w *observedResponse) Flush() {
	if w.status == 0 {
		w.WriteHeader(200)
	}
	_ = http.NewResponseController(w.ResponseWriter).Flush()
}
func requestKind(path string) string {
	switch path {
	case "/backend-api/codex/responses":
		return "生成"
	case "/backend-api/codex/responses/compact":
		return "远程压缩"
	case "/backend-api/codex/models":
		return "模型列表"
	case "/backend-api/codex/images/generations":
		return "图片生成"
	case "/backend-api/codex/images/edits":
		return "图片编辑"
	case "/backend-api/codex/alpha/search":
		return "搜索"
	default:
		return "其他接口"
	}
}
