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
	Kind        string    `json:"kind"`
	Status      int       `json:"status"`
	DurationMS  int64     `json:"duration_ms"`
	At          time.Time `json:"at"`
	ID          string    `json:"id,omitempty"`
	Phase       string    `json:"phase,omitempty"`
	StartedAt   time.Time `json:"started_at,omitzero"`
	FirstByteAt time.Time `json:"first_byte_at,omitzero"`
	Bytes       int64     `json:"bytes"`
}
type requestHistory struct {
	mu            sync.Mutex
	total, failed uint64
	recent        []requestEvent
	active        map[string]requestEvent
	groups        map[string]*conversationTraffic
	requestGroups map[string]string
}

func (h *requestHistory) snapshot() map[string]any {
	h.mu.Lock()
	defer h.mu.Unlock()
	recent := append([]requestEvent{}, h.recent...)
	return map[string]any{"total": h.total, "failed": h.failed, "recent": recent, "active": len(h.active), "conversations": h.conversationsLocked(time.Now()), "conversation_limit": conversationLimit, "recent_per_conversation": conversationRecentLimit}
}

type observedResponse struct {
	http.ResponseWriter
	status      int
	onWrite     func(int)
	writeFailed bool
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
	n, err := w.ResponseWriter.Write(p)
	if err != nil {
		w.writeFailed = true
	}
	if w.onWrite != nil {
		w.onWrite(n)
	}
	return n, err
}
func (w *observedResponse) Flush() {
	if w.status == 0 {
		w.WriteHeader(200)
	}
	if err := http.NewResponseController(w.ResponseWriter).Flush(); err != nil {
		w.writeFailed = true
	}
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
