package service

import (
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"net/http"
	"regexp"
	"sort"
	"time"

	"github.com/gylive/ccodex-sleep-state/internal/gateway"
)

const conversationLimit = 128
const conversationRecentLimit = 50

// Only protocol UUIDs are exposed. Request bodies, prompts, raw headers,
// authorization and turn-state tokens never enter traffic history.
var trafficUUID = regexp.MustCompile(`(?i)^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$`)

func conversationIdentity(r *http.Request) (key, id, source string) {
	for _, name := range []string{"thread_id", "session_id"} {
		if v := r.Header.Get(name); trafficUUID.MatchString(v) {
			id, source = v, name
			break
		}
	}
	if id == "" {
		raw := r.Header.Get("x-codex-turn-metadata")
		if len(raw) <= 4096 {
			var m struct {
				ThreadID string `json:"thread_id"`
			}
			if json.Unmarshal([]byte(raw), &m) == nil && trafficUUID.MatchString(m.ThreadID) {
				id, source = m.ThreadID, "x-codex-turn-metadata"
			}
		}
	}
	sum := sha256.Sum256([]byte(r.Header.Get("Authorization") + "\x00" + r.Header.Get("ChatGPT-Account-Id") + "\x00" + id))
	return hex.EncodeToString(sum[:]), id, source
}

type conversationTraffic struct {
	ID             string         `json:"id"`
	ConversationID string         `json:"conversation_id,omitempty"`
	IdentitySource string         `json:"identity_source,omitempty"`
	Received       uint64         `json:"received"`
	Completed      uint64         `json:"completed"`
	Failed         uint64         `json:"failed"`
	Cancelled      uint64         `json:"cancelled"`
	Unverified     uint64         `json:"unverified"`
	Active         int            `json:"active"`
	LastAt         time.Time      `json:"last_at"`
	Recent         []requestEvent `json:"recent"`
}

func (h *requestHistory) begin(r *http.Request, started time.Time) string {
	key, conversationID, source := conversationIdentity(r)
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.active == nil {
		h.active = map[string]requestEvent{}
		h.groups = map[string]*conversationTraffic{}
		h.requestGroups = map[string]string{}
	}
	group := h.groups[key]
	if group == nil {
		// Evict only inactive groups; an in-flight request is always visible.
		if len(h.groups) >= conversationLimit {
			oldest := ""
			for k, v := range h.groups {
				if v.Active == 0 && (oldest == "" || v.LastAt.Before(h.groups[oldest].LastAt)) {
					oldest = k
				}
			}
			if oldest != "" {
				delete(h.groups, oldest)
			}
		}
		group = &conversationTraffic{ID: rand.Text(), ConversationID: conversationID, IdentitySource: source, Recent: []requestEvent{}}
		h.groups[key] = group
	}
	id := rand.Text()
	group.Received++
	group.Active++
	group.LastAt = started
	h.total++
	h.active[id] = requestEvent{ID: id, Kind: requestKind(r.URL.Path), At: started, StartedAt: started, Phase: "received"}
	h.requestGroups[id] = key
	return id
}
func eventKind(e requestEvent) string {
	switch e.RequestOutcome.Kind {
	case "compact":
		return "远程压缩"
	case "approval_review":
		return "自动审批"
	}
	return e.Kind
}
func (h *requestHistory) progress(id string, outcome gateway.RequestOutcome) {
	h.mu.Lock()
	defer h.mu.Unlock()
	e, ok := h.active[id]
	if !ok {
		return
	}
	e.RequestOutcome = outcome
	e.Kind = eventKind(e)
	if !outcome.DispatchedAt.IsZero() {
		e.Phase = "waiting_upstream"
	}
	if !outcome.UpstreamAt.IsZero() {
		e.Phase = "receiving"
	}
	h.active[id] = e
}
func (h *requestHistory) written(id string, n int, at time.Time) {
	if n <= 0 {
		return
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	e, ok := h.active[id]
	if !ok {
		return
	}
	e.Bytes += int64(n)
	if e.FirstByteAt.IsZero() {
		e.FirstByteAt = at
	}
	h.active[id] = e
}
func (h *requestHistory) finish(id string, final requestEvent) {
	h.mu.Lock()
	defer h.mu.Unlock()
	e, ok := h.active[id]
	if !ok {
		return
	}
	final.ID, final.StartedAt, final.FirstByteAt, final.Bytes = id, e.StartedAt, e.FirstByteAt, e.Bytes
	final.Phase = "finished"
	final.Kind = eventKind(final)
	if final.Result == "" || final.Result == "unverified" {
		if final.Status >= 400 {
			final.Result = "failed"
		} else if final.Result == "" {
			final.Result = "unverified"
		}
	}
	if final.Result == "failed" {
		h.failed++
	}
	h.recent = appendRecent(h.recent, final, 100)
	if group := h.groups[h.requestGroups[id]]; group != nil {
		group.Active--
		group.LastAt = final.At
		switch final.Result {
		case "completed", "http_success":
			group.Completed++
		case "failed":
			group.Failed++
		case "cancelled":
			group.Cancelled++
		default:
			group.Unverified++
		}
		group.Recent = appendRecent(group.Recent, final, conversationRecentLimit)
	}
	delete(h.active, id)
	delete(h.requestGroups, id)
}
func appendRecent(events []requestEvent, e requestEvent, limit int) []requestEvent {
	if len(events) >= limit {
		copy(events, events[len(events)-limit+1:])
		events = events[:limit-1]
	}
	return append(events, e)
}
func (h *requestHistory) conversationsLocked(now time.Time) []conversationTraffic {
	result := []conversationTraffic{}
	for _, group := range h.groups {
		v := *group
		v.Recent = append([]requestEvent{}, group.Recent...)
		for id, e := range h.active {
			if h.groups[h.requestGroups[id]] == group {
				e.DurationMS = now.Sub(e.StartedAt).Milliseconds()
				v.Recent = append(v.Recent, e)
			}
		}
		sort.Slice(v.Recent, func(i, j int) bool { return v.Recent[i].StartedAt.After(v.Recent[j].StartedAt) })
		result = append(result, v)
	}
	sort.Slice(result, func(i, j int) bool {
		if (result[i].Active > 0) != (result[j].Active > 0) {
			return result[i].Active > 0
		}
		return result[i].LastAt.After(result[j].LastAt)
	})
	return result
}
