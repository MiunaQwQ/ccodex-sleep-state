package gateway

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gylive/ccodex-sleep-state/internal/turnstate"
)

const reviewRequest = `{"model":"codex-auto-review","input":"synthetic review input","instructions":"synthetic review policy","stream":true}`
const reviewDenial = "data: {\"type\":\"response.completed\",\"response\":{\"model\":\"codex-auto-review\",\"output\":[{\"type\":\"message\",\"content\":[{\"type\":\"output_text\",\"text\":\"deny: synthetic decision\"}]}]}}\n\n"

func TestApprovalReviewPreservesModelDecisionAndChatTickets(t *testing.T) {
	for _, encoding := range []string{"", "gzip", "zstd"} {
		t.Run(encoding, func(t *testing.T) {
			var calls atomic.Int32
			e, logs := testEngine(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				calls.Add(1)
				body, _ := io.ReadAll(r.Body)
				if string(body) != reviewRequest || r.Header.Get(turnstate.Header) != "" || r.Header.Get("Authorization") != "Bearer synthetic-review" {
					t.Error("review model, policy, input, or credential changed, or chat ticket leaked")
				}
				w.Header().Set(turnstate.Header, fakeToken(10, 99))
				w.Header().Set("Content-Type", "text/event-stream")
				io.WriteString(w, reviewDenial)
			}))
			e.config.PoolEnabled = true
			e.config.EgressMode = "random"
			// A node already used for collection remains usable for review traffic.
			if err := e.pool.Change([]string{"test-route"}, "used", "synthetic", false); err != nil {
				t.Fatal(err)
			}
			chat, _ := e.borrow(request(generation, "synthetic-review").Header)
			defer release(chat)
			for i := byte(1); i <= 3; i++ {
				token, _ := turnstate.Parse(fakeToken(10, i))
				chat.state.Offer(token, 0, time.Now())
			}
			observedAt := time.Now()
			before, _ := json.Marshal(e.cards(chat, observedAt))
			poolBefore := e.pool.Get("test-route")
			wire := []byte(reviewRequest)
			if encoding != "" {
				wire = encodeRequest(t, encoding, wire)
			}
			r := request(string(wire), "synthetic-review")
			r.Header.Set(turnstate.Header, "stale-client-chat-ticket")
			if encoding != "" {
				r.Header.Set("Content-Encoding", encoding)
			}
			outcome := &RequestOutcome{}
			w := httptest.NewRecorder()
			e.ServeHTTP(w, WithOutcome(r, outcome))
			if w.Code != 200 || w.Body.String() != reviewDenial || w.Header().Get(turnstate.Header) != "" || calls.Load() != 1 {
				t.Fatal("review decision was changed, rejected, replayed, or collected a ticket", w.Code, calls.Load())
			}
			after, _ := json.Marshal(e.cards(chat, observedAt))
			if string(before) != string(after) || chat.lastResponse != nil || chat.activated {
				t.Fatal("review modified chat tickets or chat observations")
			}
			if !reflect.DeepEqual(poolBefore, e.pool.Get("test-route")) {
				t.Fatal("review consumed or changed collection node state")
			}
			if outcome.Kind != "approval_review" || outcome.Model != approvalReviewModel || outcome.ResponseModel != approvalReviewModel || outcome.StateInjected {
				t.Fatal("incorrect review diagnostics", outcome)
			}
			for _, s := range e.sessions {
				if s.model == approvalReviewModel && (s.activated || s.probing != nil || s.state.Status(time.Now()).Usable) {
					t.Fatal("review activated ticket collection")
				}
			}
			if strings.Contains(logs.String(), "probe_started") || strings.Contains(logs.String(), "synthetic review policy") {
				t.Fatal("review triggered a probe or logged review content")
			}
		})
	}
}

func TestApprovalReviewErrorsAndChatLimitsStayIndependent(t *testing.T) {
	for _, status := range []int{200, 400, 401, 403, 429, 503} {
		for _, chatLimited := range []bool{false, true} {
			t.Run(fmt.Sprintf("%d/chat-limited-%t", status, chatLimited), func(t *testing.T) {
				var calls atomic.Int32
				body := `{"error":{"code":"synthetic_review_error"}}`
				if status == 200 {
					body = "data: {\"type\":\"response.failed\",\"error\":{\"code\":\"rate_limit_exceeded\"}}\n\n"
				}
				e, _ := testEngine(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					calls.Add(1)
					w.Header().Set("Content-Type", "text/event-stream")
					w.WriteHeader(status)
					io.WriteString(w, body)
				}))
				chat, _ := e.borrow(request(generation, "synthetic-review").Header)
				defer release(chat)
				if chatLimited {
					e.reject(chat, 429, time.Minute, 0)
				}
				before, _ := chat.rejection()
				w := httptest.NewRecorder()
				e.ServeHTTP(w, request(reviewRequest, "synthetic-review"))
				if w.Code != status || w.Body.String() != body || calls.Load() != 1 {
					t.Fatal("review response was replaced or retried", w.Code, calls.Load())
				}
				if after, _ := chat.rejection(); after != before {
					t.Fatal("review error changed chat quota or authentication status")
				}
			})
		}
	}
}

func TestApprovalReviewStillHonorsDisabledNodesAndAuthentication(t *testing.T) {
	e, _ := testEngine(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { t.Error("invalid request reached reviewer") }))
	w := httptest.NewRecorder()
	r := request(reviewRequest, "")
	e.ServeHTTP(w, r)
	if w.Code != 401 {
		t.Fatal("review bypassed authentication")
	}
	if err := e.pool.Change([]string{"test-route"}, "disabled", "manual", false); err != nil {
		t.Fatal(err)
	}
	w = httptest.NewRecorder()
	e.ServeHTTP(w, request(reviewRequest, "synthetic-review"))
	if w.Code != 503 {
		t.Fatal("review bypassed disabled node")
	}
}
