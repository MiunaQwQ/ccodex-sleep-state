package gateway

import (
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/gylive/ccodex-sleep-state/internal/proxyroute"
	"github.com/gylive/ccodex-sleep-state/internal/turnstate"
)

func TestCompactionReusesTicketAndSourceWithoutMutatingCards(t *testing.T) {
	for _, v2 := range []bool{false, true} {
		for _, enabled := range []bool{false, true} {
			name := "v1"
			if v2 {
				name = "v2"
			}
			if !enabled {
				name += "-disabled"
			}
			t.Run(name, func(t *testing.T) {
				calls := 0
				token, _ := turnstate.Parse(fakeToken(10, 181))
				body := compactFixtureRequest
				path := "/backend-api/codex/responses/compact"
				if v2 {
					body = `{"model":"gpt-6-astra","input":[{"type":"compaction_trigger"}],"stream":true}`
					path = "/backend-api/codex/responses"
				}
				e, logs := testEngine(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					calls++
					data, _ := io.ReadAll(r.Body)
					wantHeader := "client-ticket"
					if enabled {
						wantHeader = token.Value
					}
					if r.URL.Path != path || string(data) != body || r.Header.Get(turnstate.Header) != wantHeader {
						t.Error("compact body/path/ticket changed incorrectly")
					}
					w.Header().Set(turnstate.Header, fakeToken(13, 182))
					if v2 {
						w.Header().Set("Content-Type", "text/event-stream")
						io.WriteString(w, "data: {\"type\":\"response.completed\",\"response\":{\"model\":\"gpt-6-astra\"}}\n\n")
					} else {
						w.Header().Set("Content-Type", "application/json")
						io.WriteString(w, `{"object":"response.compaction","model":"gpt-6-astra","output":[{"type":"compaction","encrypted_content":"fixture"}]}`)
					}
				}))
				source := e.routes[0]
				e.routes = []proxyroute.Route{{ID: "wrong-exit", Transport: &http.Transport{Proxy: func(*http.Request) (*url.URL, error) { return nil, errors.New("wrong exit") }}}, source}
				e.config.PoolEnabled = true
				e.config.PinnedRoute = "wrong-exit"
				e.config.EgressMode = "fixed"
				e.disabled.Store(!enabled)
				r := request(body, "compact-ticket-account")
				r.URL.Path = path
				r.Header.Set(turnstate.Header, "client-ticket")
				s, _ := e.borrow(r.Header)
				defer release(s)
				if !s.state.Offer(token, 1, time.Now()) {
					t.Fatal("seed state rejected")
				}
				before, _ := s.state.Acquire(time.Now())
				outcome := &RequestOutcome{}
				w := httptest.NewRecorder()
				e.ServeHTTP(w, WithOutcome(r, outcome))
				after, _ := s.state.Acquire(time.Now())
				if calls != 1 || w.Code != 200 || outcome.Result != "completed" || outcome.Kind != "compact" || outcome.StateInjected != enabled || outcome.ResponseModel != "gpt-6-astra" || outcome.RouteID != source.ID {
					t.Fatalf("wrong compact result: calls=%d code=%d outcome=%+v", calls, w.Code, outcome)
				}
				if before != after || s.state.Status(time.Now()).Strikes != 0 || s.activated {
					t.Fatal("compact modified or activated collection state")
				}
				if s.lastResponse == nil || s.lastResponse.StateInjected != enabled {
					t.Fatal("missing injection evidence")
				}
				if strings.Contains(logs.String(), token.Value) {
					t.Fatal("ticket leaked to logs")
				}
				if enabled {
					// Like chat, an existing ticket may retry its own source after
					// a network failure; never silently switch to a ticketless exit.
					e.failRoute(1)
					r = request(body, "compact-ticket-account")
					r.URL.Path = path
					outcome = &RequestOutcome{}
					w = httptest.NewRecorder()
					e.ServeHTTP(w, WithOutcome(r, outcome))
					if calls != 2 || w.Code != 200 || !outcome.StateInjected || outcome.RouteID != source.ID {
						t.Fatal("network failure detached compact ticket from its source")
					}
				}
			})
		}
	}
}
