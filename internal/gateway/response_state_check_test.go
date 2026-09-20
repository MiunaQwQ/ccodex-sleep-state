package gateway

import (
	"context"
	"encoding/json"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gylive/ccodex-sleep-state/internal/proxyroute"
	"github.com/gylive/ccodex-sleep-state/internal/turnstate"
)

func TestProbeMismatchOnAnotherNodeDoesNotInvalidateMain(t *testing.T) {
	e, _ := testEngine(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		complete(w, fakeToken(11, 210))
	}))
	other := e.routes[0]
	other.ID, other.DisplayName = "other-node", "Korea node 2"
	e.routes = append(e.routes, other)
	s, _ := e.borrow(request(generation, "separate-probe-result").Header)
	defer release(s)
	main, _ := turnstate.Parse(fakeToken(10, 211))
	s.state.Offer(main, 0, time.Now())
	e.refresh(context.Background(), s, false, 1)
	active, ok := s.state.Acquire(time.Now())
	if !ok || active.Token.Fingerprint != main.Fingerprint || active.Route != 0 {
		t.Fatal("a rejected candidate on another node discarded the main")
	}
	// Later response observations must not change which node the probe tested.
	e.recordNode(s, 1, "accepted", 200, 10, 0, "response")
	var status struct {
		Sessions []struct {
			Usable          bool             `json:"usable"`
			LastProbeResult *NodeObservation `json:"last_probe_result"`
		} `json:"sessions"`
	}
	data, err := json.Marshal(e.Status())
	if err != nil || json.Unmarshal(data, &status) != nil || len(status.Sessions) != 1 {
		t.Fatal("invalid session status")
	}
	probe := status.Sessions[0].LastProbeResult
	if !status.Sessions[0].Usable || probe == nil || probe.Route != "other-node" || probe.Length != 312 || probe.Result != "shape_mismatch" || probe.Source != "probe" {
		t.Fatal("probe evidence mixed with main state or a later response")
	}
}

func TestRejectedMainIsNotReusedWithoutStandbyEvenOnLastRoute(t *testing.T) {
	for _, fallback := range []string{"strict", "passthrough"} {
		t.Run(fallback, func(t *testing.T) {
			calls := 0
			e, _ := testEngine(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				calls++
				if calls == 1 {
					if r.Header.Get(turnstate.Header) != fakeToken(10, 212) {
						t.Error("first request did not use main")
					}
					complete(w, fakeToken(11, 213))
				} else {
					if r.Header.Get(turnstate.Header) != "" {
						t.Error("rejected main was injected again")
					}
					io.WriteString(w, "plain reply without state")
				}
			}))
			e.running.Store(true)
			e.config.StateFallback = fallback
			s, _ := e.borrow(request(generation, "last-route-rejected-main").Header)
			main, _ := turnstate.Parse(fakeToken(10, 212))
			s.state.Offer(main, 0, time.Now())
			release(s)
			first := httptest.NewRecorder()
			e.ServeHTTP(first, request(generation, "last-route-rejected-main"))
			if first.Code != 200 || calls != 1 {
				t.Fatal("original answer was lost or replayed")
			}
			if _, ok := s.state.Acquire(time.Now()); ok || !s.retainedLast {
				t.Fatal("retaining last route also retained an invalid state")
			}
			if saved := s.state.Export(time.Now(), func(int) string { return "last-route" }); saved.Active != nil || len(saved.Standby) != 0 {
				t.Fatal("rejected main remained in persisted pool")
			}
			second := httptest.NewRecorder()
			next := request(generation, "last-route-rejected-main")
			// Codex may retain a response header from the previous request.
			next.Header.Set(turnstate.Header, main.Value)
			e.ServeHTTP(second, next)
			if fallback == "strict" && (second.Code != 503 || calls != 1) {
				t.Fatal("strict mode sent request after main invalidation")
			}
			if fallback == "passthrough" && (second.Code != 200 || calls != 2) {
				t.Fatal("passthrough did not forward once without the invalid main")
			}
		})
	}
}

func TestMissingResponseStateIsUnknownAndDoesNotTriggerProbe(t *testing.T) {
	calls := 0
	e, _ := testEngine(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { calls++; io.WriteString(w, "the original answer") }))
	s, _ := e.borrow(request(generation, "check-missing-header").Header)
	token, _ := turnstate.Parse(fakeToken(10, 201))
	s.state.Offer(token, 0, time.Now())
	release(s)
	w := httptest.NewRecorder()
	e.ServeHTTP(w, request(generation, "check-missing-header"))
	if calls != 1 || w.Code != 200 || w.Body.String() != "the original answer" {
		t.Fatal("missing header caused extra request or lost answer")
	}
	if s.lastStateCheck.Result != "header_missing" {
		t.Fatalf("missing header falsely accepted: %+v", s.lastStateCheck)
	}
	if _, ok := s.state.Acquire(time.Now()); !ok {
		t.Fatal("absence alone invalidated a usable state")
	}
}

func TestResponse312SwitchesStateAndSourceTogether(t *testing.T) {
	for _, mode := range []string{"state", "fixed", "random"} {
		t.Run(mode, func(t *testing.T) {
			firstCalls, secondCalls := 0, 0
			e, _ := testEngine(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				firstCalls++
				if r.Header.Get(turnstate.Header) != fakeToken(10, 202) {
					t.Error("wrong state on first source")
				}
				complete(w, fakeToken(11, 204))
			}))
			second := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				secondCalls++
				if r.Header.Get(turnstate.Header) != fakeToken(10, 203) {
					t.Error("standby was sent through wrong source")
				}
				complete(w, fakeToken(10, 203))
			}))
			defer second.Close()
			e.routes = append(e.routes, proxyroute.Route{ID: "backup-node", DisplayName: "Backup node", Transport: &http.Transport{DialContext: func(ctx context.Context, network, address string) (net.Conn, error) {
				return (&net.Dialer{}).DialContext(ctx, network, strings.TrimPrefix(second.URL, "http://"))
			}}})
			e.config.EgressMode = mode
			e.config.EgressRoute = "backup-node"
			s, _ := e.borrow(request(generation, "check-state-route").Header)
			a, _ := turnstate.Parse(fakeToken(10, 202))
			b, _ := turnstate.Parse(fakeToken(10, 203))
			s.state.Offer(a, 0, time.Now())
			s.state.Offer(b, 1, time.Now())
			release(s)
			w := httptest.NewRecorder()
			e.ServeHTTP(w, request(generation, "check-state-route"))
			if w.Code != 200 || firstCalls != 1 || secondCalls != 0 || !strings.Contains(w.Body.String(), "response.completed") {
				t.Fatal("answer replayed or state source overridden")
			}
			if s.lastStateCheck.Result != "header_rejected" {
				t.Fatal("312 not recorded")
			}
			active, ok := s.state.Acquire(time.Now())
			if !ok || active.Route != 1 || active.Token.Value != b.Value {
				t.Fatal("standby not promoted")
			}
			w = httptest.NewRecorder()
			e.ServeHTTP(w, request(generation, "check-state-route"))
			if w.Code != 200 || firstCalls != 1 || secondCalls != 1 || s.lastStateCheck.Result != "header_accepted" {
				t.Fatal("next request did not follow standby source")
			}
		})
	}
}
