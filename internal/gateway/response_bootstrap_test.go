package gateway

import (
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gylive/ccodex-sleep-state/internal/turnstate"
)

func TestPassthroughBootstrapsOnlyMissingMainWithoutProbeBudget(t *testing.T) {
	calls := 0
	e, _ := testEngine(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		complete(w, fakeToken(10, byte(100+calls)))
	}))
	e.running.Store(true) // Production dispatches collection separately.
	e.config.StateFallback = "passthrough"
	e.config.PoolEnabled = true
	e.config.EgressMode, e.config.EgressRoute = "fixed", "second-node"
	route := e.routes[0]
	route.ID = "second-node"
	e.routes = append(e.routes, route)
	s, _ := e.borrow(request(generation, "bootstrap-chat").Header)
	release(s)
	s.nextProbe = time.Now().Add(time.Hour)
	e.collection.EndRound(s.backupKey, time.Now(), time.Hour)
	before := e.collection.Status(s.backupKey, time.Now(), e.config.Collection)
	for i := 0; i < 2; i++ {
		w := httptest.NewRecorder()
		e.ServeHTTP(w, request(generation, "bootstrap-chat"))
		if w.Code != 200 || !strings.Contains(w.Body.String(), "response.completed") {
			t.Fatal("normal reply was changed")
		}
		active, ok := s.state.Acquire(time.Now())
		if !ok || active.Route != 1 || active.Token.Value != fakeToken(10, 101) {
			t.Fatal("missing main not acquired, source lost, or existing main replaced")
		}
		if s.state.Status(time.Now()).Standby != 0 {
			t.Fatal("normal reply added a standby")
		}
	}
	after := e.collection.Status(s.backupKey, time.Now(), e.config.Collection)
	if calls != 2 || after.HourlyUsed != before.HourlyUsed || !after.NextAt.Equal(before.NextAt) {
		t.Fatal("normal reply sent probes or reset probe cooldown")
	}
}

func TestPassthroughDoesNotCollectFailedOrUnverifiableReplies(t *testing.T) {
	for _, tc := range []struct{ name, body string }{
		{"truncated", "data: {\"type\":\"response.created\"}\n\n"},
		{"failed", "data: {\"type\":\"response.failed\"}\n\ndata: {\"type\":\"response.completed\"}\n\n"},
		{"not SSE", "ordinary answer"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			e, _ := testEngine(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set(turnstate.Header, fakeToken(10, 111))
				w.Header().Set("Content-Type", "text/event-stream")
				io.WriteString(w, tc.body)
			}))
			e.running.Store(true)
			e.config.StateFallback = "passthrough"
			w := httptest.NewRecorder()
			e.ServeHTTP(w, request(generation, "unverifiable-chat"))
			if w.Body.String() != tc.body {
				t.Fatal("response changed")
			}
			for _, s := range e.sessions {
				if _, ok := s.state.Acquire(time.Now()); ok {
					t.Fatal("unconfirmed response collected")
				}
			}
		})
	}
}

func TestLatePassthroughDoesNotReplaceConcurrentMainOrAddStandby(t *testing.T) {
	var s *session
	e, _ := testEngine(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		other, _ := turnstate.Parse(fakeToken(10, 120))
		s.state.Offer(other, 0, time.Now())
		complete(w, fakeToken(10, 121))
	}))
	e.running.Store(true)
	e.config.StateFallback = "passthrough"
	s, _ = e.borrow(request(generation, "late-chat").Header)
	release(s)
	e.ServeHTTP(httptest.NewRecorder(), request(generation, "late-chat"))
	main, ok := s.state.Acquire(time.Now())
	if !ok || main.Token.Value != fakeToken(10, 120) || s.state.Status(time.Now()).Standby != 0 || s.lastStateCheck.Bootstrap != "not_needed" {
		t.Fatal("late reply overwrote or duplicated main")
	}
}

func TestBootstrapStreamIsBoundedAndForwardsEveryByte(t *testing.T) {
	for _, oversized := range []bool{false, true} {
		delta := "data: {\"type\":\"response.output_text.delta\",\"delta\":\"" + strings.Repeat("x", 1024) + "\"}\r\n\r\n"
		body := strings.Repeat(delta, 2048) + "event: response.completed\r\ndata: {}\r\n\r\n"
		if oversized {
			body = "data: " + strings.Repeat("x", (1<<20)+1) + "\n\n" + body
		}
		b := &bootstrapStream{ReadCloser: io.NopCloser(strings.NewReader(body))}
		out, err := io.ReadAll(b)
		if err != nil || string(out) != body || b.complete() == oversized || cap(b.event) > 2<<20 {
			t.Fatal("stream forwarding, completion or bound incorrect")
		}
	}
}

func TestLateBadPassthroughDoesNotDropNewerMain(t *testing.T) {
	var s *session
	e, _ := testEngine(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fresh, _ := turnstate.Parse(fakeToken(10, 124))
		s.state.Offer(fresh, 0, time.Now())
		complete(w, fakeToken(11, 125))
	}))
	e.running.Store(true)
	e.config.StateFallback = "passthrough"
	route := e.routes[0]
	route.ID = "another-node"
	e.routes = append(e.routes, route)
	s, _ = e.borrow(request(generation, "late-bad-chat").Header)
	release(s)
	e.ServeHTTP(httptest.NewRecorder(), request(generation, "late-bad-chat"))
	if _, ok := s.state.Acquire(time.Now()); !ok || len(s.nodePauses) != 0 {
		t.Fatal("late 312 discarded newer main")
	}
}

func TestCompressedOrMislabeledRepliesBootstrapFromActualCompletion(t *testing.T) {
	for _, encoding := range []string{"identity", "gzip", "zstd"} {
		for _, finished := range []bool{false, true} {
			t.Run(encoding+fmt.Sprint(finished), func(t *testing.T) {
				body := "data: {\"type\":\"response.created\"}\n\n"
				if finished {
					body += "data: {\"type\":\"response.completed\"}\n\n"
				}
				wire := []byte(body)
				if encoding != "identity" {
					wire = encodeRequest(t, encoding, wire)
				}
				e, _ := testEngine(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					w.Header().Set(turnstate.Header, fakeToken(10, 159))
					w.Header().Set("Content-Type", "application/octet-stream")
					w.Header().Set("Content-Encoding", encoding)
					w.Write(wire)
				}))
				e.running.Store(true)
				e.config.StateFallback = "passthrough"
				w := httptest.NewRecorder()
				e.ServeHTTP(w, request(generation, "compressed-bootstrap"))
				s, _ := e.borrow(request(generation, "compressed-bootstrap").Header)
				defer release(s)
				if w.Code != 200 || w.Body.String() != body || s.state.Status(time.Now()).Usable != finished {
					t.Fatalf("completion/forwarding mismatch code=%d usable=%v", w.Code, s.state.Status(time.Now()).Usable)
				}
				if finished && s.lastStateCheck.Bootstrap != "saved_active" {
					t.Fatal("state not reported as saved")
				}
			})
		}
	}
}

func TestCompletedJSONBootstrapAndFailedJSONRejection(t *testing.T) {
	for _, status := range []string{"completed", "failed", "in_progress"} {
		t.Run(status, func(t *testing.T) {
			body := `{"object":"response","status":"` + status + `","error":null,"output":[]}`
			e, _ := testEngine(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set(turnstate.Header, fakeToken(10, 161))
				w.Header().Set("Content-Type", "application/json")
				io.WriteString(w, body)
			}))
			e.running.Store(true)
			e.config.StateFallback = "passthrough"
			w := httptest.NewRecorder()
			e.ServeHTTP(w, request(generation, "json-first-state"))
			s, _ := e.borrow(request(generation, "json-first-state").Header)
			defer release(s)
			if w.Body.String() != body || s.state.Status(time.Now()).Usable != (status == "completed") {
				t.Fatal("JSON completion/forwarding incorrect")
			}
		})
	}
}
