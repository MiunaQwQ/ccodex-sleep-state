package gateway

import (
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gylive/ccodex-sleep-state/internal/settings"
	"github.com/gylive/ccodex-sleep-state/internal/turnstate"
)

func TestStandbyCollectionDuringActiveGeneration(t *testing.T) {
	for _, tc := range []struct {
		name         string
		probeStatus  int
		hourlyBudget int
		wantProbes   int32
		wantStandby  int
	}{
		{"next node succeeds", 200, 30, 2, 1},
		{"429 stops round", 429, 30, 1, 0},
		{"budget stops round", 200, 1, 1, 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			started, finish := make(chan struct{}), make(chan struct{})
			var once sync.Once
			releaseReply := func() { once.Do(func() { close(finish) }) }
			var probes, activeProbes, peakProbes atomic.Int32
			mainToken := fakeToken(10, 171)
			e, _ := testEngine(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				body, _ := io.ReadAll(r.Body)
				if string(body) == generation {
					if r.Header.Get(turnstate.Header) != mainToken {
						t.Error("generation lost its bound main state")
					}
					w.Header().Set("Content-Type", "text/event-stream")
					fmt.Fprint(w, "data: {\"type\":\"response.created\"}\n\n")
					w.(http.Flusher).Flush()
					close(started)
					select {
					case <-finish:
					case <-r.Context().Done():
					}
					fmt.Fprint(w, "data: {\"type\":\"response.completed\"}\n\n")
					return
				}
				if r.Header.Get(turnstate.Header) != "" || !strings.Contains(string(body), "Reply with OK.") {
					t.Error("probe inherited main state or did not send its own message")
				}
				active := activeProbes.Add(1)
				defer activeProbes.Add(-1)
				for peak := peakProbes.Load(); active > peak; peak = peakProbes.Load() {
					if peakProbes.CompareAndSwap(peak, active) {
						break
					}
				}
				n := probes.Add(1)
				if tc.probeStatus == 429 {
					w.Header().Set("Retry-After", "300")
					w.WriteHeader(429)
					return
				}
				blocks := 10
				if n == 1 {
					blocks = 11
				}
				complete(w, fakeToken(blocks, byte(171+n)))
			}))
			t.Cleanup(releaseReply)
			e.config.PoolEnabled = true
			e.config.Collection = settings.DefaultCollection()
			e.config.Collection.StandbyTarget = 1
			e.config.Collection.HourlyBudget = tc.hourlyBudget
			e.SetRefreshMode("on_demand")
			for i := 1; i < 3; i++ {
				r := e.routes[0]
				r.ID = fmt.Sprintf("candidate-%d", i)
				e.routes = append(e.routes, r)
			}
			s, err := e.borrow(request(generation, "standby-during-generation").Header)
			if err != nil {
				t.Fatal(err)
			}
			token, _ := turnstate.Parse(mainToken)
			s.state.Offer(token, 0, time.Now())
			setTestProbeOrder(e, s, 1, 2, 0)
			release(s)
			before, _ := s.state.Acquire(time.Now())
			replyDone := make(chan *httptest.ResponseRecorder, 1)
			go func() {
				w := httptest.NewRecorder()
				e.ServeHTTP(w, request(generation, "standby-during-generation"))
				replyDone <- w
			}()
			select {
			case <-started:
			case <-time.After(2 * time.Second):
				t.Fatal("generation never reached upstream")
			}
			// Even a generation lasting longer than the idle cutoff stays active.
			s.mu.Lock()
			s.lastUsed = time.Now().Add(-idleLifetime - time.Minute)
			s.mu.Unlock()
			runCollector(t, e)
			await(t, func() bool {
				s.mu.Lock()
				defer s.mu.Unlock()
				return probes.Load() >= tc.wantProbes && s.probing == nil && !s.queued
			})
			if e.foreground.Load() != 1 {
				t.Fatal("generation ended before collection finished")
			}
			if probes.Load() != tc.wantProbes || peakProbes.Load() != 1 {
				t.Fatalf("wrong bounded probe count: calls=%d peak=%d", probes.Load(), peakProbes.Load())
			}
			if got := s.state.Status(time.Now()).Standby; got != tc.wantStandby {
				t.Fatalf("standby=%d want=%d", got, tc.wantStandby)
			}
			after, _ := s.state.Acquire(time.Now())
			if before != after {
				t.Fatal("background collection changed the live main state or route")
			}
			s.mu.Lock()
			status := e.collectionStatus(s, time.Now())
			paused := time.Until(s.upstreamPause)
			s.mu.Unlock()
			if status.Idle || status.Reason == "foreground_busy" {
				t.Fatal("active generation was reported as blocking collection")
			}
			if tc.probeStatus == 429 && paused < 299*time.Second {
				t.Fatal("probe ignored upstream Retry-After")
			}
			if tc.wantStandby == 1 && e.cards(s, time.Now())[1].RouteID != "candidate-2" {
				t.Fatal("standby lost its acquisition route")
			}
			if code, _ := s.rejection(); code != 0 {
				t.Fatal("probe failure blocked the live generation")
			}
			releaseReply()
			select {
			case w := <-replyDone:
				if w.Code != 200 || !strings.Contains(w.Body.String(), "response.completed") {
					t.Fatal("generation was interrupted")
				}
			case <-time.After(2 * time.Second):
				t.Fatal("generation failed to finish")
			}
		})
	}
}
