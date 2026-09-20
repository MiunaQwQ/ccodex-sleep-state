package gateway

import (
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/gylive/ccodex-sleep-state/internal/turnstate"
)

func TestDiscardKeepsSessionIsolationBudgetAndPersistsUnderConcurrency(t *testing.T) {
	e, _ := testEngine(t, http.HandlerFunc(func(http.ResponseWriter, *http.Request) { t.Error("discard sent a request") }))
	e.config.PoolEnabled = true
	e.log = slog.New(slog.NewTextHandler(io.Discard, nil))
	path := filepath.Join(t.TempDir(), "state-backup.json")
	backup, err := turnstate.OpenBackup(path)
	if err != nil {
		t.Fatal(err)
	}
	e.SetStateBackup(backup)
	s, _ := e.borrow(request(generation, "discard-session").Header)
	defer release(s)
	other, _ := e.borrow(request(generation, "other-session").Header)
	defer release(other)
	a, _ := turnstate.Parse(fakeToken(10, 201))
	b, _ := turnstate.Parse(fakeToken(10, 202))
	for _, session := range []*session{s, other} {
		session.state.Offer(a, 0, time.Now())
		session.state.Offer(b, 0, time.Now())
	}
	until := time.Now().Add(time.Hour)
	s.nextProbe, s.upstreamPause = until, until
	before, _ := json.Marshal(e.collection.doc)
	var wg sync.WaitGroup
	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 10; j++ {
				e.persistState(s)
				s.state.Offer(a, 0, time.Now())
			}
		}()
	}
	result, err := e.DiscardState(s.id, a.Fingerprint)
	if err != nil || result.ActiveID != b.Fingerprint {
		t.Fatal(result, err)
	}
	wg.Wait()
	e.persistState(s)
	after, _ := json.Marshal(e.collection.doc)
	if string(before) != string(after) || !s.nextProbe.Equal(until) || !s.upstreamPause.Equal(until) {
		t.Fatal("discard reset automatic restrictions")
	}
	if active, _ := other.state.Acquire(time.Now()); active.Token != a {
		t.Fatal("discard crossed session boundary")
	}
	reloaded, err := turnstate.OpenBackup(path)
	if err != nil {
		t.Fatal(err)
	}
	saved, ok := reloaded.Load(s.backupKey)
	if !ok || saved.Active == nil || saved.Active.Token != b.Value || len(saved.Discarded) != 1 {
		t.Fatal("concurrent save resurrected old main")
	}
}

func TestDiscardDuringInflightRequestPreservesReplyAndSuccessor(t *testing.T) {
	started, finish := make(chan struct{}), make(chan struct{})
	var once sync.Once
	releaseRequest := func() { once.Do(func() { close(finish) }) }
	defer releaseRequest()
	a, _ := turnstate.Parse(fakeToken(10, 203))
	b, _ := turnstate.Parse(fakeToken(10, 204))
	e, _ := testEngine(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get(turnstate.Header) != a.Value {
			t.Error("request snapshot changed")
		}
		close(started)
		<-finish
		complete(w, fakeToken(11, 205))
	}))
	e.config.PoolEnabled = true
	s, _ := e.borrow(request(generation, "discard-inflight").Header)
	defer release(s)
	s.state.Offer(a, 0, time.Now())
	s.state.Offer(b, 0, time.Now())
	done := make(chan *httptest.ResponseRecorder, 1)
	go func() {
		w := httptest.NewRecorder()
		e.ServeHTTP(w, request(generation, "discard-inflight"))
		done <- w
	}()
	<-started
	if _, err := e.DiscardState(s.id, a.Fingerprint); err != nil {
		t.Fatal(err)
	}
	releaseRequest()
	select {
	case w := <-done:
		if w.Code != 200 {
			t.Fatal(w.Code)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("discard interrupted request")
	}
	if active, _ := s.state.Acquire(time.Now()); active.Token != b {
		t.Fatal("late response changed successor")
	}
}
