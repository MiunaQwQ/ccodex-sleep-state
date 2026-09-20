package gateway

import (
	"context"
	"net"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"testing"
	"time"

	"github.com/gylive/ccodex-sleep-state/internal/settings"
	"github.com/gylive/ccodex-sleep-state/internal/turnstate"
)

func TestNetworkFailureKeepsCardsBindingsAndBackup(t *testing.T) {
	e, _ := testEngine(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get(turnstate.Header) != fakeToken(10, 151) {
			t.Error("state changed after connection failure")
		}
		complete(w, fakeToken(10, 151))
	}))
	e.config.PoolEnabled = true
	backup, err := turnstate.OpenBackup(filepath.Join(t.TempDir(), "backup.json"))
	if err != nil {
		t.Fatal(err)
	}
	e.SetStateBackup(backup)
	s, _ := e.borrow(request(generation, "retain-network-card").Header)
	defer release(s)
	for _, marker := range []byte{151, 152} {
		token, _ := turnstate.Parse(fakeToken(10, marker))
		s.state.Offer(token, 0, time.Now())
	}
	e.persistState(s)
	before, _ := s.state.Acquire(time.Now())
	calls := 0
	e.routes[0].Transport = &http.Transport{DialContext: func(ctx context.Context, network, address string) (net.Conn, error) {
		calls++
		if calls == 1 {
			return nil, context.DeadlineExceeded
		}
		return (&net.Dialer{}).DialContext(ctx, network, address)
	}}
	first := httptest.NewRecorder()
	e.ServeHTTP(first, request(generation, "retain-network-card"))
	after, ok := s.state.Acquire(time.Now())
	if first.Code != 502 || !ok || before != after || s.state.Status(time.Now()).Standby != 1 {
		t.Fatal("timeout removed or promoted held cards")
	}
	if e.pool.Get(e.routes[0].ID).State != "failed" || e.hasProbeCandidate() {
		t.Fatal("failed node remained eligible for automatic collection")
	}
	restored, _ := e.borrow(request(generation, "retain-network-card").Header)
	release(restored)
	// Reload the private persisted card set with the route still marked failed.
	restored.state = turnstate.New(turnstate.Policy{Blocks: 10, TTL: time.Hour, Refresh: 20 * time.Minute})
	e.restoreState(restored)
	if got, ok := restored.state.Acquire(time.Now()); !ok || got.Token.Fingerprint != before.Token.Fingerprint || restored.state.Status(time.Now()).Standby != 1 {
		t.Fatal("restart discarded cards on network-failed route")
	}
	second := httptest.NewRecorder()
	e.ServeHTTP(second, request(generation, "retain-network-card"))
	if second.Code != 200 || calls != 2 {
		t.Fatalf("held card cannot use original source again: %d calls=%d", second.Code, calls)
	}
	if e.pool.Get(e.routes[0].ID).State != "failed" {
		t.Fatal("normal reply silently re-enabled automatic probes")
	}
	e.pool.Change([]string{e.routes[0].ID}, "disabled", "manual", false)
	e.failRoute(0) // A late timeout must not undo explicit manual disable.
	third := httptest.NewRecorder()
	e.ServeHTTP(third, request(generation, "retain-network-card"))
	if third.Code != 503 || calls != 2 {
		t.Fatal("held card bypassed manual disable")
	}
}

func TestServerFailureDoesNotInvalidateState(t *testing.T) {
	for _, code := range []int{500, 502, 503, 504} {
		t.Run(http.StatusText(code), func(t *testing.T) {
			e, _ := testEngine(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(code) }))
			s, _ := e.borrow(request(generation, "server-failure-card").Header)
			defer release(s)
			for _, marker := range []byte{154, 155} {
				token, _ := turnstate.Parse(fakeToken(10, marker))
				s.state.Offer(token, 0, time.Now())
			}
			before, _ := s.state.Acquire(time.Now())
			w := httptest.NewRecorder()
			e.ServeHTTP(w, request(generation, "server-failure-card"))
			after, ok := s.state.Acquire(time.Now())
			if w.Code != code || !ok || before != after || s.state.Status(time.Now()).Standby != 1 {
				t.Fatal("5xx removed card or promoted standby")
			}
		})
	}
}

func TestRandomManualProbeKeepsMainAndAutomaticSchedule(t *testing.T) {
	calls := 0
	e, _ := testEngine(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { calls++; complete(w, fakeToken(10, 157)) }))
	e.config.Collection = settings.DefaultCollection()
	e.config.PoolEnabled = true
	other := e.routes[0]
	other.ID = "another-node"
	e.routes = append(e.routes, other)
	e.config.PinnedRoute = e.routes[0].ID
	s, _ := e.borrow(request(generation, "random-once-card").Header)
	defer release(s)
	s.activated = true
	token, _ := turnstate.Parse(fakeToken(10, 156))
	s.state.Offer(token, 0, time.Now())
	before, _ := s.state.Acquire(time.Now())
	e.collection.EndRound(s.backupKey, time.Now(), 180*time.Second)
	s.nextProbe = time.Now().Add(180 * time.Second)
	e.refresh(context.Background(), s, false)
	if calls != 0 {
		t.Fatal("automatic probe skipped default wait")
	}
	if err := e.RetryRandomState(context.Background(), s.id); err != nil {
		t.Fatal(err)
	}
	after, _ := s.state.Acquire(time.Now())
	if calls != 1 || before != after || s.state.Status(time.Now()).Standby != 1 || s.lastProbeResult.Route != other.ID || e.config.PinnedRoute != e.routes[0].ID {
		t.Fatal("manual random changed main, pin or sent multiple probes")
	}
	if status := e.collection.Status(s.backupKey, time.Now(), e.config.Collection); status.HourlyUsed != 0 || status.WaitSeconds < 178 {
		t.Fatal("manual random erased budget or following wait")
	}
	e.config.Collection.HourlyBudget = 1
	s.upstreamPause = time.Now().Add(time.Minute)
	// The only alternative can be retried immediately, even though the
	// repeated synthetic token is rejected as a duplicate after dispatch.
	_ = e.RetryRandomState(context.Background(), s.id)
	if calls != 2 {
		t.Fatal("manual ticket was blocked by a timer or the previous node")
	}
}

func TestEmptyMainAlsoWaitsAfterBatchAndAcrossRestart(t *testing.T) {
	calls := 0
	e, _ := testEngine(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { calls++; complete(w, fakeToken(12, 158)) }))
	e.config.Collection = settings.DefaultCollection()
	s, _ := e.borrow(request(generation, "empty-main-wait").Header)
	defer release(s)
	path := filepath.Join(t.TempDir(), "budget.json")
	e.collection, _ = OpenCollectionBudget(path)
	e.refresh(context.Background(), s, true)
	e.collection, _ = OpenCollectionBudget(path)
	s.nextProbe = time.Time{}
	e.refresh(context.Background(), s, true)
	if calls != 1 || e.collectionStatus(s, time.Now()).WaitSeconds < 178 {
		t.Fatal("empty-main restart bypassed batch wait")
	}
}
