package gateway

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/gylive/ccodex-sleep-state/internal/settings"
)

// Seed only tests whose scenario requires a specific first/second transport.
// Random coverage tests below deliberately use the production shuffle.
func setTestProbeOrder(e *Engine, s *session, indices ...int) {
	e.collection.mu.Lock()
	defer e.collection.mu.Unlock()
	if e.collection.doc.Rotations == nil {
		e.collection.doc.Rotations = map[string]probeRotation{}
	}
	r := probeRotation{Cycle: 1, Visited: map[string]bool{}, UpdatedAt: time.Now()}
	for _, i := range indices {
		r.Order = append(r.Order, e.routes[i].ID)
	}
	e.collection.doc.Rotations[s.backupKey] = r
}

func clearTestBatchCooldown(e *Engine, s *session) {
	s.nextProbe = time.Time{}
	e.collection.mu.Lock()
	x := e.collection.doc.Searches[s.backupKey]
	x.Until = time.Time{}
	e.collection.doc.Searches[s.backupKey] = x
	_ = e.collection.write()
	e.collection.mu.Unlock()
}

func TestRandomRotationCoversAllBeforeRepeatingAcrossBatchesAndRestarts(t *testing.T) {
	e, logs := testEngine(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { complete(w, fakeToken(12, 201)) }))
	e.config.Collection = settings.DefaultCollection()
	e.config.Collection.HourlyBudget = 100
	e.config.MaxProbes = 2
	for i := 1; i < 9; i++ {
		r := e.routes[0]
		r.ID = fmt.Sprintf("node-%d", i)
		e.routes = append(e.routes, r)
	}
	s, _ := e.borrow(request(generation, "rotation-rounds").Header)
	defer release(s)
	path := filepath.Join(t.TempDir(), "budget.json")
	b, err := OpenCollectionBudget(path)
	if err != nil {
		t.Fatal(err)
	}
	e.collection = b
	for batch := 0; batch < 15; batch++ {
		e.refresh(context.Background(), s, true)
		clearTestBatchCooldown(e, s)
		// A real reload must continue the same random deck instead of reshuffling.
		b, err = OpenCollectionBudget(path)
		if err != nil {
			t.Fatal(err)
		}
		e.collection = b
	}
	var ids []string
	for _, line := range strings.Split(logs.String(), "\n") {
		var event struct {
			Message string `json:"msg"`
			Route   string `json:"route"`
		}
		_ = json.Unmarshal([]byte(line), &event)
		if event.Message == "probe_started" {
			ids = append(ids, event.Route)
		}
	}
	if len(ids) != 27 {
		t.Fatalf("wrong bounded batch count: %d", len(ids))
	}
	for round := 0; round < 3; round++ {
		seen := map[string]bool{}
		for _, id := range ids[round*9 : (round+1)*9] {
			if seen[id] {
				t.Fatalf("repeat before full cycle: %v", ids)
			}
			seen[id] = true
		}
		if len(seen) != len(e.routes) {
			t.Fatal("node skipped")
		}
	}
	status := e.collection.Status(s.backupKey, time.Now(), e.config.Collection).Rotation
	if status.Cycle != 3 || status.Completed != 9 || status.Total != 9 {
		t.Fatalf("wrong progress: %+v", status)
	}
	t.Logf("three verified random cycles: %v", ids)
}

func TestRotationWaitsForUnvisitedPausedNodeAndIncludesNewNodes(t *testing.T) {
	b, _ := OpenCollectionBudget("")
	ids := []string{"a", "b", "c"}
	now := time.Now()
	ready := map[string]bool{"a": true, "b": true}
	for i := 0; i < 2; i++ {
		id := b.rotationCandidate("s", ids, ready, true, now)
		if id == "" || id == "c" {
			t.Fatal("wrong ready candidate")
		}
		_ = b.recordRotationAttempt("s", id, now)
	}
	if id := b.rotationCandidate("s", ids, ready, true, now); id != "" {
		t.Fatal("repeated a visited node while c is pending")
	}
	ids = append(ids, "d")
	ready["d"] = true
	if id := b.rotationCandidate("s", ids, ready, true, now); id != "d" {
		t.Fatal("new node did not join unfinished cycle")
	}
	_ = b.recordRotationAttempt("s", "d", now)
	ready["c"] = true
	if id := b.rotationCandidate("s", ids, ready, true, now); id != "c" {
		t.Fatal("recovered pending node skipped")
	}
	_ = b.recordRotationAttempt("s", "c", now)
	if id := b.rotationCandidate("s", ids, ready, false, now); id != "" {
		t.Fatal("crossed cycle boundary within batch")
	}
	if id := b.rotationCandidate("s", ids, ready, true, now); id == "" || b.doc.Rotations["s"].Cycle != 2 {
		t.Fatal("second cycle did not start")
	}
}

func TestBlockedBudgetDoesNotConsumeRandomCandidate(t *testing.T) {
	e, _ := testEngine(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { complete(w, fakeToken(12, 202)) }))
	e.config.Collection = settings.DefaultCollection()
	e.config.Collection.HourlyBudget = 1
	s, _ := e.borrow(request(generation, "rotation-blocked").Header)
	defer release(s)
	e.collection.doc.History = []time.Time{time.Now()}
	e.refresh(context.Background(), s, true)
	if got := e.collection.Status(s.backupKey, time.Now(), e.config.Collection).Rotation; got.Completed != 0 {
		t.Fatalf("blocked request consumed node: %+v", got)
	}
	e.collection.doc.History = nil
	s.nextProbe = time.Time{}
	e.refresh(context.Background(), s, true)
	if got := e.collection.Status(s.backupKey, time.Now(), e.config.Collection).Rotation; got.Completed != 1 || got.Cycle != 1 {
		t.Fatalf("pending node was lost: %+v", got)
	}
}

func TestSuccessfulAcquisitionKeepsRemainingCycleAndFixedProbeDoesNotConsumeIt(t *testing.T) {
	e, _ := testEngine(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { complete(w, fakeToken(10, 203)) }))
	other := e.routes[0]
	other.ID = "other"
	e.routes = append(e.routes, other)
	s, _ := e.borrow(request(generation, "rotation-success").Header)
	defer release(s)
	e.refresh(context.Background(), s, true)
	r := e.collection.doc.Rotations[s.backupKey]
	if status := rotationStatus(r); status.Completed != 1 {
		t.Fatal("success reset cycle")
	}
	clearTestBatchCooldown(e, s)
	e.config.PinnedRoute = "other"
	e.refresh(context.Background(), s, true)
	if status := rotationStatus(e.collection.doc.Rotations[s.backupKey]); status.Completed != 1 {
		t.Fatal("fixed node consumed automatic deck")
	}
	e.config.PinnedRoute = ""
	next := e.nextProbeInRotation(s, map[int]bool{}, true, time.Now())
	if next < 0 || r.Visited[e.routes[next].ID] {
		t.Fatal("success skipped remaining node")
	}
}
