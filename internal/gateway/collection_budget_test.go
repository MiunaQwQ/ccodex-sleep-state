package gateway

import (
	"path/filepath"
	"testing"
	"time"

	"github.com/gylive/ccodex-sleep-state/internal/settings"
)

func TestZeroStartBackoffStopsAtSixAndSurvivesRestart(t *testing.T) {
	p := settings.DefaultCollection()
	p.Cadence, p.FailureIntervalSeconds, p.FailureMaxIntervalSeconds, p.SearchBudget, p.SearchPauseSeconds = "backoff", 0, 300, 6, 900
	if err := p.Validate(); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(t.TempDir(), "budget.json")
	b, err := OpenCollectionBudget(path)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Date(2026, 9, 19, 12, 0, 0, 0, time.UTC)
	for i, seconds := range []int{0, 30, 60, 120, 240, 900} {
		if until, reason := b.Reserve("sol", now, p); !until.IsZero() {
			t.Fatalf("attempt %d blocked: %s %v", i+1, reason, until)
		}
		b.Complete("sol", now, false, p, "shape_mismatch", "node-a")
		status := b.Status("sol", now, p)
		if status.Failures != i+1 || status.HourlyUsed != i+1 {
			t.Fatalf("wrong counters: %+v", status)
		}
		if seconds > 0 {
			if !status.NextAt.Equal(now.Add(time.Duration(seconds) * time.Second)) {
				t.Fatalf("attempt %d next=%v", i+1, status.NextAt)
			}
			if until, _ := b.Reserve("sol", now.Add(time.Duration(seconds)*time.Second-time.Nanosecond), p); until.IsZero() {
				t.Fatalf("attempt %d backoff bypassed", i+1)
			}
		}
		now = now.Add(time.Duration(seconds) * time.Second)
	}
	b, err = OpenCollectionBudget(path)
	if err != nil {
		t.Fatal(err)
	}
	if status := b.Status("sol", now.Add(-time.Second), p); status.HourlyUsed != 6 || status.Failures != 6 || status.Reason != "search_budget" {
		t.Fatalf("restart erased budget: %+v", status)
	}
	if until, reason := b.Reserve("sol", now, p); !until.IsZero() {
		t.Fatalf("pause should end: %s %v", reason, until)
	}
	if status := b.Status("sol", now, p); status.Failures != 1 || status.HourlyUsed != 7 {
		t.Fatalf("next round should reset only consecutive failures: %+v", status)
	}
	b.Complete("sol", now, true, p)
	if status := b.Status("sol", now, p); status.Failures != 0 || status.LastFailureAt.IsZero() {
		t.Fatal("success should retain last failure timestamp")
	}
}

func TestLowerFailureBudgetAppliesToExistingRecords(t *testing.T) {
	b, _ := OpenCollectionBudget("")
	now := time.Now()
	b.doc.Searches["sol"] = collectionSearch{Failures: 7, LastFailureAt: now, Until: now.Add(time.Minute), Reason: "failure_interval"}
	p := settings.DefaultCollection()
	p.Cadence, p.SearchBudget, p.SearchPauseSeconds = "backoff", 6, 900
	until, reason := b.Reserve("sol", now.Add(2*time.Minute), p)
	if !until.Equal(now.Add(15*time.Minute)) || reason != "search_budget" || len(b.doc.History) != 0 {
		t.Fatalf("new budget dispatched extra probe: %s %v", reason, until)
	}
	if until, _ = b.Reserve("sol", now.Add(15*time.Minute), p); !until.IsZero() {
		t.Fatal("pause did not expire")
	}
	if b.doc.Searches["sol"].Failures != 1 {
		t.Fatal("resumed round did not reset count")
	}
}

func TestHourlyBudgetSpansModels(t *testing.T) {
	p := settings.DefaultCollection()
	p.HourlyBudget = 2
	b, _ := OpenCollectionBudget("")
	now := time.Now()
	for _, key := range []string{"astra", "sol"} {
		if until, _ := b.Reserve(key, now, p); !until.IsZero() {
			t.Fatal("early hourly limit")
		}
		b.Complete(key, now, true, p)
	}
	if until, reason := b.Reserve("terra", now, p); until.IsZero() || reason != "hourly_budget" {
		t.Fatal("third model bypassed program budget")
	}
	if until, _ := b.Reserve("terra", now.Add(time.Hour), p); !until.IsZero() {
		t.Fatal("rolling budget did not expire")
	}
}
