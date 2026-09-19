package gateway

import (
	"encoding/json"
	"errors"
	"os"
	"sort"
	"sync"
	"time"

	"github.com/gylive/ccodex-sleep-state/internal/fsutil"
	"github.com/gylive/ccodex-sleep-state/internal/settings"
)

type UseFailure struct {
	At      time.Time `json:"at"`
	Reason  string    `json:"reason"`
	Route   string    `json:"route"`
	StateID string    `json:"state_id"`
}

type collectionSearch struct {
	LastUseFailure    UseFailure `json:"last_use_failure"`
	LastFailureAt     time.Time  `json:"last_failure_at"`
	LastFailureReason string     `json:"last_failure_reason,omitempty"`
	LastFailureRoute  string     `json:"last_failure_route,omitempty"`
	Failures          int        `json:"failures"`
	Until             time.Time  `json:"until"`
	Reason            string     `json:"reason"`
}
type collectionDocument struct {
	History  []time.Time                 `json:"history"`
	Searches map[string]collectionSearch `json:"searches"`
}

// CollectionBudget is shared across sessions and route/config generations.
// Reserve persists before dispatch, so restarts cannot replenish the budget.
type CollectionBudget struct {
	mu   sync.Mutex
	path string
	doc  collectionDocument
	err  error
}

func OpenCollectionBudget(path string) (*CollectionBudget, error) {
	b := &CollectionBudget{path: path, doc: collectionDocument{Searches: map[string]collectionSearch{}}}
	if path == "" {
		return b, nil
	}
	if err := fsutil.RefuseLink(path); err != nil {
		return nil, err
	}
	data, err := os.ReadFile(path)
	if os.IsNotExist(err) {
		return b, nil
	}
	if err != nil {
		return nil, errors.New("无法读取采集预算记录")
	}
	if len(data) > 1<<20 || json.Unmarshal(data, &b.doc) != nil || b.doc.Searches == nil || len(b.doc.Searches) > 1024 || len(b.doc.History) > 1000 {
		return nil, errors.New("采集预算记录损坏，请保留文件后修复")
	}
	sort.Slice(b.doc.History, func(i, j int) bool { return b.doc.History[i].Before(b.doc.History[j]) })
	return b, nil
}

func (b *CollectionBudget) prune(now time.Time) {
	history := b.doc.History[:0]
	for _, at := range b.doc.History {
		if at.After(now.Add(-time.Hour)) {
			history = append(history, at)
		}
	}
	b.doc.History = history
	for key, s := range b.doc.Searches {
		if s.Until.Before(now.Add(-7*24*time.Hour)) && s.LastFailureAt.Before(now.Add(-7*24*time.Hour)) && s.LastUseFailure.At.Before(now.Add(-7*24*time.Hour)) {
			delete(b.doc.Searches, key)
		}
	}
}

func (b *CollectionBudget) write() error {
	if b.path == "" {
		return nil
	}
	if err := fsutil.RefuseLink(b.path); err != nil {
		b.err = err
		return err
	}
	data, _ := json.Marshal(b.doc)
	b.err = fsutil.Write(b.path, data)
	return b.err
}

func (b *CollectionBudget) gate(key string, now time.Time, p settings.CollectionPolicy) (time.Time, string) {
	s := b.doc.Searches[key]
	until, reason := s.Until, s.Reason
	if !now.Before(until) {
		until, reason = time.Time{}, ""
	}
	if len(b.doc.History) >= p.HourlyBudget {
		at := b.doc.History[len(b.doc.History)-p.HourlyBudget].Add(time.Hour)
		if at.After(until) {
			until, reason = at, "hourly_budget"
		}
	}
	if b.err != nil {
		return now.Add(time.Minute), "budget_storage_error"
	}
	return until, reason
}

func (b *CollectionBudget) Reserve(key string, now time.Time, p settings.CollectionPolicy) (time.Time, string) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.prune(now)
	if until, reason := b.gate(key, now, p); !until.IsZero() {
		return until, reason
	}
	s := b.doc.Searches[key]
	if s.Reason == "search_budget" {
		s.Failures, s.Reason, s.Until = 0, "", time.Time{}
	}
	// Conservatively count a dispatched probe as unsuccessful until completion.
	// A crash cannot erase an unfinished attempt or reset consecutive failures.
	s.Failures++
	delay := time.Duration(p.FailureIntervalSeconds) * time.Second
	for i := 1; i < s.Failures && delay < time.Duration(p.FailureMaxIntervalSeconds)*time.Second; i++ {
		delay *= 2
	}
	delay = min(delay, time.Duration(p.FailureMaxIntervalSeconds)*time.Second)
	s.Until, s.Reason = now.Add(delay), "failure_interval"
	if s.Failures >= p.SearchBudget {
		s.Until, s.Reason = now.Add(time.Duration(p.SearchPauseSeconds)*time.Second), "search_budget"
	}
	b.doc.Searches[key] = s
	b.doc.History = append(b.doc.History, now)
	if b.write() != nil {
		return now.Add(time.Minute), "budget_storage_error"
	}
	return time.Time{}, ""
}

func (b *CollectionBudget) Complete(key string, now time.Time, accepted bool, p settings.CollectionPolicy, detail ...string) {
	b.mu.Lock()
	defer b.mu.Unlock()
	s := b.doc.Searches[key]
	if accepted {
		s.Failures, s.Reason, s.Until = 0, "", time.Time{}
	} else {
		s.LastFailureAt = now
		if len(detail) > 0 {
			s.LastFailureReason = detail[0]
		}
		if len(detail) > 1 {
			s.LastFailureRoute = detail[1]
		}
		delay := time.Duration(p.FailureIntervalSeconds) * time.Second
		for i := 1; i < s.Failures && delay < time.Duration(p.FailureMaxIntervalSeconds)*time.Second; i++ {
			delay *= 2
		}
		delay = min(delay, time.Duration(p.FailureMaxIntervalSeconds)*time.Second)
		if s.Reason == "search_budget" {
			delay = time.Duration(p.SearchPauseSeconds) * time.Second
		}
		s.Until = now.Add(delay)
	}
	b.doc.Searches[key] = s
	_ = b.write()
}

type CollectionStatus struct {
	LastUseFailure        UseFailure `json:"last_use_failure"`
	LastFailureAt         time.Time  `json:"last_failure_at"`
	LastFailureAgoSeconds int        `json:"last_failure_ago_seconds"`
	LastFailureReason     string     `json:"last_failure_reason,omitempty"`
	LastFailureRoute      string     `json:"last_failure_route,omitempty"`
	HourlyUsed            int        `json:"hourly_used"`
	HourlyBudget          int        `json:"hourly_budget"`
	Failures              int        `json:"failures"`
	SearchBudget          int        `json:"search_budget"`
	NextAt                time.Time  `json:"next_at"`
	Reason                string     `json:"reason"`
	WaitSeconds           int        `json:"wait_seconds"`
	Idle                  bool       `json:"idle"`
}

func (b *CollectionBudget) Status(key string, now time.Time, p settings.CollectionPolicy) CollectionStatus {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.prune(now)
	at, reason := b.gate(key, now, p)
	s := b.doc.Searches[key]
	age := 0
	if !s.LastFailureAt.IsZero() {
		age = max(0, int(now.Sub(s.LastFailureAt).Seconds()))
	}
	return CollectionStatus{LastUseFailure: s.LastUseFailure, LastFailureAt: s.LastFailureAt, LastFailureAgoSeconds: age, LastFailureReason: s.LastFailureReason, LastFailureRoute: s.LastFailureRoute, HourlyUsed: len(b.doc.History), HourlyBudget: p.HourlyBudget, Failures: s.Failures, SearchBudget: p.SearchBudget, NextAt: at, Reason: reason, WaitSeconds: max(0, int(at.Sub(now).Seconds())+1)}
}

// A failed real generation is recorded separately and never consumes probe budget.
func (b *CollectionBudget) RecordUseFailure(key string, f UseFailure) {
	b.mu.Lock()
	defer b.mu.Unlock()
	s := b.doc.Searches[key]
	s.LastUseFailure = f
	b.doc.Searches[key] = s
	_ = b.write()
}
