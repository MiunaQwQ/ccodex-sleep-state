// Package routepool tracks local node lifecycle, not public IP identity.
package routepool

import (
	"encoding/json"
	"errors"
	"os"
	"sync"
	"time"

	"github.com/gylive/ccodex-sleep-state/internal/fsutil"
)

type Entry struct {
	State    string    `json:"state"`
	Reason   string    `json:"reason,omitempty"`
	Attempts int       `json:"attempts"`
	Updated  time.Time `json:"updated"`
}
type Store struct {
	mu      sync.Mutex
	path    string
	entries map[string]Entry
	err     error
}

func Open(path string) (*Store, error) {
	s := &Store{path: path, entries: map[string]Entry{}}
	if path == "" {
		return s, nil
	}
	if err := fsutil.RefuseLink(path); err != nil {
		return nil, err
	}
	b, err := os.ReadFile(path)
	if os.IsNotExist(err) {
		return s, nil
	}
	if err != nil {
		return nil, errors.New("无法读取代理池记录")
	}
	if len(b) > 2<<20 || json.Unmarshal(b, &s.entries) != nil || s.entries == nil {
		return nil, errors.New("代理池记录损坏，已停止自动采集；请保留文件备份后修复")
	}
	for _, e := range s.entries {
		if e.State != "available" && e.State != "used" && e.State != "failed" && e.State != "disabled" {
			return nil, errors.New("代理池记录状态无效")
		}
	}
	return s, nil
}
func (s *Store) Get(id string) Entry {
	s.mu.Lock()
	defer s.mu.Unlock()
	e, ok := s.entries[id]
	if !ok {
		e.State = "available"
	}
	return e
}
func (s *Store) Err() error { s.mu.Lock(); defer s.mu.Unlock(); return s.err }
func (s *Store) Change(ids []string, state, reason string, attempt bool) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.changeLocked(ids, state, reason, attempt)
}
func (s *Store) changeLocked(ids []string, state, reason string, attempt bool) error {
	if state != "available" && state != "used" && state != "failed" && state != "disabled" {
		return errors.New("无效代理池操作")
	}
	next := make(map[string]Entry, len(s.entries))
	for k, v := range s.entries {
		next[k] = v
	}
	for _, id := range ids {
		e := next[id]
		e.State, e.Reason, e.Updated = state, reason, time.Now().UTC()
		if attempt {
			e.Attempts++
		}
		next[id] = e
	}
	if len(next) > 8192 {
		return errors.New("代理池历史已达 8192 条，请先归档本地记录")
	}
	if s.path != "" {
		if err := fsutil.RefuseLink(s.path); err != nil {
			s.err = err
			return err
		}
		b, _ := json.MarshalIndent(next, "", "  ")
		if err := fsutil.Write(s.path, b); err != nil {
			s.err = errors.New("无法保存代理池记录，已停止自动采集")
			return s.err
		}
	}
	s.entries = next
	s.err = nil
	return nil
}

func (s *Store) Claim(id string, allowUsed bool) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.err != nil {
		return s.err
	}
	e := s.entries[id]
	if e.State == "disabled" || e.State == "failed" || (!allowUsed && e.State == "used") {
		return errors.New("节点已被其它请求使用，请刷新池后再试")
	}
	return s.changeLocked([]string{id}, "used", "request_started", true)
}

// ClaimProbe reserves a node atomically against both probes and generation.
// Only an explicit manual retry may reuse a used or failed node.
func (s *Store) ClaimProbe(id string, manual bool) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.err != nil {
		return s.err
	}
	e := s.entries[id]
	if e.State == "disabled" || (!manual && e.State != "" && e.State != "available") {
		return errors.New("节点已被使用或停用，请刷新清单")
	}
	return s.changeLocked([]string{id}, "used", "probe_started", true)
}
