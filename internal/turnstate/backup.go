package turnstate

import (
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/gylive/ccodex-sleep-state/internal/fsutil"
)

const maxBackupEntries = 64

type backupDocument struct {
	Version int                  `json:"version"`
	Entries map[string]Persisted `json:"entries"`
}

// BackupStore is a local, permission-0600 state cache. Its keys are hashes of
// the credential/model/upstream identity; it never stores bearer credentials.
type BackupStore struct {
	mu      sync.Mutex
	path    string
	entries map[string]Persisted
	err     error
}

func OpenBackup(path string) (*BackupStore, error) {
	store := &BackupStore{path: path, entries: map[string]Persisted{}}
	if path == "" {
		return store, nil
	}
	if err := fsutil.RefuseLink(path); err != nil {
		return nil, err
	}
	data, err := os.ReadFile(path)
	if os.IsNotExist(err) {
		return store, nil
	}
	if err != nil {
		return nil, errors.New("无法读取 state 备份")
	}
	if len(data) > 2<<20 {
		return nil, errors.New("state 备份过大")
	}
	var doc backupDocument
	if json.Unmarshal(data, &doc) != nil || doc.Entries == nil || doc.Version != 1 {
		return nil, errors.New("state 备份损坏，请保留文件后修复")
	}
	if len(doc.Entries) > maxBackupEntries {
		return nil, errors.New("state 备份条目过多")
	}
	store.entries = doc.Entries
	return store, nil
}

func (s *BackupStore) Err() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.err
}

func (s *BackupStore) Load(key string) (Persisted, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	entry, ok := s.entries[key]
	return entry, ok
}

func (s *BackupStore) Save(key string, value Persisted) error {
	if key == "" || value.Active == nil && len(value.Standby) == 0 && len(value.Parked) == 0 && len(value.Discarded) == 0 {
		return s.Delete(key)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	next := make(map[string]Persisted, len(s.entries)+1)
	for k, v := range s.entries {
		next[k] = v
	}
	next[key] = value
	if len(next) > maxBackupEntries {
		// Drop the oldest cache entry. It is only a recovery hint; active RAM
		// state remains authoritative for the running process.
		oldestKey := ""
		var oldest time.Time
		for k, v := range next {
			if k == key {
				continue
			}
			if oldestKey == "" || v.SavedAt.Before(oldest) {
				oldestKey, oldest = k, v.SavedAt
			}
		}
		if oldestKey != "" {
			delete(next, oldestKey)
		}
	}
	if err := s.writeLocked(next); err != nil {
		return err
	}
	s.entries = next
	s.err = nil
	return nil
}

func (s *BackupStore) Delete(key string) error {
	if key == "" {
		return nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.entries[key]; !ok {
		return nil
	}
	next := make(map[string]Persisted, len(s.entries)-1)
	for k, v := range s.entries {
		if k != key {
			next[k] = v
		}
	}
	if err := s.writeLocked(next); err != nil {
		return err
	}
	s.entries = next
	s.err = nil
	return nil
}

func (s *BackupStore) writeLocked(entries map[string]Persisted) error {
	if s.path == "" {
		return nil
	}
	if err := fsutil.RefuseLink(s.path); err != nil {
		s.err = err
		return err
	}
	data, err := json.MarshalIndent(backupDocument{Version: 1, Entries: entries}, "", "  ")
	if err != nil {
		s.err = errors.New("无法编码 state 备份")
		return s.err
	}
	if err := fsutil.Write(s.path, append(data, '\n')); err != nil {
		s.err = errors.New("无法保存 state 备份")
		return s.err
	}
	_ = os.Chmod(filepath.Clean(s.path), 0600)
	return nil
}
