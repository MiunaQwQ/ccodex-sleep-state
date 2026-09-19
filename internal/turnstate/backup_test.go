package turnstate

import (
	"path/filepath"
	"testing"
	"time"
)

func TestStoreKeepsMultipleStandbyAndPromotesOnInvalidation(t *testing.T) {
	now := time.Now().Truncate(time.Second)
	s := New(Policy{10, time.Hour, 20 * time.Minute})
	a, _ := Parse(synthetic(10, now, 1))
	b, _ := Parse(synthetic(10, now.Add(time.Second), 2))
	c, _ := Parse(synthetic(10, now.Add(2*time.Second), 3))
	if !s.Offer(a, 0, now) || !s.Offer(b, 1, now) || !s.Offer(c, 2, now) {
		t.Fatal("failed to offer candidates")
	}
	active, ok := s.Acquire(now)
	if !ok || active.Token.Value != a.Value {
		t.Fatal("first state was not kept active")
	}
	if got := s.Status(now).Standby; got != 2 {
		t.Fatalf("standby=%d, want 2", got)
	}
	if !s.Invalidate(active, now) {
		t.Fatal("active was not invalidated")
	}
	next, ok := s.Acquire(now)
	if !ok || next.Token.Value != c.Value {
		t.Fatal("newest standby was not promoted")
	}
}

func TestBackupRoundTrip(t *testing.T) {
	now := time.Now().Truncate(time.Second)
	s := New(Policy{10, time.Hour, 20 * time.Minute})
	a, _ := Parse(synthetic(10, now, 4))
	b, _ := Parse(synthetic(10, now.Add(time.Second), 5))
	s.Offer(a, 0, now)
	s.Offer(b, 1, now)
	path := filepath.Join(t.TempDir(), "state-backup.json")
	backup, err := OpenBackup(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := backup.Save("session", s.Export(now, func(route int) string { return []string{"a", "b"}[route] })); err != nil {
		t.Fatal(err)
	}
	reopened, err := OpenBackup(path)
	if err != nil {
		t.Fatal(err)
	}
	r := New(Policy{10, time.Hour, 20 * time.Minute})
	if n := r.Restore(mustBackup(t, reopened, "session"), now, func(id string) (int, bool) {
		if id == "a" {
			return 0, true
		}
		if id == "b" {
			return 1, true
		}
		return 0, false
	}); n != 2 {
		t.Fatalf("restored=%d, want 2", n)
	}
	active, ok := r.Acquire(now)
	if !ok || active.Token.Value != a.Value || r.Status(now).Standby != 1 {
		t.Fatal("backup did not restore active and standby")
	}
}

func mustBackup(t *testing.T, store *BackupStore, key string) Persisted {
	t.Helper()
	p, ok := store.Load(key)
	if !ok {
		t.Fatal("backup entry missing")
	}
	return p
}
