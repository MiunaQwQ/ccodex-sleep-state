package gateway

import (
	"errors"
	"time"

	"github.com/gylive/ccodex-sleep-state/internal/turnstate"
)

type DiscardResult struct {
	StateID  string `json:"state_id"`
	Role     string `json:"role"`
	ActiveID string `json:"active_id,omitempty"`
}

func (e *Engine) DiscardState(sessionID, stateID string) (DiscardResult, error) {
	e.mu.Lock()
	var s *session
	for _, candidate := range e.sessions {
		if sessionID != "" && candidate.id == sessionID {
			s = candidate
			break
		}
	}
	if s == nil {
		e.mu.Unlock()
		return DiscardResult{}, errors.New("会话已不存在，请刷新后重试")
	}
	s.mu.Lock()
	s.busy++
	s.mu.Unlock()
	e.mu.Unlock()
	defer release(s)
	// Same lock order as persistState: no older export can overwrite deletion.
	s.persistMu.Lock()
	role, err := s.state.Discard(stateID, time.Now(), func(route int) string {
		if route < 0 || route >= len(e.routes) {
			return ""
		}
		return e.routes[route].ID
	}, func(p turnstate.Persisted) error {
		if e.backup == nil {
			return nil
		}
		if err := e.backup.Save(s.backupKey, p); err != nil {
			return errors.New("无法保存丢弃结果，本次未丢弃，请检查 state 备份存储后重试")
		}
		return nil
	})
	s.persistMu.Unlock()
	if err != nil {
		return DiscardResult{}, err
	}
	active, _ := s.state.Acquire(time.Now())
	e.log.Info("state_discarded", "model", s.model, "state_id", stateID, "role", role, "active_state_id", active.Token.Fingerprint)
	// A vacancy follows the normal schedule; discarding itself sends no probe,
	// clears no budget, and changes no route or account restriction.
	e.signal()
	return DiscardResult{stateID, role, active.Token.Fingerprint}, nil
}
