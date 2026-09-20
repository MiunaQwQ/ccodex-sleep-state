package gateway

import (
	"errors"
	"github.com/gylive/ccodex-sleep-state/internal/turnstate"
	"time"
)

// SwitchStateRoute reuses the main ticket on an explicitly selected egress.
// It does not acquire a ticket, reset limits, replay or cancel any request.
func (e *Engine) SwitchStateRoute(sessionID, stateID string, version uint64, routeID string) error {
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
		return errors.New("票池已不存在，请刷新后重试")
	}
	s.mu.Lock()
	s.busy++
	s.mu.Unlock()
	e.mu.Unlock()
	defer release(s)
	route := -1
	for i, r := range e.routes {
		if r.ID == routeID {
			route = i
			break
		}
	}
	if route < 0 {
		return errors.New("所选节点不存在，请刷新后重试")
	}
	entry := e.pool.Get(routeID)
	if entry.State == "disabled" || entry.State == "failed" {
		return errors.New("所选节点已停用或失败，请选择其他可用节点")
	}
	s.persistMu.Lock()
	defer s.persistMu.Unlock()
	err := s.state.SwitchRoute(stateID, version, route, time.Now(), func(i int) string { return e.routes[i].ID }, func(p turnstate.Persisted) error {
		if e.backup == nil {
			return nil
		}
		if err := e.backup.Save(s.backupKey, p); err != nil {
			return errors.New("无法保存节点切换，本次未切换，请检查本地备份存储")
		}
		return nil
	})
	if err != nil {
		return err
	}
	e.log.Info("state_route_switched", "session_id", s.id, "model", s.model, "state_id", stateID, "route", routeID)
	return nil
}
