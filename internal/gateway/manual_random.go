package gateway

import (
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"time"
)

// RetryRandomState is one explicit manual ticket attempt, independent of all
// timed collection gates. It never clears the automatic collector's limits.
func (e *Engine) RetryRandomState(ctx context.Context, sessionID string) error {
	if e.disabled.Load() {
		return errors.New("请先开启注入")
	}
	e.mu.Lock()
	var s *session
	for _, candidate := range e.sessions {
		if candidate.id == sessionID && sessionID != "" {
			s = candidate
			break
		}
	}
	if s == nil {
		e.mu.Unlock()
		return errors.New("会话不存在，请先用此模型发送消息")
	}
	s.mu.Lock()
	e.mu.Unlock()
	if e.statePoolReady(s, time.Now()) {
		s.mu.Unlock()
		return errors.New("主备已满，停止采集；出现空位后再试")
	}
	if !s.activated || s.probing != nil || s.queued {
		s.mu.Unlock()
		return errors.New("会话尚未启用或正在采集，请等待当前采集结束")
	}
	if status, _ := s.rejection(); status == 401 || status == 403 {
		s.mu.Unlock()
		return fmt.Errorf("上游返回 %d，请先处理登录或访问权限", status)
	}
	active, usable := s.state.Acquire(time.Now())
	var candidates, alternatives []int
	for i, route := range e.routes {
		entry := e.pool.Get(route.ID)
		if entry.State != "available" || (usable && active.Route == i) {
			continue
		}
		candidates = append(candidates, i)
		if s.lastProbeResult == nil || s.lastProbeResult.Route != route.ID {
			alternatives = append(alternatives, i)
		}
	}
	if len(alternatives) > 0 {
		candidates = alternatives
	}
	if len(candidates) == 0 {
		s.mu.Unlock()
		return errors.New("没有可用的其他采集节点；已跳过当前主卡节点和停用/失败节点")
	}
	index := candidates[rand.IntN(len(candidates))]
	s.busy++
	s.queued = true
	s.mu.Unlock()
	defer releaseWork(s)
	started := time.Now()
	e.refreshOnce(ctx, s, false, true, index)
	if ctx.Err() != nil {
		return errors.New("本次随机采集已取消或超时；原有主卡保留")
	}
	if err := e.pool.Err(); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.lastProbe.After(started) {
		return errors.New("本次未发出或未完成采集，节点或服务状态已改变；原主卡保留")
	}
	if s.diagnostic != "accepted" {
		return fmt.Errorf("随机节点本次未采到合格 state：%s；原有主卡保留", s.diagnostic)
	}
	return nil
}
