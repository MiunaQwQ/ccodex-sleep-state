package service

import (
	"context"
	"errors"

	"github.com/gylive/ccodex-sleep-state/internal/codexconfig"
	"github.com/gylive/ccodex-sleep-state/internal/proxyroute"
	"github.com/gylive/ccodex-sleep-state/internal/settings"
)

type timingPreferences struct {
	ProbeSeconds    int `json:"probe_timeout_seconds"`
	RefreshSeconds  int `json:"refresh_before_seconds"`
	CooldownSeconds int `json:"probe_cooldown_seconds"`
	MaxProbes       int `json:"max_probes_per_round"`
	TTLSeconds      int `json:"state_ttl_seconds"`
}

func timingFrom(c settings.Config) timingPreferences {
	return timingPreferences{c.ProbeSeconds, c.RefreshSeconds, c.CooldownSeconds, c.MaxProbes, c.TTLSeconds}
}

// applyPreferences runs under the management lock, after active replies drain.
// A new route generation is published only after both configuration writes work.
func (c *control) applyPreferences(ctx context.Context, model, accountMode, fallback string, timing ...timingPreferences) error {
	next := c.config
	next.Model, next.AccountMode, next.StateFallback = model, accountMode, fallback
	if len(timing) > 0 {
		t := timing[0]
		next.ProbeSeconds, next.RefreshSeconds, next.CooldownSeconds = t.ProbeSeconds, t.RefreshSeconds, t.CooldownSeconds
		next.MaxProbes, next.TTLSeconds = t.MaxProbes, t.TTLSeconds
	}
	return c.applyConfig(ctx, next)
}
func (c *control) applyConfig(ctx context.Context, next settings.Config) error {
	if c.managed {
		if err := c.checkManaged(); err != nil {
			return errors.New("Codex 连接或认证已改变，请先检查与修复配置，再保存模型设置")
		}
	}
	previousTarget, previousKind, previousAuth, previousHome := c.targetURL, c.targetKind, c.authMode, c.codexHome
	if err := next.Validate(); err != nil {
		return err
	}
	c.pause()
	defer c.resume()
	if c.engine != nil && c.engine.Restricted() {
		return errors.New("账号仍处于上游拒绝或限流状态，暂不能更换模型、账号规则或采集时间")
	}
	routes, err := proxyroute.Load(ctx, next)
	if err != nil {
		return err
	}
	published := false
	defer func() {
		if !published {
			closeRoutes(routes)
		}
	}()
	// A subscription may have removed the pinned route since the previous load.
	// Do not report a successful save while replacing a working engine with none.
	for _, selectedID := range []string{next.PinnedRoute, next.EgressRoute} {
		if selectedID == "" {
			continue
		}
		found := false
		for _, route := range routes {
			if route.ID == selectedID {
				found = true
				break
			}
		}
		if !found {
			return errors.New("固定出口已不在当前订阅中，设置未保存。请在路由页恢复自动选择后再试")
		}
	}
	previous := c.config
	reconfigure := c.configure && next.SelectedModel() != previous.SelectedModel()
	rollback := func(cause error) error {
		defer func() {
			if c.setupError != "" || c.targetURL != previousTarget || c.targetKind != previousKind || c.authMode != previousAuth || c.codexHome != previousHome {
				c.stop()
				c.setupError = "设置保存失败且连接信息发生变化，已停止转发。请使用检查与修复配置重新接管；不会继续使用旧上游。"
			}
		}()
		if reconfigure {
			if restoreErr := codexconfig.Restore(c.dir); restoreErr != nil {
				c.managed = false
				c.setupError = "设置保存失败，恢复时检测到外部改动；已保留备份，请使用配置修复。"
				return errors.Join(cause, restoreErr)
			}
			c.config = previous
			if setupErr := c.installProvider(); setupErr != nil {
				c.managed = false
				c.setupError = "设置未保存，原连接未能重新接管，请使用配置修复。"
				return errors.Join(cause, setupErr)
			}
			c.managed, c.setupError = true, ""
		}
		return cause
	}
	if reconfigure {
		if err = codexconfig.Restore(c.dir); err != nil {
			return err
		}
		c.managed = false
		c.config = next
		err = c.installProvider()
		c.config = previous
		if err != nil {
			return rollback(err)
		}
	}
	if err = c.persist(next); err != nil {
		return rollback(err)
	}
	c.stop()
	c.config = next
	if reconfigure {
		c.managed, c.setupError = true, ""
	}
	c.start(routes)
	published = true
	return nil
}
