package service

import (
	"context"
	"errors"
	"github.com/gylive/ccodex-sleep-state/internal/settings"
	"net/http"
)

type advancedPreferences struct {
	ExternalProxyOnly bool   `json:"external_proxy_only"`
	RequestLimitMiB   int    `json:"request_limit_mib"`
	ZstdWindowMiB     int    `json:"zstd_window_mib"`
	CompactLimitMiB   int    `json:"compact_limit_mib"`
	EgressMode        string `json:"egress_mode"`
	EgressRoute       string `json:"egress_route"`
	PoolEnabled       bool   `json:"pool_enabled"`
}

func advancedFrom(c settings.Config) advancedPreferences {
	return advancedPreferences{c.ExternalProxyOnly, int(c.RequestBytes() >> 20), int(c.WindowBytes() >> 20), int(c.CompactBytes() >> 20), c.EgressMode, c.EgressRoute, c.PoolEnabled}
}
func (c *control) poolAPI(w http.ResponseWriter, r *http.Request, ctx context.Context) {
	fail := func(err error) { reply(w, 400, map[string]string{"error": err.Error()}) }
	if c.rescue {
		fail(errors.New("请先修复服务配置"))
		return
	}
	switch r.URL.Path {
	case "/admin/api/advanced":
		var v advancedPreferences
		if err := decode(w, r, &v); err != nil {
			fail(err)
			return
		}
		if v.RequestLimitMiB == 0 || v.ZstdWindowMiB == 0 || v.CompactLimitMiB == 0 {
			fail(errors.New("请完整填写上限，不能为零"))
			return
		}
		next := c.config
		next.ExternalProxyOnly = v.ExternalProxyOnly
		next.RequestLimitMiB, next.ZstdWindowMiB, next.CompactLimitMiB = v.RequestLimitMiB, v.ZstdWindowMiB, v.CompactLimitMiB
		next.EgressMode, next.EgressRoute, next.PoolEnabled = v.EgressMode, v.EgressRoute, v.PoolEnabled
		if v.EgressMode != "fixed" {
			next.EgressRoute = ""
		}
		// New egress settings must not retain the legacy single-route filter.
		next.PinnedRoute = ""
		if err := c.applyConfig(ctx, next); err != nil {
			fail(err)
			return
		}
		reply(w, 200, map[string]string{"message": "已备份并保存。新设置立即生效，旧 state 缓存已清空；池中已用/失败记录保留，不清除上游限流。"})
	case "/admin/api/pool":
		if c.engine == nil {
			fail(errors.New("请先导入可用代理来源；配置未就绪"))
			return
		}
		reply(w, 200, map[string]any{"routes": c.engine.PoolStatus(), "enabled": c.config.PoolEnabled})
	case "/admin/api/pool/change":
		var v struct {
			IDs   []string `json:"ids"`
			State string   `json:"state"`
		}
		if err := decode(w, r, &v); err != nil {
			fail(err)
			return
		}
		if c.engine == nil || c.pool == nil {
			fail(errors.New("代理池尚未加载"))
			return
		}
		if v.State != "available" && v.State != "disabled" {
			fail(errors.New("只允许放回可用池或清理停用"))
			return
		}
		if len(v.IDs) == 0 || len(v.IDs) > 256 {
			fail(errors.New("请选择 1–256 个节点"))
			return
		}
		c.pause()
		defer c.resume()
		if c.engine.Restricted() {
			fail(errors.New("上游拒绝/限流未解除，不能通过回收节点继续尝试"))
			return
		}
		valid := map[string]bool{}
		for _, row := range c.engine.PoolStatus() {
			valid[row.ID] = true
		}
		for _, id := range v.IDs {
			if !valid[id] {
				fail(errors.New("节点不存在，请刷新列表"))
				return
			}
		}
		if err := c.pool.Change(v.IDs, v.State, "manual", false); err != nil {
			fail(err)
			return
		}
		reply(w, 200, map[string]string{"message": "池状态已保存。只修改本地节点清单，不修改订阅提供方内容，也不会跳过采集冷却。清理是可恢复的停用，不删除订阅凭据。"})
	}
}
