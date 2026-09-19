package service

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"net/http"
	"os"

	"github.com/gylive/ccodex-sleep-state/internal/proxyroute"
	"github.com/gylive/ccodex-sleep-state/internal/settings"
)

func routeSelected(c settings.Config, id string) bool {
	if c.PinnedRoute != "" {
		return c.PinnedRoute == id
	}
	if len(c.SelectedRoutes) == 0 {
		return true
	}
	for _, selected := range c.SelectedRoutes {
		if selected == id {
			return true
		}
	}
	return false
}
func validateSelection(routes []proxyroute.Route, c settings.Config) error {
	for _, route := range routes {
		if routeSelected(c, route.ID) {
			return nil
		}
	}
	return errors.New("所选节点已不在来源中。请先选择其它节点；原有连接保持不变")
}
func sourceID(value any) string {
	b, _ := json.Marshal(value)
	hash := sha256.Sum256(b)
	return hex.EncodeToString(hash[:16])
}
func appendSource(sources []settings.Source, source settings.Source) []settings.Source {
	for _, old := range sources {
		if sourceID(old) == sourceID(source) {
			return sources
		}
	}
	return append(sources, source)
}

// Full links/configs are intentionally available to the authenticated local
// owner. They never enter public static assets, status logs or diagnostics.
func (c *control) poolStatus() map[string]any {
	sources := []map[string]any{}
	for _, raw := range c.config.ProxyURLs {
		sources = append(sources, map[string]any{"id": "proxy:" + sourceID(raw), "kind": "节点链接", "value": raw})
	}
	for _, env := range c.config.ProxyEnvs {
		sources = append(sources, map[string]any{"id": "env:" + sourceID(env), "kind": "环境变量 " + env, "value": os.Getenv(env)})
	}
	for _, source := range c.config.Subscriptions {
		kind, value := "订阅", source.URL
		if source.File != "" {
			kind, value = "本地文件", source.File
		} else if source.URLEnv != "" {
			value = os.Getenv(source.URLEnv)
		}
		sources = append(sources, map[string]any{"id": "subscription:" + sourceID(source), "kind": kind, "value": value})
	}
	routes := []map[string]any{}
	for _, original := range c.catalog {
		row := map[string]any{}
		for k, v := range original {
			row[k] = v
		}
		row["selected"] = routeSelected(c.config, row["id"].(string))
		routes = append(routes, row)
	}
	return map[string]any{"sources": sources, "routes": routes, "pinned_route": c.config.PinnedRoute, "selected_routes": c.config.SelectedRoutes}
}

func (c *control) replacePool(ctx context.Context, next settings.Config) error {
	if err := next.Validate(); err != nil {
		return err
	}
	routes, err := proxyroute.Load(ctx, next)
	if err != nil {
		return err
	}
	committed := false
	defer func() {
		if !committed {
			closeRoutes(routes)
		}
	}()
	if err = validateSelection(routes, next); err != nil {
		return err
	}
	c.pause()
	defer c.resume()
	if c.engine != nil && c.engine.Restricted() {
		return errors.New("上游登录、权限或限流暂停尚未解除，不能通过更换节点清除暂停")
	}
	if err = c.persist(next); err != nil {
		return err
	}
	c.stop()
	c.config = next
	c.start(routes)
	committed = true
	return nil
}

// Called while the normal management locks are held.
func (c *control) poolAction(w http.ResponseWriter, r *http.Request, ctx context.Context) bool {
	next := c.config
	switch r.URL.Path {
	case "/admin/api/pool/select":
		var v struct {
			IDs []string `json:"ids"`
		}
		if err := decode(w, r, &v); err != nil {
			reply(w, 400, map[string]string{"error": err.Error()})
			return true
		}
		if len(v.IDs) == 0 {
			reply(w, 400, map[string]string{"error": "至少勾选一个候选节点。需要停止采集请关闭注入"})
			return true
		}
		known := map[string]bool{}
		for _, row := range c.catalog {
			known[row["id"].(string)] = true
		}
		next.SelectedRoutes = nil
		seen := map[string]bool{}
		for _, id := range v.IDs {
			if !known[id] {
				reply(w, 400, map[string]string{"error": "节点列表已变化，请刷新后重新勾选"})
				return true
			}
			if !seen[id] {
				next.SelectedRoutes = append(next.SelectedRoutes, id)
				seen[id] = true
			}
		}
		// Selecting candidates must not silently cancel an explicit fixed node.
		// The card-level "取消固定" action is the only way to return to auto.
		// Saving the candidate list enables the continuous proxy pool. Nodes are
		// health observations and remain eligible after a probe; only a manual
		// disable removes one from automatic collection.
		next.PoolEnabled = true
		next.EgressMode, next.EgressRoute = "state", ""
		next.StateRefreshMode = "on_demand"
	case "/admin/api/pool/remove-source":
		var v struct {
			ID string `json:"id"`
		}
		if err := decode(w, r, &v); err != nil {
			reply(w, 400, map[string]string{"error": err.Error()})
			return true
		}
		found := false
		next.ProxyURLs = nil
		next.ProxyEnvs = nil
		next.Subscriptions = nil
		for _, value := range c.config.ProxyURLs {
			if "proxy:"+sourceID(value) == v.ID {
				found = true
			} else {
				next.ProxyURLs = append(next.ProxyURLs, value)
			}
		}
		for _, value := range c.config.ProxyEnvs {
			if "env:"+sourceID(value) == v.ID {
				found = true
			} else {
				next.ProxyEnvs = append(next.ProxyEnvs, value)
			}
		}
		for _, value := range c.config.Subscriptions {
			if "subscription:"+sourceID(value) == v.ID {
				found = true
			} else {
				next.Subscriptions = append(next.Subscriptions, value)
			}
		}
		if !found {
			reply(w, 400, map[string]string{"error": "来源已变化，请刷新列表"})
			return true
		}
	case "/admin/api/pool/reload":
		// Explicit subscription refresh; routine UI polling only reads the catalog.
	default:
		return false
	}
	if err := c.replacePool(ctx, next); err != nil {
		reply(w, 400, map[string]string{"error": err.Error()})
		return true
	}
	reply(w, 200, map[string]string{"message": "节点池已更新。固定时只用固定节点；取消固定后从已勾选节点自动轮换，某张 state 失效会立即切换备用 state。"})
	return true
}
