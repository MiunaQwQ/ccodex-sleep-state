package service

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gylive/ccodex-sleep-state/internal/codexconfig"
	"github.com/gylive/ccodex-sleep-state/internal/fsutil"
	"github.com/gylive/ccodex-sleep-state/internal/gateway"
	"github.com/gylive/ccodex-sleep-state/internal/netpath"
	"github.com/gylive/ccodex-sleep-state/internal/proxyroute"
	"github.com/gylive/ccodex-sleep-state/internal/routepool"
	"github.com/gylive/ccodex-sleep-state/internal/settings"
	"github.com/gylive/ccodex-sleep-state/internal/turnstate"
)

// control owns a whole route generation. Reconfiguration never mutates routes
// under an active request, and never discards an account's upstream rejection.
type control struct {
	activeRequests        atomic.Int64
	collection            *gateway.CollectionBudget
	pool                  *routepool.Store
	backup                *turnstate.BackupStore
	targetURL, targetKind string
	authMode, codexHome   string

	mu                     sync.RWMutex
	action                 sync.Mutex
	ctx                    context.Context
	config                 settings.Config
	path, dir              string
	configure, managed     bool
	rescue                 bool
	setupError, routeError string
	tests                  *nodeTester
	catalog                []map[string]any
	nodePath               *netpath.Path
	engine                 *gateway.Engine
	cancel                 context.CancelFunc
	done                   chan struct{}
	log                    *slog.Logger
	history                requestHistory
}

func (c *control) start(routes []proxyroute.Route) {
	if c.collection == nil {
		var err error
		c.collection, err = gateway.OpenCollectionBudget(filepath.Join(c.dir, "collection-budget.json"))
		if err != nil {
			closeRoutes(routes)
			c.routeError = err.Error()
			return
		}
	}
	if c.pool == nil {
		var err error
		c.pool, err = routepool.Open(filepath.Join(c.dir, "pool-state.json"))
		if err != nil {
			closeRoutes(routes)
			c.routeError = err.Error()
			return
		}
	}
	if c.tests == nil {
		c.tests = &nodeTester{}
	}
	c.catalog = routeList(routes)
	c.nodePath = nil
	if len(routes) > 0 {
		c.nodePath = routes[0].NodePath
	}
	selected := make([]proxyroute.Route, 0, len(routes))
	for _, route := range routes {
		if routeCandidateSelected(c.config, route.ID) {
			selected = append(selected, route)
		} else {
			route.Close()
		}
	}
	if c.config.PinnedRoute == "" && len(c.config.SelectedRoutes) > 0 {
		ordered := make([]proxyroute.Route, 0, len(selected))
		byID := map[string]proxyroute.Route{}
		for _, route := range selected {
			byID[route.ID] = route
		}
		for _, id := range c.config.SelectedRoutes {
			if route, ok := byID[id]; ok {
				ordered = append(ordered, route)
				delete(byID, id)
			}
		}
		selected = ordered
	}
	if len(selected) == 0 {
		c.routeError = "所选出口已不存在，请在节点池重新选择节点。"
		return
	}
	ctx, cancel := context.WithCancel(c.ctx)
	c.cancel, c.done = cancel, make(chan struct{})
	effective := c.effective()
	c.engine = gateway.New(effective, selected, c.log, c.pool)
	c.engine.SetStateBackup(c.backup)
	c.engine.SetCollectionBudget(c.collection)
	engine, done := c.engine, c.done
	go func() { defer close(done); engine.Run(ctx) }()
	c.routeError = ""
}
func (c *control) pause() {
	if c.cancel != nil {
		c.cancel()
		<-c.done
		c.cancel = nil
		c.done = nil
	}
}
func (c *control) resume() {
	if c.engine == nil || c.cancel != nil {
		return
	}
	ctx, cancel := context.WithCancel(c.ctx)
	c.cancel = cancel
	c.done = make(chan struct{})
	engine, done := c.engine, c.done
	go func() { defer close(done); engine.Run(ctx) }()
}
func (c *control) stop() {
	if c.tests != nil {
		c.tests.stop()
	}
	c.pause()
	if c.engine != nil {
		c.engine.Close()
		c.engine = nil
	}
}
func (c *control) effective() settings.Config {
	next := c.config
	if c.targetURL != "" {
		next.Upstream = c.targetURL
		next.UpstreamKind = c.targetKind
	}
	if next.IsRelay() {
		next.InjectionDisabled = true
	}
	return next
}
func (c *control) setup() {
	if !c.configure {
		return
	}
	c.managed = false
	err := codexconfig.Restore(c.dir)
	if err == nil {
		err = c.installProvider()
	}
	if err != nil {
		c.setupError = "Codex 配置未接管。" + err.Error() + " 原文件不会被强制覆盖；请检查所选 provider、认证方式或恢复备份。"
		return
	}
	c.managed, c.setupError = true, ""
}
func (c *control) installProvider() error {
	home, err := c.config.CodexDir()
	if err != nil {
		return errors.New("无法确定 Codex 配置目录")
	}
	original, err := os.ReadFile(filepath.Join(home, "config.toml"))
	if err != nil && !os.IsNotExist(err) {
		return errors.New("无法读取 Codex 配置")
	}
	authMode, err := codexconfig.ReadAuthMode(home)
	if err != nil {
		return err
	}
	selected, err := codexconfig.ResolveWithAuth(original, c.config.CodexProfile, authMode)
	if err != nil {
		return err
	}
	next := c.config
	if next.UpstreamMode != "manual" {
		next.Upstream = selected.Upstream
		next.UpstreamKind = "relay"
		if selected.Official && selected.AuthKind == "chatgpt" {
			next.UpstreamKind = "official"
		}
	} else if next.IsRelay() && selected.AuthKind != "api_key" {
		return errors.New("手动中转上游不能接管官方 ChatGPT 登录，请选择使用 API key 的 provider")
	}
	if err = next.Validate(); err != nil {
		return err
	}
	options := codexconfig.Options{Profile: c.config.CodexProfile, AuthMode: authMode, Model: c.config.SelectedModel(), ExpectedConfigSHA256: selected.ConfigSHA256}
	if err = codexconfig.InstallWithOptions(c.dir, home, "http://"+c.config.Listen+"/backend-api/codex", options); err != nil {
		return err
	}
	c.targetURL, c.targetKind = next.Upstream, next.UpstreamKind
	c.authMode, c.codexHome = authMode, home
	return nil
}
func (c *control) checkManaged() error {
	if !c.managed {
		return nil
	}
	if err := codexconfig.CheckManaged(c.dir); err != nil {
		return err
	}
	mode, err := codexconfig.ReadAuthMode(c.codexHome)
	if err != nil || mode != c.authMode {
		return errors.New("Codex 认证方式已改变，请停止服务并重新接入")
	}
	return nil
}
func (c *control) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	started := time.Now().UTC()
	id := c.history.begin(r, started)
	observed := &observedResponse{ResponseWriter: w, onWrite: func(n int) { c.history.written(id, n, time.Now().UTC()) }}
	outcome := &gateway.RequestOutcome{}
	r = gateway.WithOutcome(r, outcome)
	r = gateway.WithProgress(r, func(v gateway.RequestOutcome) { c.history.progress(id, v) })
	defer func() {
		aborted := recover()
		status := observed.status
		if status == 0 {
			status = 200
		}
		if outcome.CompletionForwarded && (aborted == nil || aborted == http.ErrAbortHandler) {
			outcome.Result = "completed"
			if r.Context().Err() != nil || aborted != nil || observed.writeFailed {
				outcome.TerminationReason = "closed_after_complete"
			}
		} else if outcome.Result == "failed" {
			// A known upstream failure is not erased by a client closing its
			// connection after reading the failure event or HTTP error.
		} else if r.Context().Err() != nil {
			outcome.Result = "cancelled"
			outcome.TerminationReason = "client_disconnected"
			if errors.Is(r.Context().Err(), context.DeadlineExceeded) {
				outcome.TerminationReason = "request_timeout"
			}
			if c.ctx != nil && c.ctx.Err() != nil {
				outcome.TerminationReason = "service_stopping"
			}
		} else if observed.writeFailed || aborted != nil {
			outcome.Result = "failed"
			outcome.ErrorCode = "response_interrupted"
			outcome.TerminationReason = "downstream_write_failed"
		}
		c.history.finish(id, requestEvent{Kind: requestKind(r.URL.Path), Status: status, DurationMS: time.Since(started).Milliseconds(), At: time.Now().UTC(), RequestOutcome: *outcome})
		if aborted != nil {
			panic(aborted)
		}
	}()
	w = observed
	c.mu.RLock()
	locked := true
	defer func() {
		if locked {
			c.mu.RUnlock()
		}
	}()
	if c.managed {
		if err := c.checkManaged(); err != nil {
			reply(w, 409, map[string]string{"error": "codex_config_changed", "message": "Codex 配置已被 CCS 或其他程序修改。请打开管理面板，点击「检查与修复配置」；确认当前选择后重新接管并重启 Codex。不会覆盖你的改动。"})
			return
		}
	}
	if c.rescue || c.engine == nil || c.setupError != "" || c.routeError != "" {
		// Codex only shows the JSON error body.  Keep the response actionable for
		// people who do not know how to inspect a terminal: expose the local
		// panel URL and the concrete setup/route reason, never credentials.
		message := "服务还没有准备好。请打开管理面板，点击「一键接入 / 检查并修复」，不用手改配置文件。"
		if c.rescue {
			message = "服务配置文件需要修复。请打开管理面板，按提示备份并重建服务配置；不会覆盖 Codex 原文件。"
		} else if c.setupError != "" {
			message = "一键接入没有完成：" + c.setupError + " 请打开管理面板，点击「检查与修复配置」；原文件不会被强制覆盖。"
		} else if c.routeError != "" {
			message = "出口还没有准备好：" + c.routeError + " 请打开管理面板，在「订阅与代理」点「一键检查并接入」，无需手填 JSON。"
		}
		adminURL := "http://" + c.config.Listen + "/admin/"
		message += " 管理面板：" + adminURL
		reply(w, 503, map[string]string{
			"error":       "service_not_ready",
			"message":     message,
			"admin_url":   adminURL,
			"next_action": "打开 admin_url，按页面唯一的绿色按钮继续；不要在 CCS 或 Codex 中手改配置。",
		})
		return
	}
	engine := c.engine
	c.activeRequests.Add(1)
	c.mu.RUnlock()
	locked = false
	defer c.activeRequests.Add(-1)
	engine.ServeHTTP(w, r)
}
func (c *control) status() map[string]any {
	c.mu.RLock()
	defer c.mu.RUnlock()
	result := map[string]any{"model": settings.Model, "routes": 0, "sessions": []any{}}
	if c.engine != nil {
		result = c.engine.Status()
	}
	result["injection_enabled"] = !c.effective().InjectionDisabled
	result["upstream_kind"] = c.effective().UpstreamKind
	result["configured_codex"], result["config_error"], result["route_error"] = c.managed, c.setupError, c.routeError
	if err := c.checkManaged(); err != nil {
		result["config_error"] = "Codex 的连接或认证配置已改变。请点击「检查与修复配置」，选择保留 CCS 当前配置，再重新接管。"
		result["configured_codex"] = false
	}
	result["pinned_route"] = c.config.PinnedRoute
	result["configuration_writable"] = c.configure
	result["rescue_mode"] = c.rescue
	result["model"] = c.config.SelectedModel()
	result["supported_models"] = settings.SupportedModels()
	result["account_mode"] = c.config.AccountMode
	result["state_fallback"] = c.config.StateFallback
	result["state_refresh_mode"] = c.config.StateRefreshMode
	result["advanced"] = advancedFrom(c.config)
	result["node_network"] = netpath.Info{Mode: "system"}
	if c.nodePath != nil {
		result["node_network"] = c.nodePath.Info
	} else if c.config.NodeNetworkMode == "physical" {
		result["node_network"] = netpath.Info{Mode: "physical", Interface: c.config.NodeInterface}
	}
	if c.pool != nil && c.pool.Err() != nil {
		result["pool_error"] = c.pool.Err().Error()
	}
	result["timing"] = timingFrom(c.config)
	result["timing_defaults"] = timingFrom(settings.Default())
	result["traffic"] = c.history.snapshot()
	if c.tests != nil {
		result["node_tests"] = c.tests.snapshot()
	}
	result["injection_effective"] = !c.effective().InjectionDisabled && c.engine != nil && result["configured_codex"] == true && result["config_error"] == "" && c.routeError == ""
	if c.effective().IsRelay() {
		result["injection_reason"] = "当前为 API / 中转转发，不采集或注入官方 state。"
	} else if c.effective().InjectionDisabled {
		result["injection_reason"] = "注入已关闭。经过本服务的请求按原样转发，不采集 state。"
	} else if result["configured_codex"] != true {
		result["injection_reason"] = "注入开关已开启，但 Codex 配置尚未接管。先完成接入，不能据此判断请求已经经过本服务。"
	} else {
		result["injection_reason"] = "注入开关已开启；收到请求后按账号和模型分别采集。是否已有可用 state，请看下方会话状态。"
	}
	result["sources"] = map[string]any{"direct": c.config.Direct, "proxies": len(c.config.ProxyURLs) + len(c.config.ProxyEnvs), "subscriptions": len(c.config.Subscriptions)}
	return result
}
func (c *control) persist(next settings.Config) error {
	if err := fsutil.RefuseLink(c.path); err != nil {
		return err
	}
	original, readErr := os.ReadFile(c.path)
	existed := readErr == nil
	if readErr != nil && !os.IsNotExist(readErr) {
		return errors.New("无法读取原配置，未保存")
	}
	// Keep an independent backup and don't overwrite edits made outside the UI.
	if data, err := os.ReadFile(c.path); err == nil {
		previous, err := settings.Load(c.path)
		if err != nil {
			return errors.New("配置文件已在外部改动或损坏，请先修复并重启服务")
		}
		a, _ := json.Marshal(previous)
		b, _ := json.Marshal(c.config)
		if string(a) != string(b) {
			return errors.New("配置文件已在外部修改。为避免覆盖，请重启服务后再保存")
		}
		if err := fsutil.Write(filepath.Join(c.dir, "backups", "config-"+time.Now().UTC().Format("20060102T150405.000000000")+".json"), data); err != nil {
			return errors.New("无法创建配置备份，未保存")
		}
	} else if !os.IsNotExist(err) {
		return errors.New("无法读取原配置，未保存")
	}
	if err := fsutil.RefuseLink(c.path); err != nil {
		return err
	}
	fresh, err := os.ReadFile(c.path)
	if (err == nil) != existed || (err != nil && !os.IsNotExist(err)) || !bytes.Equal(fresh, original) {
		return errors.New("保存期间配置被其他程序修改，已保留备份但未覆盖文件")
	}
	data, _ := json.MarshalIndent(next, "", "  ")
	return fsutil.Write(c.path, append(data, '\n'))
}
