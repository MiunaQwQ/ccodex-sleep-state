// Package service owns process lifetime; all commands share this one listener.
package service

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"time"

	"github.com/gylive/ccodex-sleep-state/internal/codexconfig"
	"github.com/gylive/ccodex-sleep-state/internal/fsutil"
	"github.com/gylive/ccodex-sleep-state/internal/instance"
	"github.com/gylive/ccodex-sleep-state/internal/logbook"
	"github.com/gylive/ccodex-sleep-state/internal/proxyroute"
	"github.com/gylive/ccodex-sleep-state/internal/settings"
	"github.com/gylive/ccodex-sleep-state/internal/turnstate"
	C "github.com/metacubex/mihomo/constant"
)

type Runtime struct {
	Address string `json:"address"`
	Token   string `json:"control_token"`
}

func Run(parent context.Context, dir string, c settings.Config, configure bool, out io.Writer) error {
	return RunWithConfig(parent, dir, filepath.Join(dir, "config.json"), c, configure, out)
}
func RunWithConfig(parent context.Context, dir, configPath string, c settings.Config, configure bool, out io.Writer) error {
	return run(parent, dir, configPath, c, configure, false, false, out)
}
func RunSetup(parent context.Context, dir, configPath string, c settings.Config, configure bool, out io.Writer) error {
	return run(parent, dir, configPath, c, configure, false, true, out)
}
func run(parent context.Context, dir, configPath string, c settings.Config, configure, rescue, launchBrowser bool, out io.Writer) (result error) {
	if err := c.Validate(); err != nil {
		return err
	}
	lock, err := instance.Acquire(dir)
	if err != nil {
		return err
	}
	defer lock.Close()
	listener, err := net.Listen("tcp", c.Listen)
	if err != nil {
		return errors.New("cannot bind loopback port; another service may already be running")
	}
	defer listener.Close()
	logger, logs, err := logbook.Open(filepath.Join(dir, "logs"))
	if err != nil {
		return err
	}
	defer logs.Close()
	C.SetHomeDir(filepath.Join(dir, "core"))
	proxyroute.QuietCore()
	ctx, cancel := context.WithCancel(parent)
	defer cancel()
	backup, err := turnstate.OpenBackup(filepath.Join(dir, "state-backup.json"))
	if err != nil {
		return err
	}
	ctl := &control{ctx: ctx, config: c, path: configPath, dir: dir, configure: configure, rescue: rescue, log: logger, backup: backup}

	var routes []proxyroute.Route
	if launchBrowser && configure && !rescue {
		if err := ctl.quickSetupMode(ctx, false); err != nil {
			ctl.setupError = "自动接入未完成：" + err.Error() + "。请在面板检查与修复，原配置和备份不会被强制覆盖。"
		}
	} else {
		ctl.setup()
		var loadErr error
		routes, loadErr = proxyroute.Load(ctx, c)
		if loadErr != nil {
			ctl.routeError = loadErr.Error()
		} else {
			ctl.start(routes)
		}
	}
	defer func() {
		ctl.mu.Lock()
		defer ctl.mu.Unlock()
		ctl.stop()
		if configure {
			if err := codexconfig.Restore(dir); err != nil {
				logger.Error("config_restore_conflict")
				result = errors.Join(result, err)
			}
		}
	}()
	secret := make([]byte, 32)
	if _, err = rand.Read(secret); err != nil {
		return err
	}
	runtime := Runtime{c.Listen, hex.EncodeToString(secret)}
	data, _ := json.Marshal(runtime)
	runtimePath := filepath.Join(dir, "runtime.json")
	if err = fsutil.Write(runtimePath, data); err != nil {
		return err
	}
	defer os.Remove(runtimePath)
	ticket, err := newBrowserTicket()
	if err != nil {
		return err
	}
	launchURL := "http://" + c.Listen + "/admin/#launch=" + ticket.value
	handler := controlHandler(c.Listen, runtime.Token, ctl, ticket)
	server := &http.Server{Handler: handler, ReadHeaderTimeout: 10 * time.Second, ReadTimeout: 30 * time.Second, IdleTimeout: 120 * time.Second, MaxHeaderBytes: 64 << 10,
		BaseContext: func(net.Listener) context.Context { return ctx }}
	stopped := make(chan error, 1)
	go func() { stopped <- server.Serve(listener) }()
	if launchBrowser {
		if err := openPanel(launchURL); err != nil {
			fmt.Fprintln(out, "未能自动打开浏览器，请手动打开管理面板并粘贴下方口令。")
		}
	}
	startupStatus := ctl.status()
	logger.Info("service_started", "routes", startupStatus["routes"], "model", c.SelectedModel(), "configured_codex", startupStatus["configured_codex"])
	fmt.Fprintf(out, "本地服务：http://%s\n管理面板：http://%s/admin/\n管理口令：%s\n口令只用于本机管理，请不要发到群里。Ctrl+C 停止并恢复已接管的配置。\n", c.Listen, c.Listen, runtime.Token)
	select {
	case err = <-stopped:
		if !errors.Is(err, http.ErrServerClosed) {
			result = errors.New("HTTP service stopped unexpectedly")
		}
	case <-parent.Done():
	}
	cancel()
	shutdownCtx, stop := context.WithTimeout(context.Background(), 5*time.Second)
	defer stop()
	if err = server.Shutdown(shutdownCtx); err != nil {
		_ = server.Close()
	}
	logger.Info("service_stopped")
	return result
}

func Status(ctx context.Context, dir string) ([]byte, error) {
	data, err := os.ReadFile(filepath.Join(dir, "runtime.json"))
	if err != nil {
		return nil, errors.New("service is not running (runtime file missing)")
	}
	var runtime Runtime
	if json.Unmarshal(data, &runtime) != nil {
		return nil, errors.New("invalid runtime file")
	}
	host, _, err := net.SplitHostPort(runtime.Address)
	if err != nil || !settings.IsLoopback(host) {
		return nil, errors.New("runtime address is not loopback")
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, "http://"+runtime.Address+"/_sleep/status", nil)
	if err != nil {
		return nil, errors.New("invalid runtime address")
	}
	req.Header.Set("Authorization", "Bearer "+runtime.Token)
	client := &http.Client{Timeout: 3 * time.Second, Transport: &http.Transport{Proxy: nil}, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
	defer client.CloseIdleConnections()
	resp, err := client.Do(req)
	if err != nil {
		return nil, errors.New("service is not reachable")
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		return nil, errors.New("service status check rejected")
	}
	return io.ReadAll(io.LimitReader(resp.Body, 64<<10))
}
