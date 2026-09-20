// ccodex-sleep-state is a local, single-process Codex turn-state service.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"

	"github.com/gylive/ccodex-sleep-state/internal/codexconfig"
	"github.com/gylive/ccodex-sleep-state/internal/fsutil"
	"github.com/gylive/ccodex-sleep-state/internal/instance"
	"github.com/gylive/ccodex-sleep-state/internal/proxyroute"
	"github.com/gylive/ccodex-sleep-state/internal/service"
	"github.com/gylive/ccodex-sleep-state/internal/settings"
)

var version = "dev"

const help = `ccodex-sleep-state — 一个本地服务，管理 Codex 连接与 turn-state

  ccodex-sleep-state setup    一键准备并启动；保留已有配置，退出时恢复接管。
  ccodex-sleep-state doctor   只读诊断，输出可发给群友的脱敏摘要。
  ccodex-sleep-state init     创建本程序配置，暂不修改 Codex。
  ccodex-sleep-state serve    启动服务，备份并接管 Codex 配置；Ctrl+C 恢复。
  ccodex-sleep-state status   从正在运行的服务读取状态。

  管理面板                   serve 启动后打开终端显示的 /admin/ 地址。
                             在网页配置订阅、代理、路由和注入开关。
  ccodex-sleep-state check    检查配置和订阅，不发送模型请求。
  ccodex-sleep-state restore  崩溃后恢复 Codex 配置，不覆盖用户的新修改。
  ccodex-sleep-state paths    显示本机的配置与日志目录。
  ccodex-sleep-state version  显示版本。

选项放在命令之后：
  --data-dir PATH             使用独立的服务数据目录。
  --config PATH               使用指定 JSON 配置；init 会创建这个文件。
  --no-config                仅限 serve / setup：只启动服务，不接管 Codex 配置。

主动探测使用你已有的 Codex 登录，会消耗实际额度。未成功时连续寻找，
成功后暂停额外采集；遇到 401、403、429 暂停。日志不记录账号、订阅、正文和完整 token。
第一次使用运行 setup，或双击 start.cmd（Windows）/ start.command（macOS）。
启动本身不发送模型请求；Codex 接入后的探测可能消耗额度。
退出请用 Ctrl+C，让程序有机会恢复配置。
`

func main() {
	proxyroute.QuietCore()
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if err := run(ctx, os.Args[1:], os.Stdout); err != nil {
		fmt.Fprintln(os.Stderr, "错误：", err)
		os.Exit(1)
	}
}
func run(ctx context.Context, args []string, out io.Writer) error {
	if len(args) == 0 || args[0] == "help" || args[0] == "--help" {
		fmt.Fprint(out, help)
		return nil
	}
	if args[0] == "version" {
		fmt.Fprintln(out, version)
		return nil
	}
	defaultDir, err := settings.DataDir()
	if err != nil {
		return err
	}
	flags := flag.NewFlagSet(args[0], flag.ContinueOnError)
	flags.SetOutput(out)
	dir := flags.String("data-dir", defaultDir, "service data directory")
	configPath := flags.String("config", "", "service config path")
	noConfig := flags.Bool("no-config", false, "do not edit Codex config")
	if err = flags.Parse(args[1:]); err != nil {
		return err
	}
	if flags.NArg() != 0 {
		return errors.New("unexpected positional arguments")
	}
	if *noConfig && args[0] != "serve" && args[0] != "setup" {
		return errors.New("--no-config 只适用于 serve 或 setup")
	}
	*dir, err = filepath.Abs(*dir)
	if err != nil {
		return err
	}
	if *configPath == "" {
		*configPath = filepath.Join(*dir, "config.json")
	}
	switch args[0] {
	case "paths":
		c := settings.Default()
		if _, err := os.Stat(*configPath); err == nil {
			c, err = settings.Load(*configPath)
			if err != nil {
				return err
			}
		}
		home, err := c.CodexDir()
		if err != nil {
			return err
		}
		fmt.Fprintf(out, "服务数据：%s\n服务配置：%s\nCodex 配置：%s\n日志目录：%s\n", *dir, *configPath, filepath.Join(home, "config.toml"), filepath.Join(*dir, "logs"))
		return nil
	case "init":
		if _, err = os.Lstat(*configPath); err == nil {
			return errors.New("config already exists; it was not overwritten")
		} else if !os.IsNotExist(err) {
			return err
		}
		data, _ := json.MarshalIndent(settings.Default(), "", "  ")
		if err = fsutil.Create(*configPath, append(data, '\n')); err != nil {
			return err
		}
		fmt.Fprintln(out, "已创建", *configPath)
		return nil
	case "setup":
		if _, statusErr := service.Status(ctx, *dir); statusErr == nil {
			if err := service.ReopenPanel(ctx, *dir); err != nil {
				return err
			}
			fmt.Fprintln(out, "服务已经在运行，已重新打开管理面板。没有启动第二个服务，也没有再次修改配置。")
			return nil
		}
		c, err := prepareSetup(*configPath, out)
		if err != nil {
			var invalid *setupConfigError
			if errors.As(err, &invalid) && repairableConfig(*configPath) {
				fmt.Fprintln(out, "服务配置需要修复，将打开本地修复面板；不会修改 Codex 或发送模型请求。")
				return service.RunRescue(ctx, *dir, *configPath, out)
			}
			return err
		}
		if *noConfig {
			fmt.Fprintln(out, "只启动管理服务，不修改 Codex 配置。")
		} else {
			fmt.Fprintln(out, "将备份并接管当前 Codex 配置；请勿同时在 CCS 中切换配置。")
		}
		fmt.Fprintln(out, "正在自动检查并接入，随后打开管理面板；一般无需手填配置，启动本身不发送模型请求。")
		return service.RunSetup(ctx, *dir, *configPath, c, !*noConfig, out)
	case "doctor":
		data, err := service.Status(ctx, *dir)
		if err != nil {
			fmt.Fprintln(out, "未能连接本地管理服务。先运行 setup，并保持服务窗口打开。")
			fmt.Fprintln(out, "本次只读检查，没有修改配置，也没有发送模型请求。")
			return nil
		}
		return writeDiagnosis(data, out)
	case "status":
		data, err := service.Status(ctx, *dir)
		if err != nil {
			return err
		}
		_, err = out.Write(data)
		return err
	case "restore":
		lock, err := instance.Acquire(*dir)
		if err != nil {
			return err
		}
		defer lock.Close()
		if err = codexconfig.Restore(*dir); err != nil {
			return err
		}
		fmt.Fprintln(out, "配置已恢复，或当前没有需要恢复的记录。")
		return nil
	case "serve", "check":
		c, err := settings.Load(*configPath)
		if err != nil {
			if args[0] == "serve" && repairableConfig(*configPath) {
				fmt.Fprintln(out, "服务配置需要修复，将打开本地修复面板；不会修改 Codex 或发送模型请求。")
				return service.RunRescue(ctx, *dir, *configPath, out)
			}
			return err
		}
		if args[0] == "serve" {
			return service.RunWithConfig(ctx, *dir, *configPath, c, !*noConfig, out)
		}
		routes, err := proxyroute.Load(ctx, c)
		if err != nil {
			return err
		}
		for _, r := range routes {
			r.Close()
		}
		fmt.Fprintf(out, "配置有效：%d 个出站配置。没有发送模型请求。\n", len(routes))
		return nil
	default:
		return errors.New("unknown command; run help")
	}
}
