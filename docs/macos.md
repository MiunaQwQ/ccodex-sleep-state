# macOS：解压后启动，不用安装 Go

Apple 芯片选 `darwin-arm64`，Intel 芯片选 `darwin-amd64`。到 [Releases](https://github.com/NanSsye/ccodex-sleep-state-r14/releases) 下载并核对来源、SHA256，完整解压到自己的目录，例如 `~/Applications/ccodex-sleep-state`。源码构建见[开发说明](development.md)。

> 当前仍是公开测试版。教程对应当前源码，旧包未必包含新入口；以对应 Release 为准。真实测试和未验证内容见[问题与验收清单](issues-and-verification.md)。

## 启动只需一步

先确认 Codex 原来的配置能用。用 CCS 的先选好这次的配置，然后退出 Codex，暂时不要继续切换。

**双击解压目录里的 `start.command`。** 它会找到同目录的程序，执行 `setup`。也可以在该目录打开终端，运行：

```sh
./ccodex-sleep-state setup
```

第一次创建本工具配置，之后沿用已有设置，不需要反复 `init`。启动本身不会发送模型请求；接入 Codex 后采集 state 可能消耗额度。

如果系统拦截，先确认来源，再使用系统提供的单应用放行方式。发布文件没有 Apple Developer 公证；不要关闭整个 Gatekeeper，也不要照着不明教程删除全局安全设置。

浏览器会自动打开并进入本地面板，通常不需要复制口令。若未打开或一次性启动凭证过期，再打开终端显示的管理地址、粘贴管理口令；默认是 [http://127.0.0.1:17841/admin/](http://127.0.0.1:17841/admin/)。不要把口令或启动链接发到群里。

## 接上自己的 Codex

1. `setup` 已自动执行接入：沿用当前账号和模型，先备份并保留配置；如果你还没配出口，尝试识别本机常见 SOCKS5 端口。正常启动不用再点一遍按钮。
2. 看到已接管后，**重启 Codex，新建会话**，发一条短消息。
3. 没找到代理、要填订阅时，再去「订阅与代理」；要换模型或 Team 规则，再去「连接设置」。

只检查 `127.0.0.1` 上的 7897、7890、10808，不扫描网络，不读取代理软件的账号库；已有代理和订阅不会被覆盖。本地握手成功不是模型连通性证明，仍要看实际短请求。
有代理软件就填它真实开放的地址。`socks5://127.0.0.1:7897` 只是格式示例，端口要看你自己的设置。程序不切系统代理，不改代理软件策略，也不读取其账号库。

官方 ChatGPT 可采集和注入；官方 API key / 中转只做普通转发。Team 的经验筛选是 332 / 12 块，不再必须满足个人 292 / 10 块。长度不是模型质量证明。

首次 `setup` 未设置策略时采用普通转发兜底；之前明确选择的严格策略会保留。「连接设置」可随时明确选择，首页接入按钮用于再次修复，确认后会选择普通转发兜底。也可直接关闭注入；两者都不会解除认证拒绝和官方限流，也不会重放已经发到上游的生成。

[面板教程](web-panel.md)说明每个按钮，包括配置修复和状态怎么看。

## 出错不用先手改文件

- **CCS 切换后 409 / 未接管：**面板点「检查与修复配置」，通常选保留当前配置再接管。文件和恢复记录会先备份。只改注释或无关项不应再触发整体冲突。
- **服务 JSON 坏了：**`setup` 打开只读修复面板，由你确认备份重建；默认配置会关闭注入，修复后重启再填自己的代理。
- **Codex TOML 坏了：**先保留文件，优先让 CCS 重新生成正确配置；明确确认后才重建，不猜原来的 API key。
- **面板没请求、Codex 却能回复：**检查是否重启了 Codex，以及 `CODEX_HOME`、profile 或项目/任务参数是不是选了另一份配置。

要发反馈，面板下载脱敏诊断，或运行：

```sh
./ccodex-sleep-state doctor
```

只输出允许分享的状态摘要，不输出登录信息、管理口令、订阅和正文。不要把整个配置目录打包发出去。

## 停止和恢复

保留服务终端窗口。退出时先等当前回复结束，按 **Ctrl+C**，等恢复完成，再重启 Codex。关闭浏览器只是关闭面板，关闭注入也不是卸载接管。

只恢复、不启动：

```sh
./ccodex-sleep-state restore
```

需要先确认服务已退出。遇到冲突，使用面板预览和修复，不要删除事务文件来“清错误”。程序不安装 LaunchAgent，不自动开机运行。

切 CCS 的推荐顺序：停止服务 → CCS 切换 → 重新启动 → 重启 Codex。

## 文件放在哪里

默认服务配置：

```text
~/Library/Application Support/ccodex-sleep-state/config.json
```

同目录有日志和修复归档。Codex 默认配置是 `~/.codex/config.toml`；设置了 `CODEX_HOME` 时会跟随它。检查实际路径：

```sh
./ccodex-sleep-state paths
```

如果只习惯使用命令行，也可以把二进制安装到 `~/.local/bin`：

```sh
mkdir -p "$HOME/.local/bin"
install -m 755 ./ccodex-sleep-state "$HOME/.local/bin/ccodex-sleep-state"
"$HOME/.local/bin/ccodex-sleep-state" setup
```

只复制二进制后，原位置的双击脚本不会替你搜索任意安装目录；选择一种方式使用即可。

## 独立试用，不修改日常 Codex

```sh
./ccodex-sleep-state setup --data-dir "$HOME/ccodex-trial" --no-config
```

只启动面板，不自动接到日常 Codex。订阅如已配置仍可能下载，手动连接测试也会联网，所以 `--no-config` 不等于网络沙盒。

自定义服务 JSON：

```sh
./ccodex-sleep-state setup --data-dir "$HOME/ccodex-trial" --config "$HOME/ccodex-trial/local.json" --no-config
```

`--config` 指本程序的 JSON，不是 Codex 的 TOML。`doctor`、`status`、`restore` 使用同一个 `--data-dir`。需要正常接管时停止服务，去掉 `--no-config` 再启动；不要把官方凭据填到未知中转上游。
