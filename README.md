# ccodex-sleep-state

由 **NanSsye 维护的 Codex 本地连接与 turn-state 管理工具**。通过网页面板管理代理节点、主票与备用票、手动采集、连接恢复和请求诊断。

本项目基于 **[gylive/ccodex-sleep-state](https://github.com/gylive/ccodex-sleep-state)** 二次开发。感谢上游作者和贡献者提供的基础实现；保留原提交历史、版权及 GPL-3.0 许可证。本仓库的功能调整、修复和发布由 NanSsye 维护，与上游版本分别迭代。

[下载发布版](https://github.com/NanSsye/ccodex-sleep-state/releases) · [更新记录](CHANGELOG.md) · [反馈问题](https://github.com/NanSsye/ccodex-sleep-state/issues) · [上游项目](https://github.com/gylive/ccodex-sleep-state)

## 当前版本：r18

- **节点连接自动恢复**：同一代连接连续发生两次网络错误后，重建该节点的代理客户端和 HTTP 连接池。后续请求使用新连接，在途请求继续使用原连接直至结束；保留主票、备用票及来源节点，不自动重发生成请求。
- **错误诊断更明确**：记录 DNS、TLS、超时、连接重置、连接拒绝、响应流中断等脱敏类别，以及错误阶段、连接代次和重建结果。「时间与诊断详情」显示最近连接错误及重建次数。
- **版本化发布文件**：Mac、Windows、源码包和校验文件的文件名均带版本号，包内附 `VERSION`、`SOURCE_COMMIT` 和逐项更新说明。

连接重建只处理本地连接状态，不能保证故障节点或远端服务立即恢复。同一节点两次重建尝试至少间隔 10 秒，以避免重复构建；这不会阻塞聊天或给手动打票增加等待。账号拒绝、限流、手动停用和自动采集失败清单仍按原规则处理。

## 二次开发中维护的功能

| 功能 | 行为 |
| --- | --- |
| 主票与备用票 | 按账号和模型隔离，票绑定取得时的节点，备用按取得顺序接替 |
| 手动丢弃 | 主票、备用票可单张丢弃，确认票号后执行；结果持久化，防止迟到响应把已丢弃票放回 |
| 手动打票 | 每次点击随机尝试一个可用节点，不受本地冷却、失败退避、备用错峰或小时预算的时间限制，不占用自动采集预算 |
| 自动采集 | 按既定轮询、错峰与预算执行；主备满额后停止，出现空位再补采 |
| 上游响应模型 | 显示响应声明的模型名，与请求模型不同时标记；未返回模型时明确提示 |
| 压缩请求带票 | 已有可用主票且开启注入时，V1/V2 压缩携带主票并使用其来源节点；不等待采集，不按普通生成规则拦截压缩响应 |
| 节点管理 | 导入订阅与本地文件，搜索、勾选、测速、停用与手动恢复节点 |
| 物理网卡出站 | macOS 可将节点连接和 DNS 绑定物理网卡，减少本机 VPN 对出站路径的影响 |
| 配置接管 | 备份并临时接入当前 Codex provider，停止后恢复；配置冲突提供检查与恢复流程 |

手动打票仍保留网络超时、已有采集正在进行、主备满额、账号登录/权限异常及节点停用等检查，不会绕过服务端限制。

## 下载与启动

前提是 Codex 已完成自己的登录或 API 配置。下载 [Releases](https://github.com/NanSsye/ccodex-sleep-state/releases) 中与电脑匹配的文件，完整解压：

| 电脑 | r18 文件 | 启动入口 |
| --- | --- | --- |
| Windows Intel / AMD 64 位 | `ccodex-sleep-state-r18-windows-amd64.zip` | `start.cmd` |
| Windows ARM64 | `ccodex-sleep-state-r18-windows-arm64.zip` | `start.cmd` |
| Mac Apple Silicon | `ccodex-sleep-state-r18-darwin-arm64.tar.gz` | `start.command` |
| Mac Intel | `ccodex-sleep-state-r18-darwin-amd64.tar.gz` | `start.command` |

1. 双击对应启动脚本，保持终端窗口运行。
2. 浏览器打开本地面板；没有配置过节点时，在「订阅与代理」添加自己的来源。
3. 确认显示已接管后，重启 Codex、新建任务，再检查请求是否出现在面板中。

默认面板地址为 `http://127.0.0.1:17841/admin/`。浏览器没有自动进入时，使用终端显示的本次管理口令；口令也保存在服务数据目录的 `runtime.json` 中，不要公开该文件。

也可以在解压目录运行：

```powershell
# Windows PowerShell；程序名开头是两个 c
.\ccodex-sleep-state.exe setup
```

```sh
# macOS
./ccodex-sleep-state setup
```

Windows CMD 对应 `ccodex-sleep-state.exe setup`。退出用 `Ctrl+C`，等待配置恢复完成。升级前停止旧程序，替换程序和文档，保留自己的配置与数据目录。

所有发布包只含程序、启动脚本、教程、版本信息和许可证，**不含维护者的节点、订阅、票、登录凭据、管理口令或运行日志**。下载后用 `ccodex-sleep-state-r18-SHA256SUMS.txt` 核对文件；源码包附锁定的 Go 依赖及其许可证。

[Windows 教程](docs/windows.md) · [macOS 教程](docs/macos.md) · [面板教程](docs/web-panel.md) · [代理与订阅](docs/proxies.md)

## 连接出问题时

先看「时间与诊断详情」中的错误类别和连接重建记录。连接连续失败会自动重建该节点的连接，下一次请求再尝试；当前失败的生成请求不会自动重放，避免重复消耗。

- **网络错误**：检查节点和网络是否可用；自动重建后仍失败时可换用备用票，或手动丢弃问题主票再采集。
- **401 / 403 / 429**：处理登录、权限或等待服务端恢复，连接重建不能解除这些限制。
- **配置冲突**：到「检查与修复配置」预览并处理。使用 CCS 等工具切换 provider 时，先停止本服务，切换后再启动。
- **模型名不一致**：面板显示的是上游响应声明。该字段和 state 长度都不能独立证明底层模型或回答质量。

`doctor` 可生成诊断信息。常规日志只记录元数据与脱敏错误分类，不记录提示词、回复正文、完整票或节点密码。反馈请提交到[本仓库 Issues](https://github.com/NanSsye/ccodex-sleep-state/issues)，注明版本、系统、时间和错误类别。

## 使用边界

工具不会增加账号额度或替账号开通模型权限。个人规则中的 292 字符、Team 规则中的 332 字符是本地经验筛选条件，不是 OpenAI 公布的质量指标；采集可能消耗实际额度。

官方 ChatGPT、官方 API key 和 Responses 中转按各自认证方式转发；API 中转不采集或注入官方 ChatGPT 的票。当前转发使用 HTTP / SSE。源码测试、交叉编译成功与真实节点长期稳定性是不同验证项，具体以每个 Release 的验证说明为准。

## 开发与来源

```sh
git clone https://github.com/NanSsye/ccodex-sleep-state.git
cd ccodex-sleep-state
go test ./...
go build -trimpath -o ccodex-sleep-state ./cmd/ccodex-sleep-state
```

Go 版本以 `go.mod` 为准；网页资源嵌入程序，无需另起前端服务。Go module 路径暂时保留上游名称以兼容现有代码，项目维护与发行入口以本 README 中的 NanSsye 仓库为准。

[开发说明](docs/development.md) · [架构说明](docs/architecture.md) · [隐私与安全](SECURITY.md) · [第三方软件说明](THIRD_PARTY_NOTICES.md)

许可证为 **GPL-3.0**，见 [LICENSE](LICENSE)。保留并感谢上游 `gylive/ccodex-sleep-state`、原贡献者及 Mihomo 等依赖项目。本项目与 OpenAI 没有隶属关系。
