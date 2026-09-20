# 问题与验收清单

群里遇到的问题都记在这里：**发生了什么、改了哪一层、用什么证明改好了、还有哪些没测。** 不再只在聊天里说“已优化”。

当前清单对应本轮源码工作，不等于旧 Release 已含这些改动。网关、settings、turnstate 的本轮竞态测试与静态检查已通过；实际 Codex CLI 0.154.0 接本地模拟上游的 V1/V2 压缩及后续回复也已通过。真实账本已保留 11 次：Sol V2 压缩及后续回复、Terra 回复成功；追加的两次 Astra 短回复成功，但此前 Astra 容量失败和 292 未取得仍然成立；V1 直连上游 404，桥接追加验收首条 Sol 回复曾发生出口传输失败，整轮不算全部通过。测试状态分三层：

- **代码与回归项已加入**：能找到实现和测试，但不能据此写“所有环境通过”。
- **本地 / 模拟上游通过**：证明可重复的实现行为，不代表真实服务商一定支持。
- **真实上游验收**：必须注明客户端、账号类型、模型、请求范围和结果；短聊天不能替代压缩测试。

历史 v0.2.0-alpha.1 的真实 Astra 记录单独保留在[测试记录](testing.md)。它不替这轮新增功能背书。

## P01 远程上下文压缩（优先项）

### 2026-09-19 r6 修复

此前版本把官方 `POST /responses/compact` 改写成普通 `POST /responses` 并追加 `compaction_trigger`；这会把独立压缩误送到普通生成容量路径。当前版本已移除该桥接：V1 原样转发，V2 仍按 `compaction_trigger` 识别；两者都跳过 state 注入，但保留当前 state 的出口绑定。此前“V1 404 / 桥接验收”的记录属于旧实现，不能代表 r6 的路径行为。新增原生路径、失败正文保持和压缩请求编码回归测试均已通过；真实上游仍需用 r6 独立验收。

### 用户看到的情况

接管生成的 provider 中有 `name = 'Sleep State (local)'`。群友问：这个名字会不会影响远程压缩？

### 已定位的原因

在核对的 Codex CLI `0.154.0` 源码中，官方 provider 的名称参与能力判断；改变名称可能丢失远程压缩能力，回到本地压缩路径。因此不是“只是个备注，肯定没影响”。[官方 provider 判断](https://github.com/openai/codex/blob/rust-v0.154.0/codex-rs/model-provider-info/src/lib.rs) · [压缩能力判断](https://github.com/openai/codex/blob/rust-v0.154.0/codex-rs/model-provider/src/provider.rs) · [客户端压缩任务](https://github.com/openai/codex/blob/rust-v0.154.0/codex-rs/core/src/tasks/compact.rs)

**伴随问题 P29：**保留官方名称后，实际 OAuth 客户端还会发送 zstd 压缩请求。只测 API key 模拟环境没有覆盖这个差异，真实验收首先暴露了本地 415；现已加入受限解码，后续真实请求已越过本地 415；不把请求编码修好等同于所有模型/压缩通过。

代理原来还把 `/responses/compact` 和普通生成一起走采集与回包形状检查，可能在真正压缩前因缺少 state 返回 503。新版远程压缩 V2 使用 `/responses` 和 `compaction_trigger`，仅按 URL 判断也不够。

### 本轮修复

- 官方 provider 保留正确能力声明，不把所有中转假装成官方。
- 分别识别 V1 `POST /responses/compact` 与 V2 `POST /responses` + `compaction_trigger`。
- 压缩不依赖先采到 292 / 332，不为压缩额外发模型采集请求。
- 已有合格 state 时保持对应出口；没有时按正常出口策略转发。
- 压缩回包不按普通生成的 state 形状硬拦截，不重放压缩请求。
- 401 / 403 / 429 仍统一处理，不因走压缩路径跳过停止规则。

### 验收清单

| 项目 | 状态 / 证据 |
| --- | --- |
| 官方 provider 名称与中转名称分别处理 | `internal/codexconfig/recovery_test.go`：`TestPatchModelsAndProviderNames`，本轮回归项 |
| V1 无合格 state 仍转发，不额外采集 | **包级竞态回归通过**：`TestCompactionDoesNotDependOnCollection` |
| V1 保持已有 state 出口，不拒绝响应形状 | **包级竞态回归通过**：`TestCompactionKeepsStateRouteAndDoesNotInspectResponseShape` |
| 压缩被上游拒绝后其他模型也停止 | **包级竞态回归通过**：`TestCompactionRejectionBlocksOtherModels` |
| V2 `compaction_trigger` 分类、转发与不探测 | **包级竞态回归通过**：`TestRemoteCompactionV2Recognition`、`TestRemoteCompactionV2PassesWithoutProbeOrShapeGate` |
| 真实 Codex CLI 0.154.0 → 本地模拟上游 → 手动压缩 | **通过**：V1、默认 V2 均产生预期请求；客户端收到压缩完成，压缩前后回复均为 OK；旧名称对照走本地压缩 |
| 真实官方上游 V2 压缩 | **Sol 通过**：第 4 次压缩完成，第 5 次压缩后回复 OK；未使用本服务注入 |
| 真实官方上游 V1 直连压缩 | **失败**：第 8 次 `/responses/compact` 返回上游 404，未追加重放 |
| 官方 V1→V2 兼容桥接 | **本地模拟通过，真实桥接链路尚未完成**：发送前选择 V2 协议，不是 404 后重放；首轮桥接验收第 1 次在普通 Sol 回复的出口传输失败处停止，后来两次 Astra 短回复验证成功；保留 11 次账本 |
| 实际第三方中转压缩、Windows 桌面压缩 | **尚未验收**；取决于上游及客户端能力 |

**完成标准：**不仅看到一次 HTTP 200，还要确认客户端确实发出了预期压缩请求、没有误触发采集，压缩结果进入客户端上下文，并能继续下一轮。不同版本不能相互代替。

## 其余问题总表

| 编号 | 原始问题 / 根因 | 本轮处理 | 验证与剩余边界 |
| --- | --- | --- | --- |
| P02 | CCS 修改配置后 409；旧版把整个文件哈希不同都当成接管失效 | 改为检查接管相关的连接、认证和选中 profile；注释与无关设置不应误报，真正换上游仍拦截 | `TestUnrelatedEditsRemainManagedAndSurviveRestore`、`TestSemanticGuardRejectsOwnedRoutingAndAuthChanges` 等回归项；不同 CCS 桌面版本待实测 |
| P03 | 1.x / 旧版升级留有事务，反复点接管仍失败 | 增加预览 → 保留当前 / 恢复备份 → 重新接管；三方比较，不直接删事务 | `TestExplicitKeepCurrentCCSSwitch`、`TestRecoveryRequiresFreshPreview`、`TestLegacyReceiptUsesChecksumVerifiedReconstruction` 等；冲突不能安全合并时明确停止 |
| P04 | 用户不懂手工恢复，担心覆盖 CCS 新配置 | 当前文件与事务先独立归档，再执行确认的恢复方式；文件又变了必须重新预览 | `TestFullRecoveryArchivesChangedManagedProvider`、`TestSnapshotChecksumAndBackupChecksumProtectRecovery`、面板恢复集成回归 |
| P05 | 服务 JSON 损坏导致连面板也打不开 | `setup` / `serve` 对现存可读普通坏配置启动只读修复面板；确认后备份重建，注入默认关闭 | `TestRescueBackupRequiresMatchingPreview`、CLI 目标筛选；端口冲突和权限问题仍可能阻止启动，不谎称任意错误都能修 |
| P06 | Codex TOML 被手改、重复段或残缺内容破坏 | 明确选择官方/API 上游后才允许备份重建最小配置；不从坏 TOML 猜密钥，不改 `auth.json` | `internal/codexconfig/clean.go` 与重建测试；旧事务需先恢复，重建会取代原偏好，需要用户确认 |
| P07 | Team 一直打不到 292 | 账号规则独立：个人 10 块 / 292，Team 12 块 / 332；支持自动提示与手动选择 | `TestAccountPolicies`、`TestTeamAccepts332AndRejects356`；**真实 Team 尚未本轮验收** |
| P08 | 把 312 / 356 当成“降智铁证”，或靠放宽规则假装成功 | 维持拒绝不合格形状；只称经验规则，不给质量打分；不能凭长度反推账号类型 | `TestUnusableTeamShapeCannotChangePersonalPolicy`；没有质量对照实验，不宣称消除降智 |
| P09 | 只有 Astra，换 Sol/Terra 报不支持 | 增加 `gpt-5.6-sol`、`gpt-5.6-terra`；探测跟随实际模型，state 按凭据、账号、模型隔离 | `TestModelsKeepIndependentStateAndProbeTheirOwnModel`；**真实 Sol/Terra 普通回复通过，Sol V2 及后续回复通过；追加两次 Astra 短回复通过**；没有合格 state 采集/注入实测，不能混写 |
| P10 | 同账号换模型可能把上游暂停清掉 | 认证与限流拒绝跨模型保留；模型/路由/规则修改不能清掉暂停 | `TestRejectionSurvivesModelSwitch` 和现有面板限流竞态测试；不靠换模型继续请求 |
| P11 | 没采到 292 只返回 503，用户不知道卡在哪 | 显示网络、缺响应头、无效封装、响应未完成、形状、时间、冷却与账号规则 | `TestProbeDiagnostics` 等；无合格值不保证能“修”成特定长度 |
| P12 | 用户只想先正常聊天，严格等待影响可用性 | 新增 `state_fallback=passthrough`，由用户明确选择；转发内核空策略按 `strict`；首次 `setup` 未设置策略时选普通转发兜底，保留用户明确 `strict`；首页修复按钮确认后选兜底；未注入时明示，不伪造成功、不重放 | **包级竞态回归通过**：`TestStateFallbackForwardsOriginalRequestOnce`、`TestStateFallbackNeverBypassesAccountRejection`；认证/限流、已发出请求不因此重试 |
| P13 | 开关按钮写“关闭注入”，旁边却还写“已关闭注入” | 每次刷新覆盖状态说明，分开开关意图与实际接入状态 | `TestInjectionStatusNeverKeepsStaleExplanation`；浏览器交互验收另记 |
| P14 | Codex 能回复，面板没有显示；“未接管”容易被误当成功 | 显示接入步骤、只读模式、请求计数与最近状态；说明重启、目录、profile 覆盖 | `TestTrafficCountsRejectedRequestsWithoutSession`、`TestMetricsCountRejectedRequestsWithoutSession`；不能自动删除任务/项目覆盖 |
| P15 | 空会话列表被当成从未收到请求 | 请求计数独立于会话；拒绝、模型列表和过期会话不再等同无流量 | 服务保留最近有限条元数据，出口切换不清空；不记录请求正文 |
| P16 | `init`、`serve`、环境变量、目录对新手太复杂 | `setup` 创建缺失配置后启动；保留旧设置；Windows / macOS 双击脚本 | CLI 包竞态测试、静态检查和 macOS 脚本空格路径测试已通过；Windows 双击体验待本机验收 |
| P17 | 群里排错要别人发完整配置，容易泄漏 | `doctor` 白名单摘要 + 面板脱敏诊断，不输出自由文本敏感字段 | `TestDoctorSafeReport`、`TestDoctorWithoutServiceDoesNotCreateFiles` 已通过；完整备份仍不可公开 |
| P18 | 不知道改了哪份配置、profile/`CODEX_HOME` 混用 | `paths`、明确数据目录和 profile 教程；只读模式不假装接管成功 | 原有目录测试及 `TestReadOnlyRecoveryDoesNotPretendSuccess`；各桌面版本覆盖项待实测 |
| P19 | 中转与官方混用，`requires_openai_auth=true` 导致判断错误 | 保留 provider 和认证类型；API key 通道不注入、不借官方凭据 | 历史真实 CLI → 本地模拟中转通过；本轮需回归，不能替所有中转作保证 |
| P20 | 仅设置 `CCODEX_PROXY`，服务仍直连；用户把命令当代理输入 | 面板直接填地址并保存，区分来源读取与网络连接测试 | 面板解析、持久化、来源失败不覆盖旧配置已有测试；端口必须以用户软件为准 |
| P21 | 本地订阅、混合协议、空节点不好排错 | 本地文件/回环 HTTP、协议与关键词筛选、User-Agent、空节点错误 | 解析和构造测试；历史 AnyTLS 真实链路通过，不能替所有协议所有节点背书 |
| P22 | 503 混成本地故障，连点重试反而浪费额度 | 区分本地 state 错误、服务未就绪、上游错误；错误来源响应头；正式请求不自动重放 | 现有网关测试与本轮诊断；上游可能已经执行，不承诺端到端恰好一次 |
| P23 | Ctrl+C、直接关终端、关闭浏览器含义混淆 | 保留正常退出恢复、崩溃事务检查，教程明确生命周期 | 原有真实子进程崩溃恢复测试；系统强杀不保证即时恢复，需下次恢复流程 |
| P24 | 长会话、加密内容报错缺少边界说明 | 保留请求正文和会话链，不自动删上下文；远程压缩单独处理 | 普通转发测试不能覆盖所有历史密文、长会话和上游变更；继续观察 |
| P25 | “学习 GPT-Load”容易变成宣称所有功能已移植 | 固定上游提交、核对许可证，列功能映射；只借鉴适合本地单用户的部分 | [GPT-Load 功能映射](gpt-load-design.md)列出账号池、多协议、计费、OAuth、监控等未移植项 |
| P26 | 第一次还要复制口令、选多项，步骤太多 | `setup` 自动打开浏览器；60 秒一次性 launch ticket 兑换登录，地址 fragment 立即清除；手动口令兜底 | `TestLaunchTicketSingleUseExpiryAndCrossOrigin` 等；不把长期口令放 URL，实际浏览器验收另记 |
| P27 | 用户希望一键接上，不想先懂配置 | `setup` 启动即备份、安全保留恢复、沿用模型/注入开关；未设置兜底策略才选普通转发；首页按钮用于再修复。仅无自定义来源时探本地常见 SOCKS5 | `quick_setup_test.go` 覆盖 CCS、坏 TOML、只读、保存冲突、固定出口与限流；真实出口可用性仍需短请求，旧会话需重启 |
| P28 | 采集失败后不知道如何再试，或连续点刷请求 | 会话有受限「重新采集」入口；按当前 RAM 凭据和随机会话 ID 调用，不清状态、不跳过冷却与次数上限 | **包级竞态回归通过**：`TestManualRetryRespectsCooldownAndKeepsOpaqueSessionID`、`TestManualRetryDoesNotClearReadyStateOrAccountLimits`、`TestManualRetryReportsBusyAndSuccess` |
| P29 | 保留官方 provider 名称后，真实 OAuth 客户端发 zstd，原网关直接返回 415 | 支持 identity/gzip/zstd，解码后再识别模型与 V2 压缩；压缩输入、解压正文和 zstd 窗口均受 16 MiB 上限约束，拒绝损坏与多层编码 | `encoding_test.go` 编码、V2、损坏/截断、超限/压缩炸弹和模型校验测试已通过；首轮实际 OAuth 在本地 415 停止，**0 次模型尝试**，修复后真实请求已通过该编码入口，模型/压缩部分成功、部分失败，见 8 次账本 |
| P30 | 关闭本服务注入时误删客户端自带 state | 官方普通转发保留客户端 state，实际注入才用已采集合格值替换；中转仍清除官方 state | `TestInjectionOffPreservesClientOwnedState`、`TestStateFallbackPreservesClientOwnedStateButProbeDoesNotUseIt` 竞态回归通过；探测不携带客户端旧 state |
| P31 | HTTP 200 的 SSE 失败被误报为网络/采集未完成 | 按明确 code 区分容量和响应失败；`server_is_overloaded` / `slow_down` 为容量原因，不等同 HTTP 429；明确 rate/quota 错误共享停止 | `sse_diagnostics_test.go` 竞态/静态检查通过；不按正文猜测、不公开未知 code/message，明确失败结束本轮且不换出口 |
| P32 | 后台队列跨空闲过期边界时会话被误删 | 后台采集持有期间固定会话生命周期，避免拒绝守卫脱离当前会话 | 网关竞态回归通过；不是清除过期限制或无限保留凭据 |
| P33 | V1 模拟成功但真实 `/responses/compact` 返回 404 | 单列真实接口差异，增加发送前的官方 V1→V2 桥接验证，不做 404 后生成重放 | 本轮第 8 次真实失败已保留；桥接本地模拟通过，真实追加桥接阶段因首条 Sol 回复出口传输失败而中止；后续两次 Astra 短回复验收成功 |
| P34 | Codex 只看到 `503 service_not_ready`，不知道下一步，也不会手改配置 | 503 响应增加本机管理面板链接、具体的接管/出口/修复原因和唯一下一步；不返回凭据，不让请求自动重放；面板继续提供一键接入与修复 | `TestNotReadyResponseExplainsOneClickRecovery`；真实出口故障仍需在面板检查，服务不会把网络失败伪装成已就绪 |

## 必须保持的回归边界

修复体验问题不能以破坏这些边界为代价：

- 不改作者日常项目、登录、系统代理和真实订阅；公开包不含私人环境。
- 不因关闭注入、改模型、换出口、恢复配置而清除尚未解除的上游拒绝。
- 不自动刷新登录，不使用重置卡，不轮换账号绕过限额。
- 不自动重放已经发到上游的生成，不在流输出一半时切身份。
- 不给中转附带官方账号头与 state，不让未知上游接收官方登录凭据。
- `--no-config` 明确不接管 Codex，面板也不能悄悄突破。
- 修改前有独立备份，确认前后的文件版本必须一致；无法安全判断时保留现场。
- 管理面板仅回环、管理口令、Host/Origin 校验；诊断不包含正文和凭据。

## 这轮还不能说“全都能用”

以下验收没有完成前，应继续按公开测试版说明：

1. **真实官方远程压缩**：Sol V2 及压缩后回复已通过；V1 直连失败；发送前桥接在本地模拟客户端已通过，真实桥接阶段首条 Sol 回复出口传输失败，不能把后续 Astra 短回复成功当成桥接成功。不能把 V2 或模拟桥接成功扩大为所有压缩方式成功。
2. **真实 Team / Business**：套餐提示与选中 workspace 的对应、332 合格值及后续注入。
3. **模型与 state**：Sol/Terra 本轮普通生成通过，但其合格 state 采集/注入尚未证明；Astra 追加两次短回复成功，但本轮仍没有取得 292；先前容量相关失败不被抹掉，也不借历史成功替代。
4. **Windows 桌面实际安装与升级**：含旧配置、CCS 切换、双击启动、停止恢复，不只是交叉编译。
5. **中转差异**：不同地址前缀、认证方式、profile、压缩能力要分别确认。
6. **多轮与长会话**：state 过期、后台采集、取消、SSE 中断、加密上下文与压缩后续接。
7. **浏览器操作**：开关往返、修复确认、窄屏、错误提示与最终状态一致。

真实请求需要明确预算和失败停止条件，不因为测试未通过就无限继续跑。没有真实测试或质量对照证据，不写“保证 292”“百分百不降智”“永久解除限流”。
