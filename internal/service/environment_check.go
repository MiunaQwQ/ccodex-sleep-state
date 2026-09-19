package service

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"time"
)

// environmentCheck is deliberately passive. It never calls checkManaged (which
// reads authentication), loads subscriptions, dials a proxy, or writes a file.
// Only explicit, sanitized observations enter this report; raw errors and paths
// may contain credentials and must never be serialized.
type environmentItem struct {
	ID       string `json:"id"`
	Title    string `json:"title"`
	Status   string `json:"status"`
	Message  string `json:"message"`
	NextStep string `json:"next_step"`
	Page     string `json:"page"`
}
type environmentReport struct {
	CheckedAt           time.Time         `json:"checked_at"`
	Summary             string            `json:"summary"`
	ReadOnly            bool              `json:"read_only"`
	NetworkTested       bool              `json:"network_tested"`
	CredentialFilesRead bool              `json:"credential_files_read"`
	Items               []environmentItem `json:"items"`
}
type environmentSession struct {
	Phase             string `json:"phase"`
	RejectedStatus    int    `json:"rejected_status"`
	CooldownSeconds   int    `json:"cooldown_seconds"`
	RetryAfterSeconds int    `json:"retry_after_seconds"`
}

func (c *control) environmentCheck() environmentReport {
	c.mu.RLock()
	defer c.mu.RUnlock()
	report := environmentReport{CheckedAt: time.Now().UTC(), ReadOnly: true, Items: []environmentItem{}}
	add := func(id, title, status, message, next, page string) {
		report.Items = append(report.Items, environmentItem{id, title, status, message, next, page})
	}
	add("panel", "本机管理面板", "pass", "能收到本次检测结果，说明当前本机面板端口正在提供服务。", "保留这个页面即可，无需再次启动一个服务。", "overview")
	if c.rescue || c.config.Validate() != nil {
		add("service_config", "服务配置", "attention", "服务配置需要处理，检测没有改动原文件。", "到连接设置按提示检查与修复，先保留备份。", "configuration")
	} else {
		add("service_config", "服务配置", "pass", "当前内存中的服务配置通过格式检查。", "这不代表磁盘配置未被其他程序修改。", "configuration")
	}
	if c.setupError != "" {
		add("codex_connection", "接入 Codex", "attention", "接入过程中发现配置问题，尚未完成。", "到连接设置查看提示；中转地址缺失时先补全地址。", "configuration")
	} else if c.managed {
		add("codex_connection", "接入 Codex", "pass", "本次服务启动已完成配置接管。", "本次只读检测不读取登录凭据；如果刚切换过 CCS 配置，请检查与修复后重启 Codex。", "configuration")
	} else if !c.configure {
		add("codex_connection", "接入 Codex", "unverified", "当前是“不自动接管配置”模式。", "如果要自动接入，请按启动说明以正常模式启动；当前检测不会替你改配置。", "configuration")
	} else {
		add("codex_connection", "接入 Codex", "attention", "服务已启动，但还没有接上 Codex。", "回到开始使用，点击“一键接入 Codex”，完成后重启 Codex。", "overview")
	}
	if home, err := c.config.CodexDir(); err != nil {
		add("codex_files", "本机 Codex 配置位置", "attention", "无法确定本机配置目录。", "到连接设置检查 Codex 配置目录。", "configuration")
	} else {
		info, err := os.Lstat(filepath.Join(home, "config.toml"))
		switch {
		case os.IsNotExist(err):
			add("codex_files", "本机 Codex 配置位置", "attention", "没有找到 config.toml；没有检查或读取登录文件。", "确认已安装并启动过 Codex，再检查连接设置中的目录。", "configuration")
		case err != nil || !info.Mode().IsRegular():
			add("codex_files", "本机 Codex 配置位置", "unverified", "配置文件无法确认或为链接/特殊文件；为安全没有打开它。", "检查连接设置中的配置位置及文件权限；不要直接删除原文件。", "configuration")
		default:
			add("codex_files", "本机 Codex 配置位置", "pass", "找到普通配置文件；本次没有读取其内容或任何登录凭据。", "如需核对 provider，请使用连接设置中的配置预览。", "configuration")
		}
	}
	if info, err := os.Lstat(c.dir); err != nil || !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
		add("data_directory", "本地保存位置", "attention", "服务数据目录不可确认或为链接；没有执行写入测试。", "检查启动时选择的数据目录，保留原文件和备份。", "configuration")
	} else {
		add("data_directory", "本地保存位置", "pass", "服务数据目录存在。为了只读，本次未测试写权限。", "保存设置若失败，按页面提示检查目录权限。", "configuration")
	}
	if c.engine == nil || c.routeError != "" {
		add("routes", "已加载的连接", "attention", "当前没有就绪的转发引擎，或加载连接时发生错误。", "到订阅与代理添加自己的连接并检查提示。", "sources")
	} else {
		rows := c.engine.PoolStatus()
		usable := 0
		allowUsed := c.config.EgressMode != "random"
		fixedUsable := false
		for _, row := range rows {
			if row.State != "disabled" && (!c.config.PoolEnabled || (row.State != "failed" && (allowUsed || row.State != "used"))) {
				usable++
			}
			if row.ID == c.config.EgressRoute && row.State != "disabled" && (!c.config.PoolEnabled || row.State != "failed") {
				fixedUsable = true
			}
		}
		switch {
		case len(rows) == 0:
			add("routes", "已加载的连接", "attention", "没有已加载的连接。", "到订阅与代理粘贴连接地址或导入 TXT。", "sources")
		case c.config.EgressMode == "fixed" && !fixedUsable:
			add("routes", "已加载的连接", "attention", "固定出口不存在或已被停用/标记失败。", "到代理池选择一个出口，或明确回收该节点。", "pool")
		case c.config.EgressMode != "fixed" && usable == 0:
			add("routes", "已加载的连接", "attention", "连接均已用完、失败或停用。", "到代理池手动回收需要复用的节点，或添加新连接。", "pool")
		case c.config.EgressMode == "random" && !c.effective().InjectionDisabled && len(rows) < 2:
			add("routes", "已加载的连接", "attention", "独立随机出口需要采集节点之外的可用节点，目前不足。", "补充连接，或在高级设置明确选择采集同出口/固定出口。", "pool")
		default:
			add("routes", "已加载的连接", "pass", fmt.Sprintf("已加载 %d 个连接，其中 %d 个处于可分配状态。没有发送代理探测。", len(rows), usable), "节点数不是公网 IP 数，加载成功也不等于网络连通。", "pool")
		}
	}
	if c.pool != nil && c.pool.Err() != nil {
		add("pool_storage", "代理池记录", "attention", "代理池记录读写异常，已保留失败状态。", "先备份数据目录，再按代理池提示处理；不要清记录来绕过暂停。", "pool")
	} else {
		add("pool_storage", "代理池记录", "pass", "当前未记录代理池存储错误；本次没有修改节点状态。", "使用记录保留，检测不会回收节点。", "pool")
	}
	var sessions []environmentSession
	if c.engine != nil {
		// Decode only numeric/state fields, never return the engine snapshot verbatim.
		raw, _ := json.Marshal(c.engine.Status()["sessions"])
		_ = json.Unmarshal(raw, &sessions)
	}
	report.Items = append(report.Items, environmentUpstreamItem(sessions))
	add("remote_access", "代理与上游真实可用性", "unverified", "本次只做本地检查：未连接订阅、未探测代理、未调用模型，也未验证上游权限或回答质量。", "本地需处理项完成后，在 Codex 手动发一句话验证；可能消耗额度。若出现 401/403/429，先处理权限或等待，不要切出口冲击重试。", "overview")
	needs := 0
	for _, item := range report.Items {
		if item.Status == "attention" {
			needs++
		}
	}
	if needs > 0 {
		report.Summary = fmt.Sprintf("有 %d 项需要处理，先做下面第一项提示即可。", needs)
	} else {
		report.Summary = "本地检查完成，没有发现必须处理的配置项；上游是否能用仍未验证。"
	}
	return report
}
func environmentUpstreamItem(sessions []environmentSession) environmentItem {
	item := environmentItem{ID: "upstream_pause", Title: "已知的上游暂停与冷却", Status: "unverified", Message: "还没有可用于判断的会话记录。", NextStep: "先完成本地连接；不会为了检测自动发模型请求。", Page: "overview"}
	maxWait := 0
	limited, auth := false, false
	for _, s := range sessions {
		auth = auth || s.RejectedStatus == 401 || s.RejectedStatus == 403 || s.Phase == "auth_blocked"
		limited = limited || s.RejectedStatus == 429 || s.Phase == "rate_limited"
		maxWait = max(maxWait, s.CooldownSeconds, s.RetryAfterSeconds)
	}
	switch {
	case auth:
		item.Status = "attention"
		item.Message = "上游已有登录或权限拒绝，检测不会解除暂停。"
		item.NextStep = "回 Codex 或服务商核对登录和权限；不要换节点重试绕过拒绝。"
	case limited:
		item.Status = "attention"
		item.Message = "上游已要求限流暂停。"
		item.NextStep = "等待上游要求的时间，检测不会重置计时或触发重试。"
	case maxWait > 0:
		item.Status = "pass"
		item.Message = fmt.Sprintf("已有会话在冷却，最长还需约 %d 秒；这是正常的保护间隔。", maxWait)
		item.NextStep = "等倒计时结束即可，不要连点重试。"
	case len(sessions) > 0:
		item.Status = "pass"
		item.Message = "现有会话没有记录需暂停的 401/403/429。"
		item.NextStep = "这不等于上游已授权或下一次调用必定成功。"
	}
	return item
}
