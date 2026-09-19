"use strict";
const $ = (id) => document.getElementById(id);
const launchTicket = new URLSearchParams(location.hash.slice(1)).get("launch");
if (location.hash) history.replaceState(null, "", location.pathname);
let token = sessionStorage.getItem("sleep-state-control") || "";
let state = null;
let busy = false;
let noticeTimer;
let recovery = null;
let preferencesDirty = false;
let timingDirty = false;
let pool = null;
let selectionDirty = false;
const timingFields = {
  probe_timeout_seconds: "probe-timeout",
  probe_cooldown_seconds: "probe-cooldown",
  state_ttl_seconds: "state-ttl",
  refresh_before_seconds: "refresh-before",
  max_probes_per_round: "max-probes",
};
function readTiming() {
  return Object.fromEntries(Object.entries(timingFields).map(([key, id]) => [key, Number($(id).value)]));
}
function timingSummary() {
  const t = readTiming();
  $("refresh-before").setCustomValidity(t.refresh_before_seconds >= t.state_ttl_seconds ? "提前刷新时间必须小于本地有效期" : "");
  $("timing-summary").textContent = `${timingDirty ? "尚未保存 · " : "已保存 · "}成功后至少间隔 ${t.probe_cooldown_seconds} 秒；失败不冷却，每批 ${t.max_probes_per_round} 次后继续；state 签发约 ${t.state_ttl_seconds - t.refresh_before_seconds} 秒后进入刷新窗口。`;
}
function fillTiming(t) {
  if (!t) return;
  for (const [key, id] of Object.entries(timingFields)) $(id).value = t[key];
  timingSummary();
}
function notice(text) {
  $("notice").textContent = text;
  $("notice").hidden = false;
  clearTimeout(noticeTimer);
  noticeTimer = setTimeout(() => {
    $("notice").hidden = true;
  }, 12000);
}
async function api(path, body) {
  const response = await fetch("/admin/api/" + path, {
    method: body === undefined ? "GET" : "POST",
    headers: {
      Authorization: "Bearer " + token,
      ...(body === undefined ? {} : { "Content-Type": "application/json" }),
    },
    cache: "no-store",
    redirect: "error",
    ...(body === undefined ? {} : { body: JSON.stringify(body) }),
  });
  const value = await response.json();
  if (!response.ok)
    throw new Error(value.error || "操作未完成，请检查服务终端。");
  return value;
}
async function action(fn) {
  if (busy) return;
  busy = true;
  document.querySelectorAll("button").forEach((button) => {
    button.disabled = true;
  });
  try {
    await fn();
  } catch (error) {
    notice(error.message);
  } finally {
    busy = false;
    document.querySelectorAll("button").forEach((button) => {
      button.disabled = false;
    });
    if (state && state.upstream_kind === "relay") $("toggle").disabled = true;
    document
      .querySelectorAll("[data-locked]")
      .forEach(
        (button) => (button.disabled = button.dataset.locked === "true"),
      );
    if (recovery) {
      $("keep-current").disabled = !recovery.can_keep_current;
      $("restore-backup").disabled = !recovery.can_restore_backup;
    }
    if (state && !state.configuration_writable)
      for (const id of [
        "recover",
        "inspect-recovery",
        "keep-current",
        "restore-backup",
        "quick-setup",
      ])
        $(id).disabled = true;
  }
}
const phases = {
  ready: "已有符合规则的 state · 正常使用",
 search_queued: "等待采集槽位 · 将连续寻找",
 upstream_paused: "上游报告繁忙 / 失败，暂时暂停",
  collecting: "正在采集，请稍等",
  waiting_for_state: "尚未采到合格 state",
  auth_blocked: "上游拒绝登录或权限",
  rate_limited: "上游要求等待",
  passthrough: "普通转发，不注入",
};
function textNode(tag, text, className) {
  const node = document.createElement(tag);
  node.textContent = text;
  if (className) node.className = className;
  return node;
}
async function refresh() {
  state = await api("status");
 if(typeof renderUpgrade==="function") renderUpgrade(state);
  $("rescue-card").hidden = !state.rescue_mode;
  $("headline").textContent =
    state.config_error || state.route_error
      ? "有一项配置需要处理。"
      : !state.configured_codex
        ? "服务已启动，等待 Codex 接入。"
        : state.injection_enabled
          ? "已接入 Codex，等待或正在处理请求。"
          : "服务在运行，当前不注入。";
  $("mode").textContent =
    state.upstream_kind === "relay" ? "中转 / API 转发" : "官方 ChatGPT";
  $("route-count").textContent = state.routes;
  $("managed").textContent = state.configured_codex ? "已接管" : "未接管";
  $("toggle").textContent = state.injection_enabled ? "关闭注入" : "开启注入";
  $("toggle").disabled = state.upstream_kind === "relay";
  $("injection-help").textContent =
    state.injection_reason ||
    (state.injection_enabled
      ? "注入已开启，是否已有可用 state 请看会话状态。"
      : "注入已关闭，不采集或替换 state。");
  for (const id of ["recover", "inspect-recovery"])
    $(id).disabled = !state.configuration_writable;
  if (!preferencesDirty && document.activeElement?.id !== "model-select")
    $("model-select").value = state.model;
  if (!preferencesDirty && document.activeElement?.id !== "account-select")
    $("account-select").value = state.account_mode || "auto";
  if (!preferencesDirty && document.activeElement?.id !== "fallback-select")
    $("fallback-select").value = state.state_fallback || "strict";
  if (!timingDirty) fillTiming(state.timing);
  const traffic = state.traffic || { total: 0, failed: 0, recent: [] };
  $("step-config").textContent = state.configured_codex
    ? "已接入 · 可检查或修复"
    : "未接入 · 点这里处理";
  $("step-route").textContent = state.routes
    ? `${state.routes} 个出口配置 · 可更换`
    : "没有可用出口 · 点这里配置";
  $("step-request").textContent = traffic.total
    ? `已收到 ${traffic.total} 次请求`
    : "重启 Codex，再试一次";
  $("traffic-summary").textContent =
    `本次启动收到 ${traffic.total} 次请求，${traffic.failed} 次返回错误。切换出口不会清空这份记录。`;
  $("recent-requests").replaceChildren();
  for (const item of (traffic.recent || []).slice(-5).reverse()) {
    $("recent-requests").append(
      textNode(
        "div",
        `${new Date(item.at).toLocaleTimeString()} · ${item.kind} · HTTP ${item.status} · ${item.duration_ms} ms`,
        "hint",
      ),
    );
  }
  $("warnings").replaceChildren();
  for (const message of [state.config_error, state.route_error, state.pool_error])
    if (message) $("warnings").append(textNode("div", message, "warning"));
  $("sessions").replaceChildren();
  if (!state.sessions?.length)
    $("sessions").append(
      textNode(
        "p",
        traffic.total
          ? "已经有请求到达，但还没有保留中的模型会话。请看上面的状态码；模型列表请求、被拦截请求或过期会话不代表已成功生成。"
          : state.configured_codex
            ? "还没收到请求。请重启 Codex，并新建会话发一条短消息；若仍为空，检查是否启动了另一份 Codex 配置。"
            : "还没有请求到达本服务。先到「连接设置」完成配置接管，再重启 Codex。仅能打开面板不代表接入完成。",
        "hint",
      ),
    );
  for (const [i, session] of (state.sessions || []).entries()) {
    let description =
      "会话 " + (i + 1) + " · " + (phases[session.phase] || session.phase);
    if (session.upstream_pause_seconds) description += ` · 上游暂停 ${session.upstream_pause_seconds} 秒`;
    if (session.retry_after_seconds)
      description += " · 还需等待约 " + session.retry_after_seconds + " 秒";
    description += session.model ? " · " + session.model : "";
    description += session.expected_length
      ? " · 目标 " + session.expected_length
      : "";
    if (typeof session.standby === "number")
      description += ` · 备用 state ${session.standby} 张`;
    const row = textNode("div", "", "session");
    const detail = textNode("div", description);
    for (const note of [session.account_note, session.diagnostic_message])
      if (note)
        detail.append(
          textNode(
            "p",
            typeof note === "string" ? note : JSON.stringify(note),
            "hint",
          ),
        );
    row.append(detail);
    if (session.observed_length)
      detail.append(
        textNode(
          "p",
          `最近收到 ${session.observed_length} 字符；当前目标 ${session.expected_length}。`,
          "hint",
        ),
      );
    if (session.id && state.injection_enabled) {
      const retry = textNode(
        "button",
        session.cooldown_seconds > 0
          ? `等待 ${session.cooldown_seconds} 秒后再采集`
          : "重新采集",
        "secondary",
      );
      retry.dataset.retry = session.id;
      retry.dataset.locked = String(
        session.cooldown_seconds > 0 ||
          ["ready", "collecting", "auth_blocked", "rate_limited"].includes(
            session.phase,
          ),
      );
      retry.disabled = retry.dataset.locked === "true";
      retry.addEventListener("click", () =>
        action(async () => {
          if (
            !confirm(
              `重新采集会使用这个会话的账号和模型，失败会在后台连续寻找直到成功，会消耗额度；登录失效或上游限流时暂停。继续？`,
            )
          )
            return;
          try {
            const result = await api("state/retry", { id: session.id });
            notice(result.message);
          } finally {
            await refresh();
          }
        }),
      );
      row.append(retry);
    }
    $("sessions").append(row);
  }
  renderNodeResults();
 renderNodeLatency();
}
async function enter() {
  await refresh();
  await loadPool();
  sessionStorage.setItem("sleep-state-control", token);
  $("token").value = "";
  $("login").hidden = true;
  $("workspace").hidden = false;
  $("logout").hidden = false;
}
$("login-form").addEventListener("submit", (event) => {
  event.preventDefault();
  action(async () => {
    token = $("token").value.trim();
    await enter();
    clearTimeout(noticeTimer);$("notice").hidden=true;
  });
});
$("logout").addEventListener("click", () => {
  sessionStorage.removeItem("sleep-state-control");
  token = "";
  state = null;
  $("login").hidden = false;
  $("workspace").hidden = true;
  $("logout").hidden = true;
});
document.querySelectorAll("[data-page]").forEach((button) =>
  button.addEventListener("click", () => {
    document
      .querySelectorAll("[data-page]")
      .forEach((item) => item.classList.toggle("active", item === button));
    document.querySelectorAll("[data-view]").forEach((view) => {
      view.hidden = view.dataset.view !== button.dataset.page;
    });
  }),
);
$("toggle").addEventListener("click", () =>
  action(async () => {
    const result = await api("injection", {
      enabled: !state.injection_enabled,
    });
    notice(result.message);
    await refresh();
  }),
);
$("recover").addEventListener("click", () =>
  action(async () => {
    const result = await api("recover", {});
    notice(result.message);
    await refresh();
  }),
);
function sourceMode() {
  const mode = $("source-mode").value;
  const subscription = mode === "subscription" || mode === "file";
  $("subscription-options").hidden = !subscription;
  $("source-value-wrap").hidden = mode === "direct";
  $("source-label").textContent =
    {
      proxy: "节点链接（多个节点每行一个）",
      subscription: "订阅链接",
      file: "本机订阅文件的完整路径",
    }[mode] || "";
  $("source-value").placeholder =
    {
      proxy: "vless://…\nsocks5://…\nhttp://…",
      subscription: "https://… 或 http://127.0.0.1:端口/…",
      file: "C:\\Users\\你的用户名\\Downloads\\subscription.yaml",
    }[mode] || "";
  $("source-hint").textContent =
    mode === "file"
      ? "在这台电脑上读取你指定的普通文件，支持 YAML、URI 列表和 Base64 订阅。不会修改原文件。"
      : mode === "subscription"
        ? "链接可能含订阅密码，请勿截图分享。HTTP 只接受 127.0.0.1 等本机地址；远程链接必须使用 HTTPS。"
        : "支持 VLESS、VMess、SS、Trojan、Hysteria2、TUIC、HTTP / SOCKS5 等。每行一个完整链接，不隐藏内容。";
}
$("source-mode").addEventListener("change", sourceMode);
function sourceBody() {
  const split = (id) =>
    $(id)
      .value.split(/[,，]/)
      .map((value) => value.trim())
      .filter(Boolean);
  return {
    append: true,
    mode: $("source-mode").value,
    value: $("source-value").value.trim(),
    user_agent: $("user-agent").value.trim(),
    exclude_keywords: split("exclude"),
    include_protocols: split("protocols"),
  };
}
function resultAt(id, value) {
  $(id).hidden = false;
  $(id).textContent =
    value.message +
    (value.routes ? "\n共 " + value.routes.length + " 个出口配置。" : "") +
    (value.status
      ? "\nHTTP " + value.status + " · " + value.duration_ms + " ms"
      : "");
}
$("test-source").addEventListener("click", () =>
  action(async () => {
    const value = await api("sources/test", sourceBody());
    resultAt("source-result", value);
  }),
);
$("source-form").addEventListener("submit", (event) => {
  event.preventDefault();
  action(async () => {
    if (
      !confirm(
        "添加这个来源并保留现有来源？旧配置会备份；重新载入后会按候选节点重新采集。",
      )
    )
      return;
    const value = await api("sources/apply", sourceBody());
    resultAt("source-result", value);
    $("source-value").value = "";
    await loadPool();
    await refresh();
  });
});
async function copyValue(value) {
  try { await navigator.clipboard.writeText(value); notice("已复制完整链接 / 配置"); }
  catch { notice("复制未获浏览器允许，请直接选中文本复制。"); }
}
function selectionSummary() {
 const boxes = [...document.querySelectorAll("[data-candidate]")];
 const selected = boxes.filter(box => box.checked).length;
 $("selection-summary").textContent = `${selectionDirty ? "尚未保存 · " : "已保存 · "}${selected} / ${boxes.length} 个候选节点。保存勾选启用连续轮换：失败后继续下一个，全部失败则继续下一轮。`;
}
async function loadPool() {
 pool = await api("pool");
 selectionDirty = false;
 $("source-list").replaceChildren();
 for (const source of pool.sources || []) {
  const card = textNode("div", "", "pool-source");
  card.append(textNode("strong",source.kind));
  card.append(textNode("pre",source.value,"full-connection"));
  const copy=textNode("button","复制完整来源","secondary");
  copy.addEventListener("click",()=>copyValue(source.value));
  const remove=textNode("button","移除此来源","quiet");
  remove.addEventListener("click",()=>action(async()=>{
   if (!confirm("移除此来源？如果它包含唯一选中节点，请先保存其它候选节点。")) return;
   notice((await api("pool/remove-source",{id:source.id})).message);
   await loadPool();await refresh();
  }));
  card.append(copy,remove);$("source-list").append(card);
 }
 if (!pool.sources?.length) $("source-list").append(textNode("p","尚无第三方来源。可以在上方追加节点或订阅。","hint"));
 $("routes-list").replaceChildren();
 for (const route of pool.routes || []) {
  const card=textNode("div","","card node-card");card.dataset.nodeCard=route.id;
  const label=textNode("label","","node-heading");
  const checkbox=document.createElement("input");checkbox.type="checkbox";
  checkbox.dataset.candidate=route.id;checkbox.checked=route.selected;
  checkbox.addEventListener("change",()=>{selectionDirty=true;selectionSummary();});
  label.append(checkbox,textNode("strong",`${route.label || route.id} · ${route.protocol}`));card.append(label);
  const connection=route.connection || "直连（使用系统路由）";
  card.append(textNode("pre",connection,"full-connection"));
 const latency=textNode("p","尚未测试延迟","hint");latency.dataset.nodeLatency=route.id;card.append(latency);
  const results=textNode("div","","node-results");results.dataset.nodeResults=route.id;card.append(results);
  const actions=textNode("div","","actions");
  const copy=textNode("button","复制完整链接 / 配置","secondary");copy.addEventListener("click",()=>copyValue(connection));
  const test=textNode("button","测试连接","secondary");test.addEventListener("click",()=>action(async()=>{notice((await api("pool/test",{ids:[route.id],timeout_seconds:Number($("node-test-timeout").value)})).message);await refresh();}));
  const pinned=pool.pinned_route===route.id;
  const pin=textNode("button",pinned?"取消固定":"固定此节点",pinned?"quiet":"secondary");
  pin.addEventListener("click",()=>action(async()=>{notice((await api("routes/pin",{id:pinned?"":route.id})).message);await loadPool();await refresh();}));
  actions.append(copy,test,pin);card.append(actions);$("routes-list").append(card);
 }
 selectionSummary();renderNodeResults();renderNodeLatency();
}
const nodeResults = {accepted:"符合目标",shape_mismatch:"非目标 state",network_failed:"连接失败 / 超时",incomplete_response:"回复未完整结束",missing_state_header:"缺少 state",invalid_state_envelope:"state 格式异常",state_time_rejected:"state 时间异常",upstream_rejected:"上游拒绝",model_capacity:"模型繁忙",response_failed:"上游失败",upstream_rate_limited:"上游限流"};
function renderNodeResults() {
 for (const element of document.querySelectorAll("[data-node-results]")) {
  element.replaceChildren();let found=false;
  for (const [index,session] of (state?.sessions || []).entries()) {
   const record=(session.nodes || []).find(node=>node.route===element.dataset.nodeResults);
   if (!record) continue;found=true;
   const active=session.active_route===record.route ? " · 当前使用" : "";
   element.append(textNode("p",`会话 ${index+1} · ${session.model}${active} · ${nodeResults[record.result] || record.result}${record.length ? ` · 返回 ${record.length} / 目标 ${record.expected_length}` : ""}`,record.result==="accepted" ? "node-good" : "node-pending"));
   element.append(textNode("p",`${new Date(record.at).toLocaleTimeString()} · ${record.source==="probe" ? "主动采集" : "正常回复"} · HTTP ${record.http_status || "未收到响应"} · 累计观察 ${record.attempts} 次，符合 ${record.matches} 次`,"hint"));
  }
  if (!found) element.append(textNode("p","本次运行尚无该节点的模型采集记录","hint"));
 }
}
$("load-routes").addEventListener("click",()=>action(async()=>{
 if (selectionDirty && !confirm("刷新会撤销未保存的勾选，继续？")) return;
 await loadPool();
}));
$("auto-route").addEventListener("click",()=>{
 document.querySelectorAll("[data-candidate]").forEach(box=>box.checked=true);
 selectionDirty=true;selectionSummary();
});
$("save-selection").addEventListener("click",()=>action(async()=>{
 const ids=[...document.querySelectorAll("[data-candidate]:checked")].map(box=>box.dataset.candidate);
 notice((await api("pool/select",{ids})).message);
 await loadPool();await refresh();
}));
$("reload-pool").addEventListener("click",()=>action(async()=>{
 if (!confirm("重新下载订阅并载入节点？现有 state 会清空；已保存的勾选会保留。")) return;
 notice((await api("pool/reload",{})).message);await loadPool();await refresh();
}));
if (launchTicket)
  action(async () => {
    const value = await api("launch", { ticket: launchTicket });
    token = value.control_token;
    await enter();
  });
else if (token) action(enter);
setInterval(() => {
  if (token && !busy && !document.hidden)
    refresh().catch((error) => notice(error.message));
}, 2000);

$("preferences-form").addEventListener("submit", (event) => {
  event.preventDefault();
  action(async () => {
    const result = await api("preferences", {
      model: $("model-select").value,
      account_mode: $("account-select").value,
      state_fallback: $("fallback-select").value,
    });
    preferencesDirty = false;
    notice(result.message);
    await refresh();
  });
});
$("inspect-recovery").addEventListener("click", () =>
  action(async () => {
    recovery = await api("recovery/preview", {});
    $("recovery-panel").hidden = false;
    $("recovery-summary").textContent = recovery.message;
    $("keep-current").disabled = !recovery.can_keep_current;
    $("restore-backup").disabled = !recovery.can_restore_backup;
  }),
);
async function repair(mode) {
  if (!recovery) return;
  const question =
    mode === "restore_backup"
      ? "恢复接管前的备份？这会撤销之后对 Codex 配置的修改。当前文件会独立备份。"
      : "保留 CCS 当前选择，移除能确认属于旧版本的配置，再重新接管？当前文件会先备份。";
  if (!confirm(question)) return;
  const result = await api("recovery/apply", {
    mode,
    expected_config_sha256: recovery.config_sha256,
    expected_transaction_sha256: recovery.transaction_sha256,
  });
  recovery = null;
  $("recovery-panel").hidden = true;
  notice(result.message);
  await refresh();
}
$("keep-current").addEventListener("click", () =>
  action(() => repair("keep_current")),
);
$("restore-backup").addEventListener("click", () =>
  action(() => repair("restore_backup")),
);
$("download-diagnostics").addEventListener("click", () =>
  action(async () => {
    const value = await api("diagnostics", {});
    const url = URL.createObjectURL(
      new Blob([JSON.stringify(value, null, 2)], { type: "application/json" }),
    );
    const a = document.createElement("a");
    a.href = url;
    a.download = "sleep-state-diagnostics.json";
    a.click();
    setTimeout(() => URL.revokeObjectURL(url), 1000);
  }),
);

$("reset-service-config").addEventListener("click", () =>
  action(async () => {
    const preview = await api("service-config/preview", {});
    if (!confirm(preview.message)) return;
    const result = await api("service-config/reset", {
      expected_config_sha256: preview.config_sha256,
    });
    notice(result.message);
  }),
);

$("rebuild-kind").addEventListener("change", () => {
  $("rebuild-relay").hidden = $("rebuild-kind").value !== "relay";
});
$("rebuild-form").addEventListener("submit", (event) => {
  event.preventDefault();
  action(async () => {
    const preview = await api("codex-config/preview", {});
    if (
      !confirm(
        "将完整备份旧 Codex 配置，再按所选账号重建最小配置。原有插件、MCP 和其他设置只保留在备份中，不会自动迁移。确认继续？",
      )
    )
      return;
    const value = await api("codex-config/rebuild", {
      kind: $("rebuild-kind").value,
      upstream: $("rebuild-url").value.trim(),
      env_key: $("rebuild-env").value.trim(),
      expected_config_sha256: preview.config_sha256,
      expected_exists: preview.exists,
    });
    $("rebuild-url").value = "";
    notice(value.message);
    await refresh();
  });
});

document.querySelectorAll("[data-jump]").forEach((button) =>
  button.addEventListener("click", () => {
    document.querySelector(`[data-page="${button.dataset.jump}"]`).click();
  }),
);

$("quick-setup").addEventListener("click", () =>
  action(async () => {
    if (
      !confirm(
        "自动备份并接入当前 Codex，检查常见本地代理；已有订阅和账号不重置。采不到合格 state 时先普通转发。开启注入后的模型请求可能触发有限采集并消耗额度。继续？",
      )
    )
      return;
    try {
      const value = await api("quick-setup", {});
      notice(value.message);
    } finally {
      await refresh();
    }
  }),
);

for (const id of ["model-select", "account-select", "fallback-select"])
  $(id).addEventListener("change", () => {
    preferencesDirty = true;
  });

for (const id of Object.values(timingFields)) {
  $(id).addEventListener("input", () => { timingDirty = true; timingSummary(); });
}
$("timing-defaults").addEventListener("click", () => {
  if (!state) return;
  timingDirty = true; fillTiming(state.timing_defaults);
});
$("timing-low-frequency").addEventListener("click", () => {
  if (!state) return;
  timingDirty = true;
  fillTiming({ ...state.timing_defaults, probe_cooldown_seconds: 600, max_probes_per_round: 2 });
});
$("timing-cancel").addEventListener("click", () => {
  if (!state) return;
  timingDirty = false; fillTiming(state.timing);
});
$("timing-form").addEventListener("submit", (event) => {
  event.preventDefault();
  timingSummary();
  if (!$("timing-form").reportValidity()) return;
  const next = readTiming();
  if (!confirm("保存采集设置？旧配置会备份，现有 state 缓存会清空。无需重启 Codex；后续请求可能重新采集并消耗额度。")) return;
  action(async () => {
    const result = await api("timing", next);
    // Do not discard edits typed while the save request was in flight.
    timingDirty = JSON.stringify(readTiming()) !== JSON.stringify(next);
    $("timing-result").textContent = result.message;
    await refresh();
  });
});

function renderNodeLatency() {
 const tests=state?.node_tests;
 $("node-test-progress").textContent=tests ? `${tests.running ? "正在测试" : "测试任务已结束"} · ${tests.completed} / ${tests.total}；勾选更改后需点“保存勾选”。` : "尚未开始";
 for (const element of document.querySelectorAll("[data-node-latency]")) {
  const result=tests?.results?.[element.dataset.nodeLatency];
  element.textContent=result ? `${result.message}${result.reachable ? ` · ${result.duration_ms} ms · HTTP ${result.status}` : ""}${result.at && !result.at.startsWith("0001") ? ` · ${new Date(result.at).toLocaleTimeString()}` : ""}` : "尚未测试延迟";
  element.className=result?.selectable ? "node-good" : "hint";
 }
}
async function testNodes(onlySelected) {
 const ids=onlySelected ? [...document.querySelectorAll("[data-candidate]:checked")].map(box=>box.dataset.candidate) : [];
 if (onlySelected && !ids.length) throw new Error("请先勾选需要测试的节点");
 notice((await api("pool/test",{ids,timeout_seconds:Number($("node-test-timeout").value)})).message);await refresh();
}
$("test-all-nodes").addEventListener("click",()=>action(()=>testNodes(false)));
$("test-selected-nodes").addEventListener("click",()=>action(()=>testNodes(true)));
$("stop-node-tests").addEventListener("click",()=>action(async()=>{notice((await api("pool/test-stop",{})).message);await refresh();}));
$("select-reachable").addEventListener("click",()=>{
 if (state?.node_tests?.running) {notice("请等待测试结束，或先停止测试");return;}
 const results=state?.node_tests?.results || {};
 document.querySelectorAll("[data-candidate]").forEach(box=>box.checked=results[box.dataset.candidate]?.selectable===true);
 selectionDirty=true;selectionSummary();
});
$("sort-latency").addEventListener("click",()=>{
 const results=state?.node_tests?.results || {};
 const score=card=>{const r=results[card.dataset.nodeCard];return r?.selectable ? r.duration_ms : Infinity;};
 [...document.querySelectorAll("[data-node-card]")].sort((a,b)=>score(a)-score(b)).forEach(card=>$("routes-list").append(card));
});
