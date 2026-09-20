"use strict";
const $ = (id) => document.getElementById(id);
const launchTicket = new URLSearchParams(location.hash.slice(1)).get("launch");
if (location.hash) history.replaceState(null, "", location.pathname);
let token = sessionStorage.getItem("sleep-state-control") || "";
let state = null;
const pendingButtons = new Set();
let refreshing = null;
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
const collectionFields = {
  standby_target: "standby-target", standby_spacing_seconds: "standby-spacing",
  failure_interval_seconds: "failure-interval", failure_max_interval_seconds: "failure-max-interval",
  search_budget: "search-budget", search_pause_seconds: "search-pause",
  hourly_budget: "hourly-budget", idle_seconds: "collection-idle",
};
function readTiming() {
  return { ...Object.fromEntries(Object.entries(timingFields).map(([key, id]) => [key, Number($(id).value)])),
    collection: {cadence:$("collection-cadence").value, ...Object.fromEntries(Object.entries(collectionFields).map(([key, id]) => [key, Number($(id).value)]))} };
}
function timingSummary() {
  const t = readTiming();
  $("refresh-before").setCustomValidity(t.refresh_before_seconds >= t.state_ttl_seconds ? "提前刷新时间必须小于本地有效期" : "");
  const c = t.collection;
  const round = c.cadence === "round";
  for (const id of ["failure-interval", "failure-max-interval", "search-budget", "search-pause"]) $(id).parentElement.hidden = round;
  $("failure-max-interval").setCustomValidity(c.failure_max_interval_seconds < c.failure_interval_seconds ? "最大间隔不得小于初始间隔" : "");
  const retryStart = c.failure_interval_seconds === 0 ? "首次失败立即重试一次，此后从 30 秒起翻倍等待" : `失败从 ${c.failure_interval_seconds} 秒起翻倍等待`;
  const cadence = round ? `默认轮次：每批最多尝试 ${t.max_probes_per_round} 个节点，单次最长 ${t.probe_timeout_seconds} 秒；不论有无主用，每批结束后等待 ${t.probe_cooldown_seconds} 秒；可手动随机换节点立即采集一次；节点每轮随机排序，不重复，全部可用节点走完后才开启下一轮。不叠加逐次退避和连续失败暂停。` : `${retryStart}，最多 ${c.failure_max_interval_seconds} 秒；连续失败 ${c.search_budget} 次暂停 ${c.search_pause_seconds} 秒。`;
  $("timing-summary").textContent = `${timingDirty ? "尚未保存 · " : "已保存 · "}${cadence}所有会话合计滚动一小时最多 ${c.hourly_budget} 次额外采集；登录拒绝或限流会暂停；模型容量不足继续下一节点。备用目标 ${c.standby_target} 张，主用加备用达到 ${c.standby_target+1} 张后停止；按取得顺序使用，出现空位才补。按轮次及至少 ${c.standby_spacing_seconds} 秒的错峰间隔补充后续备用；聊天期间也按上述节奏补采；无 AI 在途请求且空闲 ${c.idle_seconds} 秒后暂停，面板与模型列表不延长计时。`;
}
function fillTiming(t) {
  if (!t) return;
  $("collection-cadence").value = t.collection?.cadence || "backoff";
  for (const [key, id] of Object.entries(timingFields)) $(id).value = t[key];
  for (const [key, id] of Object.entries(collectionFields)) $(id).value = t.collection?.[key] ?? "";
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
  const button = document.activeElement?.closest("button");
  if (button && pendingButtons.has(button)) return;
  if (button) {
    pendingButtons.add(button);
    button.disabled = true;
  }
  try {
    await fn();
  } catch (error) {
    notice(error.message);
  } finally {
    if (button) {
      pendingButtons.delete(button);
      button.disabled = false;
    }
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
    pendingButtons.forEach(button => { button.disabled = true; });
  }
}
const phases = {
  ready: "主用符合本地规则，未到期",
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
function refresh() {
  if (!refreshing) refreshing = refreshStatus().finally(() => { refreshing = null; });
  return refreshing;
}
async function refreshStatus() {
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
  $("toggle").setAttribute("aria-checked", String(state.injection_enabled));
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
  const slots=state.request_slots;
 $("request-slots").textContent=slots ? `聊天 / 压缩：处理中 ${slots.generation_active}/${slots.generation_limit} · 排队 ${slots.generation_waiting}；生图等带正文请求：处理中 ${slots.feature_active}/${slots.feature_limit} · 排队 ${slots.feature_waiting}` : "";
 $("recent-requests").replaceChildren();
  for (const item of (traffic.recent || []).slice(-5).reverse()) {
    $("recent-requests").append(
      textNode(
        "div",
        `${new Date(item.at).toLocaleTimeString()} · ${item.kind} · HTTP ${item.status} · ${item.duration_ms} ms · ${({completed:"完成",failed:"失败",unverified:"未确认完成",cancelled:"已取消",http_success:"接口已响应"})[item.result]||"仅HTTP记录"}${item.error_code ? " · "+item.error_code : ""}${item.route_label ? " · "+item.route_label : ""}`,
        "hint",
      ),
    );
  }
  $("warnings").replaceChildren();
  for (const message of [state.config_error, state.route_error, state.pool_error])
    if (message) $("warnings").append(textNode("div", message, "warning"));
  const expandedSessions = new Set([...$("sessions").querySelectorAll("[data-session-details][open]")].map(el => el.dataset.sessionDetails));
  const focusedSession = document.activeElement?.closest("[data-session-details]")?.dataset.sessionDetails;
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
  const sessionRows = (state.supported_models || []).flatMap((model, modelIndex) => {
    const matches = (state.sessions || []).filter(session => session.model === model).sort((a, b) => a.id.localeCompare(b.id));
    return matches.length ? matches.map((session, index) => ({session, number:matches.length === 1 ? String(modelIndex + 1) : `${modelIndex + 1}.${index + 1}`})) : [{session:{model}, number:String(modelIndex + 1)}];
  });
  for (const {session, number} of sessionRows) {
    if (!session.id) {
      const row = textNode("div", "", "session");
      row.dataset.sessionModel = session.model;
      const detail = textNode("div", "", "session-detail");
      const heading = textNode("div", "", "session-heading");
      heading.append(textNode("strong", `会话 ${number} · ${session.model}`, "session-title"), textNode("span", "等待首次请求", "session-badge"));
      detail.append(heading, textNode("p", "主用 0 · 备用 0 · 暂无失败记录", "session-empty"));
      row.append(detail);
      $("sessions").append(row);
      continue;
    }
    let description =
      "会话 " + number + " · " + (phases[session.phase] || session.phase);
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
    row.dataset.sessionModel = session.model;
    const heading = textNode("div", "", "session-heading");
    const phaseNames = {ready:"主用未到期",search_queued:"等待采集",upstream_paused:"上游暂停",collecting:"正在采集",waiting_for_state:"等待主用",auth_blocked:"登录 / 权限异常",rate_limited:"上游限流"};
    heading.append(textNode("strong", `会话 ${number} · ${session.model}`, "session-title"), textNode("span", phaseNames[session.phase] || phases[session.phase] || session.phase, `session-badge ${session.usable ? "is-ready" : "is-pending"}`));
    heading.append(textNode("span", `目标 ${session.expected_length || "—"} · 备用 ${session.standby || 0}`, "session-meta"));
    row.append(heading);
    const detail = textNode("details", "", "session-diagnostics");
    detail.dataset.sessionDetails = session.id;
    detail.open = expandedSessions.has(session.id);
    detail.append(textNode("summary", "时间与诊断详情"), textNode("p", description, "hint"));
    const routeName = id => (state.pool_nodes || []).find(r => r.id === id)?.label || (pool?.routes || []).find(r => r.id === id)?.label || (session.states || []).find(r => r.route_id === id)?.route_label || id;
    // Collector diagnostics describe its candidate, not the card being used.
    for (const note of [session.account_note, !session.usable && session.diagnostic_message])
      if (note)
        detail.append(
          textNode(
            "p",
            typeof note === "string" ? note : JSON.stringify(note),
            "hint",
          ),
        );
    const cards = textNode("div", "", "state-cards");
    let reserveIndex=0;
    for (const card of session.states || []) {
      const role=card.role==="active"?"主用 · 第 1 张":card.role==="parked"?"来源停用 · 暂不使用":`备用 ${++reserveIndex} · 第 ${reserveIndex+1} 张`;
      const item = textNode("div", "", "state-card");
      const cardHeading = textNode("div", "", "state-card-heading");
      cardHeading.append(textNode("strong", `${role} · ${card.id.slice(0, 8)}`));
      const left = textNode("span", `剩余 ${briefDuration(card.remaining_seconds)}`, "state-countdown");
      left.dataset.expires = card.expires_at;
      left.dataset.compactTime = "true";
      cardHeading.append(left);
      const source = textNode("p", card.route_label || card.route_id, "state-source");
      source.title = `来源节点：${card.route_label || card.route_id}`;
      const times = textNode("p", `${stateClock(card.acquired_at)} 取得 · ${stateClock(card.expires_at)} 预计到期`, "state-times");
      times.title = `取得：${stateTime(card.acquired_at)}；本地预计到期：${stateTime(card.expires_at)}`;
      item.append(cardHeading, source, times);
      if (card.role === "active") {
        const checked = session.last_state_check;
        const sameCard = checked?.state_id === card.id;
        const evidence = sameCard && checked.result === "header_missing" ? "最近回复未带 state · 无法核验" : sameCard && checked.result === "header_accepted" ? "最近回复符合本地规则" : "符合本地规则 · 尚无当前牌的核验记录";
        item.append(textNode("p", evidence, "state-evidence"));
      }
      if(card.role==="parked") detail.append(item);else cards.append(item);
      const full = textNode("div", "", "state-full-detail");
      full.append(textNode("strong", `${role} · ${card.id.slice(0, 8)} · ${card.route_label || card.route_id}`));
      full.append(textNode("p", `取得：${stateTime(card.acquired_at)} · 已取得 ${card.acquired_at?.startsWith("0001") ? "未知" : duration(card.age_seconds)}；签发：${stateTime(card.issued_at)}；本地预计到期：${stateTime(card.expires_at)}`));
      const measured=state.node_tests?.results?.[card.route_id];if(measured?.exit_ip)full.append(textNode("p",`最近测速出口：${measured.exit_ip} · ${stateTime(measured.at)}（测速时观测）`,"hint"));
 detail.append(full);
    }
    if ((session.states || []).length) {
      row.append(cards);
      detail.append(textNode("p", "state 使用时固定来源节点；到期前 30 秒停止接入新请求。", "hint"));
    } else {
      const boot = session.last_state_check?.bootstrap;
      const awaiting = session.last_state_check?.result === "header_accepted" && boot && boot !== "saved_active";
      row.append(textNode("p", awaiting ? `返回长度符合，但尚未入库（${({pending:"等待回复完整结束",incomplete:"回复未完整结束",unverified_stream:"旧版未核验响应格式",unsupported_encoding:"不支持的响应编码",invalid_encoding:"响应压缩数据损坏",node_paused:"来源节点暂停"})[boot] || boot}）` : session.diagnostic_message || "尚无可用主 state", "session-empty"));
    }
	const check = session.last_state_check;
	if (check?.at && !check.at.startsWith("0001")) {
	  const checkNames = {header_unverified:"响应未确认完成，未据此撤牌",pending_response:"正在接收回复，完成后核验",header_missing:"响应未带 state 头，本次无法核验；不能据此判定失效",header_accepted:"响应头符合当前长度与时间规则（不证明服务端认可或模型质量）",header_rejected:"本次返回值不合格、不入池；若请求使用了主用牌，该牌已撤下，有备用则切换",upstream_error:"上游返回错误，不能证明 state 失效；原牌保留",network_failed:"连接失败，不能证明 state 失效；原牌和来源保留"};
	  detail.append(textNode("p", `最近正式回复的 state 检查：${stateTime(check.at)} · ${checkNames[check.result] || check.result} · ${check.route_label || check.route_id}${check.state_id ? ` · state ${check.state_id.slice(0,8)}` : ""}`, "hint"));
	  const bootstrapNames = {pending:"没有主用，等待本次回复完整结束后收取",saved_active:"首次收牌：已从普通回复取得主用，绑定本次出口；未增加备用或额外探测",not_needed:"收牌时已有可用主用，保留原牌，不覆盖、不增加备用",node_paused:"本次来源节点已暂停，未收取主用",incomplete:"本次回复未满足完整结束条件，未收取主用",unverified_stream:"响应格式未获旧版核验，未入库",unsupported_encoding:"响应编码不受支持，未核验完整性，未入库",invalid_encoding:"响应压缩数据损坏，未入库"};
	  if (check.bootstrap) detail.append(textNode("p", bootstrapNames[check.bootstrap] || check.bootstrap, "hint"));
	} else detail.append(textNode("p", "最近正式回复的 state 检查：尚无记录", "hint"));
    if (session.collection) {
      const c = session.collection;
      const reasons = {pool_ready:"已满额，自动采集已停止；按取得顺序使用，出现空位才补", missing_active:"没有主用，继续寻找剩余节点", missing_standby:"准备补第一张备用", standby_spacing:"备用错峰等待", standby_refresh:"等待更新最早到期的备用", refresh_active:"等待更新主用", success_cooldown:"成功后冷却", upstream_pause:"上游要求暂停采集", failure_interval:"失败后退避", search_budget:"连续失败预算暂停", hourly_budget:"每小时总预算暂停", idle:"空闲暂停，收到新请求后恢复", budget_storage_error:"预算记录异常，额外采集停止"};
      const failureNames = {shape_mismatch:"state 长度不符合", state_time_rejected:"state 过期或时间异常", duplicate_state:"返回重复 state", network_failed:"连接失败或超时", missing_state_header:"未返回 state", invalid_state_envelope:"state 格式错误", incomplete_response:"回复未完整结束", upstream_rejected:"上游拒绝", model_capacity:"模型容量不足", upstream_rate_limited:"上游限流", response_failed:"上游回复失败"};
      if (c.last_failure_at && !c.last_failure_at.startsWith("0001")) {
        const brief = textNode("p", "最近补采失败：", "session-failure");
        const briefAge = textNode("span", `${briefDuration(c.last_failure_ago_seconds)}前`);
        briefAge.dataset.since = c.last_failure_at;
        briefAge.dataset.compactTime = "true";
        brief.append(briefAge, textNode("span", ` · ${routeName(c.last_failure_route)} · ${failureNames[c.last_failure_reason] || c.last_failure_reason} · 候选未入池`));
        brief.title = `${stateTime(c.last_failure_at)} · ${routeName(c.last_failure_route)}`;
        row.append(brief);
        const failed = textNode("p", "", "hint");
        failed.append(textNode("span", `最近补采失败：${stateTime(c.last_failure_at)} · `));
        const age = textNode("span", `${duration(c.last_failure_ago_seconds)}前`);
        age.dataset.since = c.last_failure_at;
        failed.append(age, textNode("span", ` · ${routeName(c.last_failure_route)} · ${failureNames[c.last_failure_reason] || c.last_failure_reason}`));
        detail.append(failed);
      } else detail.append(textNode("p", "最近补采失败：暂无记录", "hint"));
      const use = c.last_use_failure;
      if (use?.at && !use.at.startsWith("0001")) {
        const failed = textNode("p", `最近 state 使用失败：${stateTime(use.at)} · `, "hint");
        const age = textNode("span", ""); age.dataset.since = use.at;
        failed.append(age, textNode("span", ` · ${routeName(use.route)} · ${failureNames[use.reason] || use.reason} · state ${use.state_id?.slice(0,8)}`));
        detail.append(failed);
      }
      reasons.round_cooldown = "本批结束，等待下一批采集";
      const rotation = c.rotation;
      if (rotation?.cycle > 0) {
        row.append(textNode("p", `随机轮询 · 第 ${rotation.cycle} 轮 · 已尝试 ${rotation.completed}/${rotation.total} 个节点`, "hint"));
      }
      reasons.collecting = "正在补采；聊天回复与采集独立进行";
      const failures = c.cadence === "round" ? `${c.failures} 次（默认轮次）` : `${c.failures}/${c.search_budget} 次`;
      detail.append(textNode("p", `全程序最近一小时额外采集 ${c.hourly_used}/${c.hourly_budget} 次；本会话连续失败 ${failures}。${reasons[c.reason] || c.reason}${!c.idle && c.wait_seconds > 0 ? `，约 ${duration(c.wait_seconds)} 后可采集（${stateTime(c.next_at)}）` : ""}。`, "hint"));
    }
    if(session.state_events?.length) {
      const names={acquired_main:"取得主用",acquired_standby:"取得备用",promoted:"接替为主用",expired:"已到期",invalidated:"收到明确不合格结果，已撤下",source_suspended:"来源停用，暂停使用",source_resumed:"来源恢复，重新入队"};
      const history=textNode("details","","state-history");history.append(textNode("summary","卡片变更记录"));
      for(const event of session.state_events.slice(-12).reverse())history.append(textNode("p",`${stateClock(event.at)} · ${event.state_id.slice(0,8)} · ${names[event.kind]||event.kind}`,"hint"));
      detail.append(history);
    }
    const probe = session.last_probe_result;
    if (probe) detail.append(textNode("p", `最近主动采集：${stateTime(probe.at)} · ${routeName(probe.route)} · ${nodeResults[probe.result] || probe.result}${probe.length ? ` · 返回 ${probe.length} / 目标 ${probe.expected_length}` : ""}。此处是采集候选的结果，当前主用的核验请看“最近正式回复”。`, "hint"));
    if (session.id && state.injection_enabled) {
      const randomOnce = textNode("button", "随机换节点采集一次", "secondary");
      randomOnce.title = "跳过本地等待，随机选择另一个可用采集节点；保留主卡、聊天出口和固定设置。仍遵守上游暂停与小时预算。";
      randomOnce.dataset.locked = String(["collecting", "auth_blocked", "rate_limited", "upstream_paused"].includes(session.phase) || ["collecting","pool_ready"].includes(session.collection?.reason));
      randomOnce.disabled = randomOnce.dataset.locked === "true";
      randomOnce.addEventListener("click", () => action(async () => {
        try { notice((await api("state/retry", {id:session.id, random_once:true})).message); }
        finally { await refresh(); }
      }));
      row.append(randomOnce);
      const retry = textNode(
        "button",
        session.collection?.reason==="pool_ready" ? "主备已满 · 停止采集" : session.cooldown_seconds > 0
          ? `${session.cooldown_seconds} 秒后可采集`
          : "重新采集",
        "secondary",
      );
      retry.dataset.retry = session.id;
      retry.dataset.locked = String(
        session.collection?.reason==="pool_ready" ||
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
              `重新采集会使用这个会话的账号和模型，失败会按间隔和预算自动补采，会消耗额度；登录失效或上游限流时暂停。继续？`,
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
      heading.append(retry);
    }
    row.append(detail);
    $("sessions").append(row);
  }
  if (focusedSession) [...$("sessions").querySelectorAll("[data-session-details]")].find(el => el.dataset.sessionDetails === focusedSession)?.querySelector("summary")?.focus({preventScroll:true});
  renderNodeResults();
 renderNodeLatency();
  if (typeof loadLifecyclePool === "function" && !document.querySelector('[data-view="pool"]').hidden)
    await loadLifecyclePool();
  pendingButtons.forEach(button => { button.disabled = true; });
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
      .forEach((item) => { item.classList.toggle("active", item === button); if(item === button) item.setAttribute("aria-current","page"); else item.removeAttribute("aria-current"); });
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
 $("node-toolbar").classList.toggle("is-dirty", selectionDirty);
 for (const card of document.querySelectorAll("[data-node-card]")) card.classList.toggle("is-selected", card.querySelector("[data-candidate]").checked);
 const boxes = [...document.querySelectorAll("[data-candidate]")];
 const selected = boxes.filter(box => box.checked).length;
 $("selection-summary").textContent = `${selectionDirty ? "尚未保存 · " : "已保存 · "}${selected} / ${boxes.length} 个候选节点。确认 11 块（312）不符合当前模型目标时临时暂停并轮询下一节点；最后可用节点保留普通转发，其他节点到期后恢复轮询。连接失败或超时移入失败列表，须手动恢复。`;
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
  checkbox.addEventListener("change",()=>{selectionDirty=true;selectionSummary();filterNodes();});
  card.classList.toggle("is-pinned",pool.pinned_route===route.id);
  label.append(checkbox,textNode("strong",route.label || route.id),textNode("span",route.protocol,"node-protocol"));card.append(label);
  const connection=route.connection || "直连（使用系统路由）";
  const config=textNode("details","","node-connection");config.append(textNode("summary","查看完整连接"),textNode("pre",connection,"full-connection"));card.append(config);
 const latency=textNode("p","尚未测试延迟","hint");latency.dataset.nodeLatency=route.id;card.append(latency);
  const results=textNode("div","","node-results");results.dataset.nodeResults=route.id;card.append(results);
  const actions=textNode("div","","actions");
  const copy=textNode("button","复制链接","secondary");copy.addEventListener("click",()=>copyValue(connection));
  const test=textNode("button","测试连接","secondary");test.addEventListener("click",()=>action(async()=>{notice((await api("pool/test",{ids:[route.id],timeout_seconds:Number($("node-test-timeout").value)})).message);await refresh();}));
  const pinned=pool.pinned_route===route.id;
  const pin=textNode("button",pinned?"取消固定":"固定此节点",pinned?"quiet":"secondary");
  pin.addEventListener("click",()=>action(async()=>{notice((await api("routes/pin",{id:pinned?"":route.id})).message);await loadPool();await refresh();}));
  actions.append(copy,test,pin);card.append(actions);$("routes-list").append(card);
 }
 selectionSummary();renderNodeResults();renderNodeLatency();filterNodes();
}
const nodeResults = {accepted:"符合目标",shape_mismatch:"非目标 state",network_failed:"连接失败 / 超时",incomplete_response:"回复未完整结束",missing_state_header:"缺少 state",invalid_state_envelope:"state 格式异常",state_time_rejected:"state 时间异常",upstream_rejected:"上游拒绝",model_capacity:"模型繁忙",response_failed:"上游失败",upstream_rate_limited:"上游限流"};
function renderNodeResults() {
 for (const element of document.querySelectorAll("[data-node-results]")) {
  element.replaceChildren();let found=false;
  const node=(state?.pool_nodes || []).find(n=>n.id===element.dataset.nodeResults);
  if(node?.state==="failed") {
   element.append(textNode("p","连接失败 / 超时 · 停止自动采集；已有卡保留原出口","node-pending"));
   const restore=textNode("button","手动恢复节点","secondary");restore.addEventListener("click",()=>action(async()=>{
    const session=state?.sessions?.[0], id=element.dataset.nodeResults;
    const result=session ? await api("nodes/resume",{session_id:session.id,route_id:id}) : await api("pool/change",{ids:[id],state:"available"});
    notice(result.message);await refresh();
   }));element.append(restore);continue;
  }
  for (const session of state?.sessions || []) {
   const record=(session.nodes || []).find(node=>node.route===element.dataset.nodeResults);
   if (!record) continue;found=true;
   const active=session.active_route===record.route ? " · 当前使用" : "";
   const number = (state.supported_models || []).indexOf(session.model)+1;
   element.append(textNode("p",`会话 ${number} · ${session.model}${active} · ${nodeResults[record.result] || record.result}${record.length ? ` · 返回 ${record.length} / 目标 ${record.expected_length}` : ""}`,record.result==="accepted" ? "node-good" : "node-pending"));
   if (record.pool_state === "failed") {
    element.append(textNode("p","连接失败 · 自动采集需手动恢复，已有卡不删除","node-pending"));
    const restore=textNode("button","手动恢复节点","secondary");restore.addEventListener("click",()=>action(async()=>{notice((await api("nodes/resume",{session_id:session.id,route_id:record.route})).message);await refresh();}));element.append(restore);
   } else if (record.pause_seconds > 0) {
    const pause = textNode("p",`暂停新采集 · 返回 11 块（312） · ${duration(record.pause_seconds)}后自动恢复`,"node-pending");
    pause.dataset.nodePauseUntil = record.pause_until;
    element.append(pause);
    const resume = textNode("button", "解除暂停", "secondary");
    resume.dataset.resumeNode = record.route;
    resume.addEventListener("click",()=>action(async()=>{notice((await api("nodes/resume",{session_id:session.id,route_id:record.route})).message);await refresh();}));
    element.append(resume);
   } else if (record.retained_last) element.append(textNode("p","最后可用节点 · 保留普通转发，不暂停；312 不作为合格 state。其他节点恢复后继续轮询。","node-pending"));
   else if (record.pause_until && !record.pause_until.startsWith("0001")) element.append(textNode("p","暂停已结束 · 已恢复参与轮询","node-good"));
   else element.append(textNode("p","当前可参与采集轮询","hint"));
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
  for (const el of document.querySelectorAll("[data-node-pause-until]")) {
    const left = Math.max(0, Math.ceil((Date.parse(el.dataset.nodePauseUntil)-Date.now())/1000));
    el.textContent = left ? `暂停新采集 · 返回 11 块（312） · ${duration(left)}后自动恢复` : "暂停已结束 · 已恢复参与轮询";
  }
  if (token && !document.hidden)
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

for (const id of ["collection-cadence", ...Object.values(timingFields), ...Object.values(collectionFields)]) {
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
  if (!confirm("保存采集设置？旧配置会备份，现有 state 和连接保留，预算不重置。无需重启 Codex；有效期或备用上限变更可能淘汰部分 state。")) return;
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
  element.textContent=result ? `${result.message}${result.checks?.length ? " · "+result.checks.map(c=>`${c.target} ${c.ok ? "✓" : "×"} ${c.duration_ms} ms${c.status ? ` · HTTP ${c.status}` : ""}`).join(" / ") : ""}${result.exit_ip ? " · 出口 "+result.exit_ip : ""}${result.reachable && !result.checks?.length ? ` · ${result.duration_ms||0} ms · HTTP ${result.status}` : ""}${result.at && !result.at.startsWith("0001") ? ` · ${new Date(result.at).toLocaleTimeString()}` : ""}` : "尚未测试延迟";
  element.className=result?.selectable ? "node-good" : "hint";
 }
 filterNodes();
}
async function testNodes(onlySelected, recoverFailed=false) {
 const ids=onlySelected ? [...document.querySelectorAll("[data-candidate]:checked")].map(box=>box.dataset.candidate) : [];
 if (onlySelected && !ids.length) throw new Error("请先勾选需要测试的节点");
 notice((await api("pool/test",{ids,timeout_seconds:Number($("node-test-timeout").value),target:$("node-test-target").value,include_exit:$("node-test-exit").checked,recover_failed:recoverFailed})).message);await refresh();
}
$("retest-failed-nodes").addEventListener("click",()=>action(()=>testNodes(false,true)));
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

function stateTime(value) {
  if (!value || value.startsWith("0001")) return "未知（旧备份未记录）";
  const date = new Date(value);
  return Number.isNaN(date.getTime()) ? "未知" : date.toLocaleString("zh-CN", {hour12:false});
}
function duration(seconds) {
  const n = Math.max(0, Math.floor(seconds || 0));
  return `${Math.floor(n / 3600)}时${Math.floor(n % 3600 / 60)}分${n % 60}秒`;
}
function briefDuration(seconds) {
  const n = Math.max(0, Math.floor(seconds || 0));
  if (n >= 3600) return `${Math.floor(n / 3600)}时${Math.floor(n % 3600 / 60)}分`;
  if (n >= 60) return `${Math.floor(n / 60)}分${n % 60}秒`;
  return `${n}秒`;
}
function stateClock(value) {
  if (!value || value.startsWith("0001")) return "未知时间";
  const date = new Date(value);
  return Number.isNaN(date.getTime()) ? "未知时间" : date.toLocaleTimeString("zh-CN", {hour12:false});
}
setInterval(() => {
  for (const el of document.querySelectorAll("[data-since]")) el.textContent = `${(el.dataset.compactTime ? briefDuration : duration)((Date.now() - Date.parse(el.dataset.since))/1000)}前`;
  for (const el of document.querySelectorAll("[data-expires]")) {
    const left = Math.max(0, Math.floor((Date.parse(el.dataset.expires) - Date.now()) / 1000));
    el.textContent = left > 30 ? `剩余 ${(el.dataset.compactTime ? briefDuration : duration)(left)}` : "即将到期 · 停用";
  }
}, 1000);

function filterNodes() {
 const query=$("node-search").value.trim().toLocaleLowerCase(), view=$("node-view").value;
 const cards=[...document.querySelectorAll("[data-node-card]")]; let visible=0;
 for(const card of cards) {
  const id=card.dataset.nodeCard, box=card.querySelector("[data-candidate]");
  const matches=card.querySelector(".node-heading").textContent.toLocaleLowerCase().includes(query);
  card.hidden=!(matches && (view!=="selected" || box.checked) && (view!=="reachable" || state?.node_tests?.results?.[id]?.selectable));
  if(!card.hidden) visible++;
 }
 $("node-visible-count").textContent=`显示 ${visible} / ${cards.length}`;
 $("node-search-empty").hidden=visible!==0;
}
$("node-search").addEventListener("input",filterNodes);
$("node-view").addEventListener("change",filterNodes);
for(const [id,checked] of [["select-visible-nodes",true],["unselect-visible-nodes",false]]) $(id).addEventListener("click",()=>{
 for(const card of document.querySelectorAll("[data-node-card]")) if(!card.hidden) card.querySelector("[data-candidate]").checked=checked;
 selectionDirty=true;selectionSummary();filterNodes();
});
for(const button of document.querySelectorAll("[data-node-view]")) button.addEventListener("click",()=>{
 $("routes-list").classList.toggle("is-list",button.dataset.nodeView==="list");
 document.querySelectorAll("[data-node-view]").forEach(b=>b.classList.toggle("active",b===button));
});
