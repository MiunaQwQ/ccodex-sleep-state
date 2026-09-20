"use strict";
const trafficResultNames = {completed:"完整返回",http_success:"接口已返回",failed:"失败",cancelled:"已取消",unverified:"未确认完成"};
const trafficPhaseNames = {received:"已收到 · 本地处理中",waiting_upstream:"已发起请求 · 等待上游",receiving:"正在返回"};
function requestStatus(item) {
  return item.phase === "finished" ? trafficResultNames[item.result] || "未确认完成" : trafficPhaseNames[item.phase] || "已收到";
}
const trafficTerminationNames = {
  response_complete:"完成标记已转发",closed_after_complete:"完整返回后关闭连接",
  client_disconnected:"客户端中途断开",request_timeout:"请求超时",service_stopping:"服务停止",
  upstream_failed:"上游返回失败",upstream_stream_interrupted:"上游响应流中断",downstream_write_failed:"写回客户端失败"
};
const conversationIdleMs = 10 * 60 * 1000;
let dismissedConversations = {};
try { const saved = JSON.parse(localStorage.getItem("sleep-state-dismissed-conversations") || "{}"); if (saved && typeof saved === "object" && !Array.isArray(saved)) dismissedConversations = saved; } catch (_) {}
function saveDismissedConversations() {
  try { localStorage.setItem("sleep-state-dismissed-conversations", JSON.stringify(dismissedConversations)); } catch (_) {}
}
function trafficBadgeClass(item) {
  return item?.phase !== "finished" ? "live" : item.result === "failed" ? "bad" : ["completed","http_success"].includes(item.result) ? "good" : "";
}
function renderConversations(traffic) {
  const container = $("conversations");
  if (!container) return;
  const scrollPositions = new Map([...container.querySelectorAll("details")].map(el=>[el.dataset.conversation,el.querySelector(".conversation-requests")?.scrollTop || 0]));
  const opened = new Set([...container.querySelectorAll("details[open]")].map(el=>el.dataset.conversation));
  const focusedElement = document.activeElement;
  const focused = focusedElement?.closest("[data-conversation]")?.dataset.conversation;
  const focusedRemove = focusedElement?.classList.contains("conversation-remove");
  const allGroups = traffic.conversations || [];
  let dismissedChanged = false;
  for (const id of Object.keys(dismissedConversations)) {
    const group = allGroups.find(g=>g.id === id);
    if (!group || group.received > dismissedConversations[id]) { delete dismissedConversations[id]; dismissedChanged = true; }
  }
  if (dismissedChanged) saveDismissedConversations();
  const now = Date.now();
  const groups = allGroups.filter(group => !Object.hasOwn(dismissedConversations, group.id) && (group.active > 0 || now - Date.parse(group.last_at) < conversationIdleMs));
  $("conversation-summary").textContent = `${groups.length} 个近期对话 · ${traffic.active || 0} 个请求处理中 · 累计收到 ${traffic.total || 0} 次`;
  container.replaceChildren();
  if (!groups.length) container.append(textNode("p", "暂无近期对话，有新请求时会自动出现。", "hint conversation-empty"));
  for (const group of groups) {
    const wrapper = textNode("div", "", "conversation-tile");
    wrapper.dataset.conversation = group.id;
    const card = textNode("details", "", "conversation-card");
    card.dataset.conversation = group.id;
    card.open = opened.has(group.id);
    wrapper.classList.toggle("is-open", card.open);
    const summary = textNode("summary", "", "conversation-summary");
    const latest = group.recent?.[0];
    const title = group.conversation_id ? `对话 ${group.conversation_id.slice(0,8)}…${group.conversation_id.slice(-4)}` : "未识别对话";
    summary.append(textNode("span", title, "conversation-identity"));
    const heading = textNode("div", "", "conversation-heading");
    heading.append(textNode("strong", latest?.model || "接口请求", "conversation-model"));
    heading.append(textNode("span", group.active ? `${group.active} 个处理中` : latest ? requestStatus(latest) : "空闲", `traffic-badge ${group.active ? "live" : latest ? trafficBadgeClass(latest) : ""}`));
    summary.append(heading);
    const counts = textNode("div", "", "conversation-counts");
    for (const [label,value,style] of [["收到",group.received,""],["返回",group.completed,"good"],["失败",group.failed,"bad"],["取消",group.cancelled,""]]) {
      const stat = textNode("span", "", value ? style : "");
      stat.append(textNode("b", String(value || 0)),document.createTextNode(` ${label}`));
      counts.append(stat);
    }
    if (group.unverified) counts.append(textNode("span", `${group.unverified} 未确认`));
    summary.append(counts);
    if (latest) {
      const node = latest.dispatched_at ? latest.route_label || latest.route_id || "未知节点" : "尚未发往上游";
      const route = textNode("p", node, "conversation-node"); route.title = node; summary.append(route);
      summary.append(textNode("span", `${stateClock(latest.started_at || latest.at)} · ${latest.kind} · 查看明细`, "conversation-latest"));
    }
    card.append(summary);
    const renderDetails = () => {
      card.append(textNode("p", group.conversation_id ? `对话标识：${group.conversation_id}（客户端 ${group.identity_source}）` : "客户端未提供有效对话标识，暂按账号归在此处，不能据此区分真实对话。", "hint"));
      const list = textNode("div", "", "conversation-requests");
      for (const item of group.recent || []) {
        const row = textNode("article", "", "traffic-request");
        const top = textNode("div", "", "traffic-request-heading");
        top.append(textNode("strong", `${stateClock(item.started_at || item.at)} · ${item.kind}`));
        top.append(textNode("span", requestStatus(item), `traffic-badge ${trafficBadgeClass(item)}`));
        row.append(top);
        const status = item.status ? `HTTP ${item.status}` : item.upstream_status ? `上游 HTTP ${item.upstream_status}` : "尚无 HTTP 响应";
        row.append(textNode("p", `${status} · ${(Math.max(0,item.duration_ms || 0)/1000).toFixed(1)} 秒 · 已向客户端写出 ${Number(item.bytes || 0).toLocaleString()} 字节`, "hint"));
        row.append(textNode("p", item.dispatched_at ? `实际出站：${item.route_label || item.route_id} · ${item.state_injected ? "已带主票" : "未注入主票"}` : "尚未发往上游 · 本地等待或拦截", "traffic-node"));
        if (item.model) row.append(textNode("p", `请求 ${item.model} → 上游 ${item.response_model || (item.phase === "finished" ? "未返回可识别模型" : "结束后显示")}${item.response_model && item.model !== item.response_model ? " · 模型不一致" : ""}`, "hint"));
        if (item.upstream_at) row.append(textNode("p", `收到上游响应头：${stateClock(item.upstream_at)}${item.first_byte_at ? ` · 首次写出响应：${stateClock(item.first_byte_at)}` : ""}`, "hint"));
        if (item.termination_reason) row.append(textNode("p", trafficTerminationNames[item.termination_reason] || item.termination_reason, "traffic-termination"));
        if (item.completion_observed && !item.completion_forwarded) row.append(textNode("p", "已读到完成标记，但未完整写出给客户端。", "traffic-error"));
        if (item.error_code) row.append(textNode("p", `错误：${item.error_code}`, "traffic-error"));
        list.append(row);
      }
      card.append(list);
      list.scrollTop = scrollPositions.get(group.id) || 0;
    };
    if (card.open) renderDetails();
    card.addEventListener("toggle",()=>{
      wrapper.classList.toggle("is-open", card.open);
      if(card.open && !card.querySelector(".conversation-requests")) renderDetails();
    });
    const remove = textNode("button", "×", "conversation-remove quiet");
    remove.type = "button";
    remove.title = "移除卡片；新请求到达时重新显示";
    remove.setAttribute("aria-label", `移除卡片：${title}`);
    remove.addEventListener("click",()=>{
      dismissedConversations[group.id] = group.received;
      saveDismissedConversations();
      renderConversations(traffic);
      notice("卡片已移除。有新请求时会重新出现，不影响请求和票池。");
    });
    wrapper.append(card,remove);
    container.append(wrapper);
    if (focused === group.id) (focusedRemove ? remove : summary).focus({preventScroll:true});
  }
}

let switchTarget = null;
function openRouteSwitch(session,card) {
  switchTarget = {session_id:session.id,state_id:card.id,version:card.version};
  $("switch-ticket-label").textContent = `${session.model} · 主票 ${card.id.slice(0,8)}`;
  $("switch-current-route").textContent = `当前节点：${card.route_label || card.route_id}；采集来源：${card.source_route_label || card.source_route_id || card.route_label || card.route_id}`;
  const select = $("switch-route-select");
  select.replaceChildren();
  for (const node of state.pool_nodes || []) {
    if (node.id === card.route_id || ["disabled","failed"].includes(node.state)) continue;
    const option = textNode("option", node.label || node.id);
    option.value = node.id;
    select.append(option);
  }
  $("confirm-route-switch").disabled = select.options.length === 0;
  $("switch-route-result").textContent = select.options.length ? "" : "暂无其他可用节点，请先在节点池恢复或启用节点。";
  $("route-switch-dialog").showModal();
}
document.addEventListener("DOMContentLoaded",()=>{
  $("cancel-route-switch").addEventListener("click",()=>$("route-switch-dialog").close());
  $("route-switch-form").addEventListener("submit",async(event)=>{
    event.preventDefault();
    const button=$("confirm-route-switch");
    button.disabled=true;
    try {
      const result=await api("state/switch-route",{...switchTarget,route_id:$("switch-route-select").value});
      $("route-switch-dialog").close();notice(result.message);await refreshStatus();
    } catch(error) {$("switch-route-result").textContent=error.message;}
    finally {button.disabled=false;}
  });
});
