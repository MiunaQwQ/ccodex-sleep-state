"use strict";
const trafficResultNames = {completed:"完整返回",http_success:"接口已返回",failed:"失败",cancelled:"已取消",unverified:"未确认完成"};
const trafficPhaseNames = {received:"已收到 · 本地处理中",waiting_upstream:"已发起请求 · 等待上游",receiving:"正在返回"};
function requestStatus(item) {
  return item.phase === "finished" ? trafficResultNames[item.result] || "未确认完成" : trafficPhaseNames[item.phase] || "已收到";
}
function requestDescription(item) {
  const when = stateClock(item.started_at || item.at);
  const node = item.dispatched_at ? item.route_label || item.route_id || "未知节点" : "尚未发往上游";
  return `${when} · ${item.kind}${item.model ? ` · ${item.model}` : ""} · ${requestStatus(item)} · ${node}`;
}
function renderConversations(traffic) {
  const container = $("conversations");
  if (!container) return;
  const scrollPositions = new Map([...container.querySelectorAll("details")].map(el=>[el.dataset.conversation,el.querySelector(".conversation-requests")?.scrollTop || 0]));
  const opened = new Set([...container.querySelectorAll("details[open]")].map(el=>el.dataset.conversation));
  const focused = document.activeElement?.closest("details[data-conversation]")?.dataset.conversation;
  const groups = traffic.conversations || [];
  $("conversation-summary").textContent = `本次启动收到 ${traffic.total || 0} 次请求 · ${traffic.active || 0} 个正在处理`;
  container.replaceChildren();
  if (!groups.length) container.append(textNode("p", "还没有请求到达。发起对话后，这里会立即出现记录，无需等回复结束。", "hint"));
  for (const group of groups) {
    const card = textNode("details", "", "conversation-card");
    card.dataset.conversation = group.id;
    card.open = opened.has(group.id);
    const summary = textNode("summary", "", "conversation-summary");
    const heading = textNode("div", "", "conversation-heading");
    const title = group.conversation_id ? `对话 · ${group.conversation_id.slice(0,8)}…${group.conversation_id.slice(-6)}` : "未提供对话标识的请求";
    heading.append(textNode("strong", title));
    heading.append(textNode("span", group.active ? `${group.active} 个处理中` : "当前无在途请求", group.active ? "traffic-badge live" : "traffic-badge"));
    summary.append(heading);
    summary.append(textNode("p", `收到 ${group.received} · 已返回 ${group.completed} · 失败 ${group.failed} · 取消 ${group.cancelled} · 未确认 ${group.unverified}`, "conversation-counts"));
    const latest = group.recent?.[0];
    if (latest) summary.append(textNode("p", requestDescription(latest), "conversation-latest"));
    card.append(summary);
    const renderDetails = () => {
    card.append(textNode("p", group.conversation_id ? `对话标识：${group.conversation_id}（客户端 ${group.identity_source}）` : "客户端未提供有效对话标识，暂按账号归在此处，不能据此区分真实对话。", "hint"));
    const list = textNode("div", "", "conversation-requests");
    for (const item of group.recent || []) {
      const row = textNode("article", "", "traffic-request");
      const top = textNode("div", "", "traffic-request-heading");
      top.append(textNode("strong", `${stateClock(item.started_at || item.at)} · ${item.kind}`));
      top.append(textNode("span", requestStatus(item), `traffic-badge ${item.phase !== "finished" ? "live" : item.result === "failed" ? "bad" : ["completed","http_success"].includes(item.result) ? "good" : ""}`));
      row.append(top);
      const status = item.status ? `HTTP ${item.status}` : item.upstream_status ? `上游 HTTP ${item.upstream_status}` : "尚无 HTTP 响应";
      row.append(textNode("p", `${status} · ${(Math.max(0,item.duration_ms || 0)/1000).toFixed(1)} 秒 · 已向客户端写出 ${Number(item.bytes || 0).toLocaleString()} 字节`, "hint"));
      row.append(textNode("p", item.dispatched_at ? `实际出站：${item.route_label || item.route_id} · ${item.state_injected ? "已带主票" : "未注入主票"}` : "尚未发往上游 · 本地等待或拦截", "traffic-node"));
      if (item.model) row.append(textNode("p", `请求 ${item.model} → 上游 ${item.response_model || (item.phase === "finished" ? "未返回可识别模型" : "结束后显示")}${item.response_model && item.model !== item.response_model ? " · 模型不一致" : ""}`, "hint"));
      if (item.upstream_at) row.append(textNode("p", `收到上游响应头：${stateClock(item.upstream_at)}${item.first_byte_at ? ` · 首次写出响应：${stateClock(item.first_byte_at)}` : ""}`, "hint"));
      if (item.error_code) row.append(textNode("p", `错误：${item.error_code}`, "traffic-error"));
      list.append(row);
    }
    card.append(list);
      list.scrollTop = scrollPositions.get(group.id) || 0;
    };
    if (card.open) renderDetails();
    card.addEventListener("toggle",()=>{if(card.open && !card.querySelector(".conversation-requests")) renderDetails();});
    container.append(card);
    if (focused === group.id) summary.focus({preventScroll:true});
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
