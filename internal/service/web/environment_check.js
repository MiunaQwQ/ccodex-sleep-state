"use strict";
// One explicit click, one passive local GET. No automatic probe or repair.
$("environment-check").addEventListener("click",()=>action(async()=>{
 const report=await api("environment-check");
 const container=$("environment-results");container.replaceChildren();
 container.append(textNode("h3",report.summary));
 container.append(textNode("p","只读检测 · 不改配置 · 不读登录凭据 · 不发送模型请求","hint"));
 const other=document.createElement("details");other.append(textNode("summary","查看其他检查项（通过与尚未验证）"));
 const labels={pass:"通过",attention:"需处理",unverified:"未验证"};
 const ordered=[...(report.items||[])].sort((a,b)=>(a.status==="attention"?0:1)-(b.status==="attention"?0:1));
 for(const item of ordered){
  const box=textNode("div","","card");
  box.append(textNode("h4",`${labels[item.status]||"未验证"} · ${item.title}`),textNode("p",item.message),textNode("p",`下一步：${item.next_step}`,"hint"));
  if(item.status==="attention" && ["overview","configuration","sources","pool"].includes(item.page)){
   const button=textNode("button","带我去处理","secondary");
   button.addEventListener("click",()=>document.querySelector(`[data-page="${item.page}"]`).click());box.append(button);
  }
  (item.status==="attention"?container:other).append(box);
 }
 container.append(other);
 container.append(textNode("p","本地检查不代表上游可用；实际发消息前请先处理权限或限流提示。","hint"));
 container.hidden=false;
 container.scrollIntoView({behavior:"smooth",block:"start"});
}));
