package service

import "net/http"

func (c *control) switchStateRouteAction(w http.ResponseWriter, r *http.Request) {
	var v struct {
		SessionID string `json:"session_id"`
		StateID   string `json:"state_id"`
		Version   uint64 `json:"version"`
		RouteID   string `json:"route_id"`
	}
	if err := decode(w, r, &v); err != nil {
		reply(w, 400, map[string]string{"error": err.Error()})
		return
	}
	if v.SessionID == "" || len(v.StateID) != 16 || v.Version == 0 || v.RouteID == "" {
		reply(w, 400, map[string]string{"error": "请选择主票和目标节点"})
		return
	}
	if c.engine == nil {
		reply(w, 409, map[string]string{"error": "服务尚未准备好"})
		return
	}
	if err := c.engine.SwitchStateRoute(v.SessionID, v.StateID, v.Version, v.RouteID); err != nil {
		reply(w, 409, map[string]string{"error": err.Error()})
		return
	}
	reply(w, 200, map[string]string{"message": "主票已保留并切换节点。共用此模型票池的后续带票请求使用新节点，在途请求继续使用原节点；是否可用请看实际返回。"})
}
