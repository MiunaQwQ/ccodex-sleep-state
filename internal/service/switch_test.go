package service

import (
	"strings"
	"testing"
)

func TestSwitchRouteManagementBoundaryAndFrontendAsset(t *testing.T) {
	c, h := panelControl(t, nil)
	for _, headers := range []map[string]string{nil, {"Authorization": "Bearer " + panelTestToken, "Origin": "https://external.example"}} {
		w := panelRequest(h, "POST", "/admin/api/state/switch-route", `{}`, headers)
		if w.Code != 401 && w.Code != 403 {
			t.Fatal("switch was not protected", w.Code)
		}
	}
	if w := panelPost(h, "state/switch-route", `{"session_id":"stale","state_id":"1234","route_id":"direct","version":1}`); w.Code != 400 {
		t.Fatal("partial ticket accepted", w.Code)
	}
	c.action.Lock()
	w := panelPost(h, "state/switch-route", `{"session_id":"stale","state_id":"1234567890abcdef","route_id":"direct","version":1}`)
	c.action.Unlock()
	if w.Code != 409 || !strings.Contains(w.Body.String(), "票池已不存在") {
		t.Fatal("switch waits behind network action or accepts stale session", w.Code)
	}
	w = panelRequest(h, "GET", "/admin/traffic.js", "", nil)
	if w.Code != 200 || !strings.Contains(w.Header().Get("Content-Type"), "javascript") || !strings.Contains(w.Body.String(), "renderConversations") {
		t.Fatal("traffic frontend unavailable")
	}
}
