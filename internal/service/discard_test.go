package service

import (
	"crypto/sha256"
	"encoding/base64"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/gylive/ccodex-sleep-state/internal/settings"
	"github.com/gylive/ccodex-sleep-state/internal/turnstate"
)

func TestDiscardHTTPExactCardAndAuthorization(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		io.WriteString(w, "data: {\"type\":\"response.completed\"}\n\n")
	}))
	defer upstream.Close()
	c, h := panelControl(t, func(cfg *settings.Config) {
		cfg.Upstream = upstream.URL
		cfg.PoolEnabled = true
		cfg.InjectionDisabled = true
	})
	c.pause()
	backup, err := turnstate.OpenBackup(filepath.Join(c.dir, "state-backup.json"))
	if err != nil {
		t.Fatal(err)
	}
	c.backup = backup
	c.engine.SetStateBackup(backup)
	auth := "Bearer discard-http-synthetic"
	credential := sha256.Sum256([]byte(auth + "\x00"))
	now := time.Now().Truncate(time.Second)
	var ids []string
	for i, model := range []string{"gpt-6-astra", "gpt-5.6-sol"} {
		var cards []turnstate.PersistedSnapshot
		for j := 0; j < 3; j++ {
			raw := make([]byte, 217)
			raw[0] = 0x80
			binary.BigEndian.PutUint64(raw[1:9], uint64(now.Unix()))
			raw[30] = byte(10*i + j + 1)
			token := base64.URLEncoding.EncodeToString(raw)
			parsed, _ := turnstate.Parse(token)
			if i == 0 {
				ids = append(ids, parsed.Fingerprint)
			}
			cards = append(cards, turnstate.PersistedSnapshot{Token: token, RouteID: "direct", Issued: now, AcquiredAt: now.Add(time.Duration(j) * time.Second)})
		}
		key := sha256.Sum256([]byte(hex.EncodeToString(credential[:]) + "\x00" + model + "\x00" + c.config.Upstream))
		if err := backup.Save(hex.EncodeToString(key[:]), turnstate.Persisted{Active: &cards[0], Standby: cards[1:], SavedAt: now}); err != nil {
			t.Fatal(err)
		}
		w := panelRequest(h, "POST", "/backend-api/codex/responses", `{"model":"`+model+`","input":"synthetic"}`, map[string]string{"Authorization": auth})
		if w.Code != 200 {
			t.Fatal(w.Code)
		}
	}
	type panelSession struct {
		ID     string
		Model  string
		States []turnstate.Card
	}
	sessions := func() []panelSession {
		data, _ := json.Marshal(c.engine.Status())
		var s struct{ Sessions []panelSession }
		json.Unmarshal(data, &s)
		return s.Sessions
	}
	var astra, sol panelSession
	for _, s := range sessions() {
		if s.Model == "gpt-6-astra" {
			astra = s
		} else {
			sol = s
		}
	}
	payload := func(id string) string {
		b, _ := json.Marshal(map[string]string{"session_id": astra.ID, "state_id": id})
		return string(b)
	}
	for _, headers := range []map[string]string{nil, {"Authorization": "Bearer " + panelTestToken, "Origin": "https://external.example"}} {
		w := panelRequest(h, "POST", "/admin/api/state/discard", payload(ids[0]), headers)
		if w.Code != 401 && w.Code != 403 {
			t.Fatal("discard lacks authorization", w.Code)
		}
	}
	if w := panelPost(h, "state/discard", `{"session_id":"`+astra.ID+`","state_id":"`+ids[0][:8]+`"}`); w.Code != 400 {
		t.Fatal("partial identifier accepted")
	}
	beforeConfig := diskConfig(t, c)
	c.action.Lock() // Discard must not wait behind a manual network probe.
	w := panelPost(h, "state/discard", payload(ids[1]))
	c.action.Unlock()
	if w.Code != 200 {
		t.Fatal(w.Code, w.Body.String())
	}
	w = panelPost(h, "state/discard", payload(ids[0]))
	if w.Code != 200 || !strings.Contains(w.Body.String(), "已接替") {
		t.Fatal(w.Code, w.Body.String())
	}
	if w = panelPost(h, "state/discard", payload(ids[0])); w.Code != 409 {
		t.Fatal("stale deletion was accepted")
	}
	for _, s := range sessions() {
		if s.ID == astra.ID && (len(s.States) != 1 || s.States[0].ID != ids[2] || s.States[0].Role != "active") {
			t.Fatal("wrong successor")
		}
		if s.ID == sol.ID && len(s.States) != 3 {
			t.Fatal("discard crossed models")
		}
	}
	if diskConfig(t, c) != beforeConfig {
		t.Fatal("discard modified configuration")
	}
}
