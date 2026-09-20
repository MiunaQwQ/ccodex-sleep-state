package service

import (
	"crypto/sha256"
	"encoding/base64"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"github.com/gylive/ccodex-sleep-state/internal/gateway"
	"github.com/gylive/ccodex-sleep-state/internal/proxyroute"
	"github.com/gylive/ccodex-sleep-state/internal/settings"
	"github.com/gylive/ccodex-sleep-state/internal/turnstate"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

// Synthetic browser fixture: only loopback servers and test credentials.
func TestBrowserR20Fixture(t *testing.T) {
	if os.Getenv("SLEEPSTATE_R20_UI") != "1" {
		t.Skip("opt-in browser fixture")
	}
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == "GET" {
			w.WriteHeader(401)
			return
		}
		w.Header().Set("Content-Type", "text/event-stream")
		var input struct {
			Model string `json:"model"`
		}
		json.NewDecoder(r.Body).Decode(&input)
		model := map[string]string{"gpt-6-astra": "gpt-6-astra", "gpt-5.6-sol": "gpt-6-sol"}[input.Model]
		payload, _ := json.Marshal(map[string]any{"type": "response.completed", "response": map[string]string{"model": model}})
		fmt.Fprintf(w, "data: %s\n\n", payload)
	}))
	defer upstream.Close()
	c, _ := panelControl(t, func(cfg *settings.Config) {
		cfg.Listen = "127.0.0.1:17849"
		cfg.Upstream = upstream.URL + "/backend-api/codex"
		cfg.PoolEnabled = true
		cfg.StateRefreshMode = "standby"
		cfg.Collection.StandbyTarget = 2
	})
	c.stop()
	c.start([]proxyroute.Route{{ID: "direct", DisplayName: "演示节点 A", Transport: &http.Transport{Proxy: nil}}, {ID: "demo-b", DisplayName: "演示节点 B", Transport: &http.Transport{Proxy: nil}}, {ID: "demo-c", DisplayName: "演示节点 C", Transport: &http.Transport{Proxy: nil}}})
	c.pause()
	backup, err := turnstate.OpenBackup(filepath.Join(c.dir, "synthetic-state-backup.json"))
	if err != nil {
		t.Fatal(err)
	}
	c.backup = backup
	c.engine.SetStateBackup(backup)
	auth := "Bearer r20-synthetic-browser-account"
	credential := sha256.Sum256([]byte(auth + "\x00"))
	now := time.Now().Truncate(time.Second)
	for i, model := range settings.SupportedModels() {
		cards := []turnstate.PersistedSnapshot{}
		for j, age := range []time.Duration{30 * time.Minute, 20 * time.Minute, 10 * time.Minute} {
			issued := now.Add(-age)
			raw := make([]byte, 217)
			raw[0] = 0x80
			binary.BigEndian.PutUint64(raw[1:9], uint64(issued.Unix()))
			raw[30] = byte(i*10 + j + 1)
			cards = append(cards, turnstate.PersistedSnapshot{Token: base64.URLEncoding.EncodeToString(raw), RouteID: "direct", Issued: issued, AcquiredAt: issued.Add(time.Second)})
		}
		key := sha256.Sum256([]byte(hex.EncodeToString(credential[:]) + "\x00" + model + "\x00" + c.config.Upstream))
		if err := backup.Save(hex.EncodeToString(key[:]), turnstate.Persisted{Active: &cards[0], Standby: []turnstate.PersistedSnapshot{cards[2], cards[1]}, SavedAt: now}); err != nil {
			t.Fatal(err)
		}
		r := httptest.NewRequest("POST", "http://127.0.0.1:17849/backend-api/codex/responses", nil)
		r = httptest.NewRequest("POST", r.URL.String(), strings.NewReader(fmt.Sprintf(`{"model":%q,"input":"synthetic"}`, model)))
		r.Header.Set("Authorization", auth)
		r.Header.Set("session_id", fmt.Sprintf("01a0bcd%d-229d-7402-a276-a3be28651674", i))
		w := httptest.NewRecorder()
		c.ServeHTTP(w, r)
		if w.Code != 200 {
			t.Fatal(w.Code)
		}
	}
	ticket := &browserTicket{value: "r20-synthetic-launch", expires: time.Now().Add(15 * time.Minute)}
	handler := controlHandler(c.config.Listen, panelTestToken, c, ticket)
	listener, err := net.Listen("tcp", c.config.Listen)
	if err != nil {
		t.Fatal(err)
	}
	// Include a concurrent request using only synthetic metadata.
	extra := httptest.NewRequest("POST", "http://127.0.0.1:17849/backend-api/codex/responses", nil)
	extra.Header.Set("Authorization", auth)
	extra.Header.Set("session_id", "01a0bcd0-229d-7402-a276-a3be28651674")
	requestID := c.history.begin(extra, time.Now())
	c.history.progress(requestID, gateway.RequestOutcome{Kind: "responses", Model: "gpt-6-astra", RouteID: "direct", RouteLabel: "演示节点 A", DispatchedAt: time.Now(), UpstreamAt: time.Now(), UpstreamStatus: 200, StateInjected: true})
	c.history.written(requestID, 2048, time.Now())
	done := make(chan struct{})
	var once sync.Once
	server := &http.Server{Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/__finish" {
			once.Do(func() { close(done) })
			w.WriteHeader(204)
			return
		}
		handler.ServeHTTP(w, r)
	})}
	go server.Serve(listener)
	defer server.Close()
	t.Log("R20 UI fixture http://127.0.0.1:17849/admin/#launch=r20-synthetic-launch")
	select {
	case <-done:
	case <-time.After(15 * time.Minute):
		t.Fatal("UI fixture expired")
	}
}
