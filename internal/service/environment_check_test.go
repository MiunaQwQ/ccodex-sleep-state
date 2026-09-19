package service

import (
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/gylive/ccodex-sleep-state/internal/gateway"
	"github.com/gylive/ccodex-sleep-state/internal/proxyroute"
	"github.com/gylive/ccodex-sleep-state/internal/routepool"
	"github.com/gylive/ccodex-sleep-state/internal/settings"
)

func envItem(t *testing.T, r environmentReport, id string) environmentItem {
	t.Helper()
	for _, item := range r.Items {
		if item.ID == id {
			return item
		}
	}
	t.Fatal("missing check", id)
	return environmentItem{}
}
func TestEnvironmentCheckReadOnlyNoSecretsOrNetwork(t *testing.T) {
	var calls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { calls.Add(1); http.Error(w, "unexpected network", 500) }))
	defer server.Close()
	dir := t.TempDir()
	files := map[string]string{"config.toml": "api_key = 'CONFIG-CONTENT-SECRET'\n", "auth.json": "AUTH-CONTENT-SECRET", "config.json": "SERVICE-CONTENT-SECRET"}
	for name, value := range files {
		if err := os.WriteFile(filepath.Join(dir, name), []byte(value), 0600); err != nil {
			t.Fatal(err)
		}
	}
	before := map[string]os.FileInfo{}
	for name := range files {
		before[name], _ = os.Stat(filepath.Join(dir, name))
	}
	cfg := settings.Default()
	cfg.CodexHome = dir
	cfg.Upstream = server.URL + "/v1"
	cfg.UpstreamKind = "relay"
	cfg.Subscriptions = []settings.Source{{URL: server.URL + "/subscription?token=SUBSCRIPTION-SECRET"}}
	cfg.ProxyURLs = []string{"http://user:PROXY-SECRET@127.0.0.1:18080"}
	c := &control{config: cfg, dir: dir, path: filepath.Join(dir, "config.json"), managed: true, setupError: "RAW-ERROR-SECRET", routeError: "ROUTE-ERROR-SECRET"}
	r := c.environmentCheck()
	b, _ := json.Marshal(r)
	for _, secret := range []string{"SECRET", dir, server.URL, "auth.json", "api_key", "subscription?"} {
		if strings.Contains(string(b), secret) {
			t.Fatalf("report leaked %q", secret)
		}
	}
	if !r.ReadOnly || r.NetworkTested || r.CredentialFilesRead || calls.Load() != 0 {
		t.Fatal("check crossed passive boundary")
	}
	entries, _ := os.ReadDir(dir)
	if len(entries) != len(files) {
		t.Fatal("check created files")
	}
	for name, value := range files {
		after, err := os.Stat(filepath.Join(dir, name))
		if err != nil {
			t.Fatal(err)
		}
		data, _ := os.ReadFile(filepath.Join(dir, name))
		if string(data) != value || after.Mode() != before[name].Mode() || after.ModTime() != before[name].ModTime() {
			t.Fatal("file changed", name)
		}
	}
	if envItem(t, r, "remote_access").Status != "unverified" {
		t.Fatal("claimed remote validation")
	}
}
func TestEnvironmentCheckExistingConnectionIsNotConnectivity(t *testing.T) {
	cfg := settings.Default()
	cfg.CodexHome = t.TempDir()
	if err := os.WriteFile(filepath.Join(cfg.CodexHome, "config.toml"), []byte("invalid config is deliberately not read"), 0600); err != nil {
		t.Fatal(err)
	}
	pool, _ := routepool.Open("")
	e := gateway.New(cfg, []proxyroute.Route{{ID: "demo-route", DisplayName: "LABEL-SECRET", Protocol: "http"}}, slog.New(slog.NewTextHandler(io.Discard, nil)), pool)
	c := &control{config: cfg, dir: cfg.CodexHome, engine: e, pool: pool, managed: true}
	report := c.environmentCheck()
	if envItem(t, report, "routes").Status != "pass" || envItem(t, report, "remote_access").Status != "unverified" {
		t.Fatal(report)
	}
	b, _ := json.Marshal(report)
	if strings.Contains(string(b), "LABEL-SECRET") {
		t.Fatal("route data leaked")
	}
	cfg.EgressMode = "random"
	c.config = cfg
	if envItem(t, c.environmentCheck(), "routes").Status != "attention" {
		t.Fatal("one route cannot be independent random with injection")
	}
}
func TestEnvironmentPauseAssessment(t *testing.T) {
	tests := []struct {
		name             string
		sessions         []environmentSession
		status, contains string
	}{
		{"no session", nil, "unverified", "还没有"},
		{"auth", []environmentSession{{RejectedStatus: 403}}, "attention", "权限拒绝"},
		{"rate", []environmentSession{{RejectedStatus: 429, RetryAfterSeconds: 100}}, "attention", "限流"},
		{"cooldown", []environmentSession{{CooldownSeconds: 90}}, "pass", "90"},
		{"known session", []environmentSession{{Phase: "ready"}}, "pass", "没有记录"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			item := environmentUpstreamItem(tt.sessions)
			if item.Status != tt.status || !strings.Contains(item.Message, tt.contains) {
				t.Fatal(item)
			}
		})
	}
}
func TestEnvironmentMissingAndSymlinkConfig(t *testing.T) {
	dir := t.TempDir()
	cfg := settings.Default()
	cfg.CodexHome = dir
	c := &control{config: cfg, dir: dir}
	if envItem(t, c.environmentCheck(), "codex_files").Status != "attention" {
		t.Fatal("missing config should prompt action")
	}
	if err := os.Symlink(filepath.Join(dir, "missing-target"), filepath.Join(dir, "config.toml")); err != nil {
		t.Fatal(err)
	}
	if envItem(t, c.environmentCheck(), "codex_files").Status != "unverified" {
		t.Fatal("symlink should not be followed")
	}
}
