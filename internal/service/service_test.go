package service

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/gylive/ccodex-sleep-state/internal/settings"
	"github.com/gylive/ccodex-sleep-state/internal/turnstate"
)

func freeAddress(t *testing.T) string {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	address := l.Addr().String()
	l.Close()
	return address
}
func TestServiceLifecycleInIsolatedHomes(t *testing.T) {
	data, home := t.TempDir(), t.TempDir()
	original := []byte("# personal config\nmodel = 'previous'\n[features]\nexample = true\n")
	target := filepath.Join(home, "config.toml")
	if err := os.WriteFile(target, original, 0600); err != nil {
		t.Fatal(err)
	}
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		raw := make([]byte, 217)
		raw[0] = 0x80
		binary.BigEndian.PutUint64(raw[1:9], uint64(time.Now().Unix()))
		w.Header().Set(turnstate.Header, base64.URLEncoding.EncodeToString(raw))
		w.Header().Set("Content-Type", "text/event-stream")
		fmt.Fprint(w, "data: {\"type\":\"response.completed\"}\n\n")
	}))
	defer upstream.Close()
	c := settings.Default()
	c.Listen = freeAddress(t)
	c.CodexHome = home
	c.UpstreamMode = "manual"
	c.Upstream = upstream.URL + "/backend-api/codex"
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- Run(ctx, data, c, true, io.Discard) }()
	awaitStatus(t, data, done)
	if err := Run(ctx, data, c, true, io.Discard); err == nil {
		t.Fatal("second service was allowed")
	}
	req, _ := http.NewRequest("POST", "http://"+c.Listen+"/backend-api/codex/responses", strings.NewReader(`{"model":"gpt-6-astra","input":"private prompt","stream":true}`))
	req.Header.Set("Authorization", "Bearer synthetic-private-credential")
	client := &http.Client{Timeout: 5 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	io.Copy(io.Discard, resp.Body)
	resp.Body.Close()
	if resp.StatusCode != 503 {
		t.Fatalf("cold strict request should return promptly while background collects: %d", resp.StatusCode)
	}
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		snapshot, err := Status(context.Background(), data)
		if err == nil && strings.Contains(string(snapshot), `"usable":true`) {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	req, _ = http.NewRequest("POST", "http://"+c.Listen+"/backend-api/codex/responses", strings.NewReader(`{"model":"gpt-6-astra","input":"private prompt","stream":true}`))
	req.Header.Set("Authorization", "Bearer synthetic-private-credential")
	resp, err = client.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Fatalf("collected state not used: %d", resp.StatusCode)
	}
	status, err := Status(ctx, data)
	if err != nil || !bytes.Contains(status, []byte(`"usable":true`)) {
		t.Fatalf("status=%s err=%v", status, err)
	}
	unauth, err := client.Get("http://" + c.Listen + "/_sleep/status")
	if err != nil {
		t.Fatal(err)
	}
	unauth.Body.Close()
	if unauth.StatusCode != 401 {
		t.Fatal("management endpoint unauthenticated")
	}
	patched, _ := os.ReadFile(target)
	if !bytes.Contains(patched, []byte("ccodex-sleep-state")) {
		t.Fatal("Codex was not configured")
	}
	cancel()
	select {
	case err = <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(8 * time.Second):
		t.Fatal("shutdown timed out")
	}
	restored, _ := os.ReadFile(target)
	if !bytes.Equal(restored, original) {
		t.Fatal("configuration not restored byte-for-byte")
	}
	if _, err = os.Stat(filepath.Join(data, "runtime.json")); !os.IsNotExist(err) {
		t.Fatal("runtime file not cleaned up")
	}
	logs, _ := os.ReadFile(filepath.Join(data, "logs", "service.jsonl"))
	for _, secret := range []string{"synthetic-private-credential", "private prompt", "gAAAA"} {
		if bytes.Contains(logs, []byte(secret)) {
			t.Fatal("secret logged")
		}
	}
}
func TestNoConfigModeDoesNotCreateCodexHome(t *testing.T) {
	data := t.TempDir()
	c := settings.Default()
	c.Listen = freeAddress(t)
	c.CodexHome = filepath.Join(t.TempDir(), "must-stay-absent")
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- Run(ctx, data, c, false, io.Discard) }()
	awaitStatus(t, data, done)
	cancel()
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(c.CodexHome); !os.IsNotExist(err) {
		t.Fatal("--no-config touched Codex directory")
	}
}
func TestStatusRejectsNonLocalRuntime(t *testing.T) {
	dir := t.TempDir()
	data, _ := json.Marshal(Runtime{"example.invalid:80", "private-token"})
	os.WriteFile(filepath.Join(dir, "runtime.json"), data, 0600)
	if _, err := Status(context.Background(), dir); err == nil {
		t.Fatal("status sent control token to remote host")
	}
}
func awaitStatus(t *testing.T, dir string, done <-chan error) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		select {
		case err := <-done:
			t.Fatalf("service stopped: %v", err)
		default:
		}
		if _, err := Status(context.Background(), dir); err == nil {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatal("service startup timed out")
}
