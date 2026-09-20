package service

import (
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/gylive/ccodex-sleep-state/internal/settings"
)

// Opt-in browser fixture. It uses only loopback synthetic proxies/upstream and
// a temporary config; it never reads the real Codex home or real credentials.
func TestBrowserNodePoolFixture(t *testing.T) {
	if os.Getenv("SLEEPSTATE_UI_SMOKE") != "1" {
		t.Skip("opt-in interactive browser fixture")
	}
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(401) }))
	defer target.Close()
	makeProxy := func(delay time.Duration) *httptest.Server {
		return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.Method != "CONNECT" {
				w.WriteHeader(405)
				return
			}
			time.Sleep(delay)
			conn, err := net.DialTimeout("tcp", r.Host, time.Second)
			if err != nil {
				w.WriteHeader(502)
				return
			}
			client, buffer, err := w.(http.Hijacker).Hijack()
			if err != nil {
				conn.Close()
				return
			}
			fmt.Fprint(buffer, "HTTP/1.1 200 Connection Established\r\n\r\n")
			buffer.Flush()
			go func() { io.Copy(conn, buffer); conn.Close() }()
			io.Copy(client, conn)
			client.Close()
			conn.Close()
		}))
	}
	fast, slow := makeProxy(15*time.Millisecond), makeProxy(240*time.Millisecond)
	defer fast.Close()
	defer slow.Close()
	_, handler := panelControl(t, func(c *settings.Config) {
		c.Listen = "127.0.0.1:17849"
		c.InjectionDisabled = true
		c.Direct = false
		c.Upstream = target.URL + "/backend-api/codex"
		c.ProxyURLs = []string{fast.URL + "#本地测试-快速", slow.URL + "#本地测试-较慢", "socks5://127.0.0.1:1#本地测试-不可用"}
	})
	listener, err := net.Listen("tcp", "127.0.0.1:17849")
	if err != nil {
		t.Fatal(err)
	}
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
	t.Log("Synthetic browser fixture ready: http://127.0.0.1:17849/admin/")
	select {
	case <-done:
	case <-time.After(12 * time.Minute):
		t.Fatal("browser fixture expired")
	}
}
