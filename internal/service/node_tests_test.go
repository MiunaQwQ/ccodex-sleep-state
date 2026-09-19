package service

import (
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"
)

func TestLatencyDistinguishes401RestrictionAndTimeoutWithoutCredentials(t *testing.T) {
	for _, code := range []int{200, 401, 403, 502} {
		t.Run(fmt.Sprint(code), func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.Header.Get("Authorization") != "" || r.Method != "GET" || r.URL.Path != "/models" {
					t.Error("latency test sent auth or a model request")
				}
				w.WriteHeader(code)
			}))
			defer server.Close()
			result := testNode(context.Background(), map[string]any{"id": "direct"}, server.URL+"/models", time.Second)
			if !result.Reachable || result.Selectable != (code == 200 || code == 401) || result.Status != code {
				t.Fatalf("unexpected result: %+v", result)
			}
		})
	}
	slow := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { <-r.Context().Done() }))
	defer slow.Close()
	result := testNode(context.Background(), map[string]any{"id": "direct"}, slow.URL+"/models", 30*time.Millisecond)
	if result.Phase != "timeout" || result.Selectable {
		t.Fatal("timeout marked selectable")
	}
}

func TestBatchLatencyBoundedConcurrencyAndCancellation(t *testing.T) {
	var active, peak, requests atomic.Int32
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "" {
			t.Error("unexpected upstream credentials")
		}
		n := active.Add(1)
		requests.Add(1)
		defer active.Add(-1)
		for {
			p := peak.Load()
			if n <= p || peak.CompareAndSwap(p, n) {
				break
			}
		}
		select {
		case <-r.Context().Done():
		case <-time.After(100 * time.Millisecond):
			w.WriteHeader(401)
		}
	}))
	defer target.Close()
	proxy := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != "CONNECT" {
			t.Error("expected CONNECT")
			return
		}
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
	defer proxy.Close()
	catalog := []map[string]any{}
	for i := 0; i < 9; i++ {
		catalog = append(catalog, map[string]any{"id": fmt.Sprintf("node-%d", i), "connection": proxy.URL})
	}
	tester := &nodeTester{}
	if err := tester.start(context.Background(), catalog, target.URL+"/models", time.Second); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(3 * time.Second)
	for tester.snapshot().Running && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	done := tester.snapshot()
	if done.Running || done.Completed != 9 || len(done.Results) != 9 || peak.Load() > 4 || peak.Load() < 2 {
		t.Fatalf("batch incomplete/serial/unbounded: completed=%d peak=%d", done.Completed, peak.Load())
	}
	for _, result := range done.Results {
		if !result.Selectable {
			t.Fatalf("node not tested through proxy: %+v", result)
		}
	}
	if err := tester.start(context.Background(), catalog, target.URL+"/models", time.Second); err != nil {
		t.Fatal(err)
	}
	tester.stop()
	deadline = time.Now().Add(2 * time.Second)
	for tester.snapshot().Running && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if tester.snapshot().Running {
		t.Fatal("cancel did not stop worker job")
	}
}
