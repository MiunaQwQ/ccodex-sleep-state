package proxyroute

import (
	"context"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
)

func TestRecoveryKeepsInflightAndDisposesRetiredProxy(t *testing.T) {
	held := make(chan struct{})
	var once sync.Once
	release := func() { once.Do(func() { close(held) }) }
	defer release()
	var calls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		if r.URL.Path == "/held" {
			w.WriteHeader(200)
			w.(http.Flusher).Flush()
			<-held
		}
		io.WriteString(w, "OK")
	}))
	defer server.Close()
	var builds, closed atomic.Int32
	rt, err := newRuntime(func() (*http.Transport, func() error, error) {
		generation := builds.Add(1)
		tr := baseTransport()
		tr.Proxy = func(req *http.Request) (*url.URL, error) {
			if generation == 1 && req.URL.Path == "/broken" {
				return nil, syscall.ECONNRESET
			}
			return nil, nil
		}
		return tr, func() error { closed.Add(1); return nil }, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	route := Route{runtime: rt, Transport: rt.current.transport}
	defer route.Close()
	client := &http.Client{Transport: route.ClientTransport(0, nil)}
	response, err := client.Get(server.URL + "/held")
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 2; i++ {
		if _, err = client.Post(server.URL+"/broken", "text/plain", strings.NewReader("once")); err == nil {
			t.Fatal("expected connection error")
		}
	}
	if h := route.ConnectionHealth(); h.Generation != 2 || h.Recoveries != 1 || h.LastError != "connection_reset" {
		t.Fatal(h)
	}
	if closed.Load() != 0 {
		t.Fatal("recovery closed the in-flight proxy")
	}
	good, err := client.Get(server.URL + "/ok")
	if err != nil {
		t.Fatal(err)
	}
	io.Copy(io.Discard, good.Body)
	good.Body.Close()
	if calls.Load() != 2 {
		t.Fatal("failed requests were replayed", calls.Load())
	}
	release()
	data, err := io.ReadAll(response.Body)
	response.Body.Close()
	if err != nil || string(data) != "OK" {
		t.Fatal("in-flight response interrupted", err)
	}
	if closed.Load() != 1 {
		t.Fatal("retired proxy leaked")
	}
}

func TestBodyFailuresRecoverButCancellationAndHTTPRejectionDoNot(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/truncated" {
			w.Header().Set("Content-Length", "30")
			io.WriteString(w, "short")
			return
		}
		w.WriteHeader(429)
		io.WriteString(w, "limited")
	}))
	defer server.Close()
	rt, _ := newRuntime(func() (*http.Transport, func() error, error) { return baseTransport(), nil, nil })
	route := Route{runtime: rt, Transport: rt.current.transport}
	defer route.Close()
	var events []ConnectionEvent
	client := &http.Client{Transport: route.ClientTransport(0, func(e ConnectionEvent) { events = append(events, e) })}
	for i := 0; i < 2; i++ {
		resp, err := client.Get(server.URL + "/truncated")
		if err != nil {
			t.Fatal(err)
		}
		_, err = io.ReadAll(resp.Body)
		resp.Body.Close()
		if !errors.Is(err, io.ErrUnexpectedEOF) {
			t.Fatal(err)
		}
	}
	if len(events) != 2 || events[1].Stage != "read_body" || !events[1].Recovered {
		t.Fatal(events)
	}
	for i := 0; i < 3; i++ {
		resp, err := client.Get(server.URL)
		if err != nil {
			t.Fatal(err)
		}
		io.Copy(io.Discard, resp.Body)
		resp.Body.Close()
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	req, _ := http.NewRequestWithContext(ctx, "GET", server.URL, nil)
	client.Do(req)
	if h := route.ConnectionHealth(); h.Recoveries != 1 || h.ConsecutiveFailures != 0 {
		t.Fatal(h)
	}
}

func TestRecoveryCoalescesOldFailuresAndHandlesRebuildFailure(t *testing.T) {
	var builds atomic.Int32
	rt, _ := newRuntime(func() (*http.Transport, func() error, error) { builds.Add(1); return baseTransport(), nil, nil })
	old := rt.current
	old.users = 24
	var wg sync.WaitGroup
	for i := 0; i < 24; i++ {
		wg.Add(1)
		go func() { defer wg.Done(); rt.finish(old, io.ErrUnexpectedEOF, "read_body") }()
	}
	wg.Wait()
	if builds.Load() != 2 || rt.health.Recoveries != 1 {
		t.Fatal("concurrent failures rebuilt repeatedly")
	}
	rt.close()
	builds.Store(0)
	rt, _ = newRuntime(func() (*http.Transport, func() error, error) {
		if builds.Add(1) > 1 {
			return nil, nil, errors.New("private-node-secret")
		}
		return baseTransport(), nil, nil
	})
	old = rt.current
	old.users = 2
	rt.finish(old, syscall.ECONNREFUSED, "connect")
	event := rt.finish(old, syscall.ECONNREFUSED, "connect")
	if event.Recovered || event.RecoveryError != "rebuild_failed" || rt.current != old {
		t.Fatal(event)
	}
	rt.close()
}

func TestErrorCategoriesNeverExposeAddressesOrSecrets(t *testing.T) {
	cases := []struct {
		err  error
		want string
	}{
		{&net.DNSError{Name: "private.node.secret", Err: "no such host"}, "dns"},
		{context.DeadlineExceeded, "timeout"}, {syscall.ECONNRESET, "connection_reset"},
		{errors.New("tls: handshake failed for private.node.secret"), "tls"},
		{errors.New("anytls unknown secret=super-private"), "io_error"},
	}
	for _, tt := range cases {
		classified := &connectionError{category: ErrorCategory(tt.err)}
		if ErrorCategory(classified) != tt.want || strings.Contains(classified.Error(), "private") {
			t.Fatal(classified)
		}
	}
}
