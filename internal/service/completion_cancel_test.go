package service

import (
	"context"
	"github.com/gylive/ccodex-sleep-state/internal/settings"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

type cancelOnResponseWrite struct {
	*httptest.ResponseRecorder
	cancel context.CancelFunc
}

func (w *cancelOnResponseWrite) Write(p []byte) (int, error) {
	n, err := w.ResponseRecorder.Write(p)
	w.cancel()
	return n, err
}
func TestClientCloseAfterCompletionIsNotCancelled(t *testing.T) {
	for _, tc := range []struct{ name, payload, want, reason string }{
		{"completed", `{"type":"response.completed","response":{"model":"gpt-6-astra"}}`, "completed", "closed_after_complete"},
		{"unfinished", `{"type":"response.output_text.delta","delta":"synthetic"}`, "cancelled", "client_disconnected"},
		{"upstream_failed", `{"type":"response.failed","response":{"error":{"code":"server_is_overloaded"}}}`, "failed", "upstream_failed"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", "text/event-stream")
				io.WriteString(w, "data: "+tc.payload+"\n\n")
				w.(http.Flusher).Flush()
				<-r.Context().Done() // Keep the socket open after the protocol event.
			}))
			defer up.Close()
			c, _ := panelControl(t, func(cfg *settings.Config) { cfg.InjectionDisabled = true; cfg.Upstream = up.URL })
			r := trafficRequest(trafficThreadA, `{"model":"gpt-6-astra"}`)
			ctx, cancel := context.WithCancel(r.Context())
			defer cancel()
			w := &cancelOnResponseWrite{httptest.NewRecorder(), cancel}
			func() {
				defer func() {
					if p := recover(); p != nil && p != http.ErrAbortHandler {
						panic(p)
					}
				}()
				c.ServeHTTP(w, r.WithContext(ctx))
			}()
			e := c.history.snapshot()["recent"].([]requestEvent)[0]
			if e.Result != tc.want {
				t.Fatalf("result=%s want=%s; a client closing after the terminal event must preserve that terminal result", e.Result, tc.want)
			}
			if e.TerminationReason != tc.reason || e.CompletionForwarded != (tc.want == "completed") {
				t.Fatalf("wrong terminal evidence: %+v", e)
			}
			if !strings.Contains(w.Body.String(), tc.payload) {
				t.Fatal("forwarded content changed")
			}
		})
	}
}

type shortResponseWriter struct{ *httptest.ResponseRecorder }

func (w *shortResponseWriter) Write(p []byte) (int, error) {
	if len(p) > 0 {
		return w.ResponseRecorder.Write(p[:len(p)-1])
	}
	return 0, nil
}
func TestObservedCompletionMustActuallyBeForwarded(t *testing.T) {
	up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		io.WriteString(w, "data: {\"type\":\"response.completed\"}\n\n")
	}))
	defer up.Close()
	c, _ := panelControl(t, func(cfg *settings.Config) { cfg.InjectionDisabled = true; cfg.Upstream = up.URL })
	func() {
		defer func() {
			if p := recover(); p != nil && p != http.ErrAbortHandler {
				panic(p)
			}
		}()
		c.ServeHTTP(&shortResponseWriter{httptest.NewRecorder()}, trafficRequest(trafficThreadA, `{"model":"gpt-6-astra"}`))
	}()
	e := c.history.snapshot()["recent"].([]requestEvent)[0]
	if e.Result != "failed" || !e.CompletionObserved || e.CompletionForwarded {
		t.Fatalf("parsed but not written completion treated as successful: %+v", e)
	}
}
func TestCancellationReasonDistinguishesDeadlineAndServiceStop(t *testing.T) {
	for _, stop := range []bool{false, true} {
		c, _ := panelControl(t, func(cfg *settings.Config) { cfg.InjectionDisabled = true })
		var ctx context.Context
		var cancel context.CancelFunc
		want := "request_timeout"
		if stop {
			ctx, cancel = context.WithCancel(context.Background())
			cancel()
			c.ctx = ctx
			want = "service_stopping"
		} else {
			ctx, cancel = context.WithDeadline(context.Background(), time.Now().Add(-time.Second))
			defer cancel()
		}
		c.ServeHTTP(httptest.NewRecorder(), trafficRequest(trafficThreadA, `{"model":"gpt-6-astra"}`).WithContext(ctx))
		e := c.history.snapshot()["recent"].([]requestEvent)[0]
		if e.Result != "cancelled" || e.TerminationReason != want {
			t.Fatalf("wrong cancellation cause: %+v", e)
		}
	}
}
