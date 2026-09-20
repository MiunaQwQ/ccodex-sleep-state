package gateway

import (
	"bytes"
	"io"
	"mime/multipart"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gylive/ccodex-sleep-state/internal/turnstate"
)

func TestProviderNamespacePassesThrough(t *testing.T) {
	var edit bytes.Buffer
	form := multipart.NewWriter(&edit)
	_ = form.WriteField("prompt", "private edit prompt")
	file, _ := form.CreateFormFile("image[]", "input.png")
	_, _ = file.Write([]byte("\x89PNG\r\n\x1a\n\x00image-fixture"))
	_ = form.Close()
	for _, tc := range []struct {
		method, path, body, contentType, encoding string
	}{
		{"POST", "images/generations", `{"model":"gpt-image-2","prompt":"private image prompt"}`, "application/json", ""},
		{"POST", "images/edits", edit.String(), form.FormDataContentType(), ""},
		{"POST", "alpha/search", string(encodeRequest(t, "zstd", []byte(`{"query":"private search"}`))), "application/json", "zstd"},
		{"POST", "future-feature", "opaque-payload", "application/octet-stream", "future-encoding"},
		{"PUT", "files/a%2Fb", "\x00opaque-file", "application/octet-stream", ""},
		{"PATCH", "jobs/job-id", `{"update":true}`, "application/json", ""},
		{"DELETE", "jobs/job-id", "", "", ""},
		{"GET", "jobs/job-id", "", "", ""},
		{"HEAD", "files/file-id", "", "", ""},
		{"OPTIONS", "future-feature", "", "", ""},
	} {
		t.Run(tc.method+"/"+tc.path, func(t *testing.T) {
			var calls atomic.Int32
			e, logs := testEngine(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				calls.Add(1)
				if r.Method != tc.method || r.URL.EscapedPath() != codexPrefix+tc.path || r.URL.RawQuery != "trace=a%2Fb" {
					t.Errorf("method or URL changed: %s %s", r.Method, r.URL)
				}
				if r.Header.Get("Authorization") != "Bearer feature-account" || r.Header.Get("ChatGPT-Account-Id") != "fixture-account" {
					t.Error("authentication changed")
				}
				if r.Header.Get(turnstate.Header) != "" {
					t.Error("chat state injected into auxiliary endpoint")
				}
				body, _ := io.ReadAll(r.Body)
				if !bytes.Equal(body, []byte(tc.body)) || r.Header.Get("Content-Type") != tc.contentType || r.Header.Get("Content-Encoding") != tc.encoding {
					t.Error("opaque request body or encoding changed")
				}
				w.Header().Set("Content-Type", "application/octet-stream")
				w.Header().Set(turnstate.Header, fakeToken(11, 3))
				_, _ = w.Write([]byte("\x00native-result"))
			}))
			r := httptest.NewRequest(tc.method, "http://127.0.0.1"+codexPrefix+tc.path+"?trace=a%2Fb", strings.NewReader(tc.body))
			r.Header.Set("Authorization", "Bearer feature-account")
			r.Header.Set("ChatGPT-Account-Id", "fixture-account")
			r.Header.Set("Content-Type", tc.contentType)
			r.Header.Set("Content-Encoding", tc.encoding)
			s, err := e.borrow(r.Header)
			if err != nil {
				t.Fatal(err)
			}
			token, _ := turnstate.Parse(fakeToken(10, 1))
			s.state.Offer(token, 0, time.Now())
			standby, _ := turnstate.Parse(fakeToken(10, 2))
			s.state.Offer(standby, 0, time.Now())
			before, _ := s.state.Acquire(time.Now())
			s.mu.Lock()
			s.lastAI = time.Now().Add(-time.Hour)
			lastAI := s.lastAI
			s.mu.Unlock()
			release(s)
			w := httptest.NewRecorder()
			e.ServeHTTP(w, r)
			if w.Code != 200 || calls.Load() != 1 {
				t.Fatalf("status=%d calls=%d body=%s", w.Code, calls.Load(), w.Body.String())
			}
			if tc.method != "HEAD" && w.Body.String() != "\x00native-result" {
				t.Error("response bytes changed")
			}
			after, usable := s.state.Acquire(time.Now())
			if !usable || after != before || s.state.Status(time.Now()).Standby != 1 || s.activated || s.lastAI != lastAI || s.lastProbeResult != nil {
				t.Fatal("auxiliary request altered chat state or collection activity")
			}
			for _, secret := range []string{"feature-account", "private", "opaque-payload", "native-result", "trace="} {
				if strings.Contains(logs.String(), secret) {
					t.Fatal("payload/credential leaked to log")
				}
			}
		})
	}
}

func TestAuxiliaryFailureDoesNotPoisonChat(t *testing.T) {
	for _, code := range []int{401, 403, 404, 429, 500, 503} {
		t.Run(http.StatusText(code), func(t *testing.T) {
			const body = `{"error":{"message":"feature-specific failure"}}`
			e, _ := testEngine(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Retry-After", "120")
				w.WriteHeader(code)
				_, _ = io.WriteString(w, body)
			}))
			r := request(`{"model":"gpt-image-2"}`, "feature-failure")
			r.URL.Path = codexPrefix + "images/generations"
			w := httptest.NewRecorder()
			e.ServeHTTP(w, r)
			if w.Code != code || w.Body.String() != body || w.Header().Get("Retry-After") != "120" {
				t.Fatal("upstream error changed")
			}
			s, _ := e.borrow(r.Header)
			defer release(s)
			if status, _ := s.rejection(); status != 0 {
				t.Fatalf("feature error blocked chat: %d", status)
			}
			if e.pool.Get(e.routes[0].ID).State == "failed" {
				t.Fatal("feature error disabled route")
			}
		})
	}
}

func TestProviderNamespaceBoundary(t *testing.T) {
	var calls atomic.Int32
	e, _ := testEngine(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { calls.Add(1) }))
	for _, path := range []string{"/admin/api/status", "/_sleep/status", "/backend-api/other", "/backend-api/codex-other/foo", codexPrefix + "../other", codexPrefix + "%2e%2e/other", codexPrefix + "files/%2e%2e/other", codexPrefix + "files/%5cother"} {
		r := httptest.NewRequest("POST", "http://127.0.0.1"+path, strings.NewReader("opaque"))
		r.Header.Set("Authorization", "Bearer boundary-test")
		w := httptest.NewRecorder()
		e.ServeHTTP(w, r)
		if w.Code != 404 {
			t.Errorf("outside namespace allowed: %s = %d", path, w.Code)
		}
	}
	for _, method := range []string{"CONNECT", "TRACE"} {
		r := request("", "boundary-test")
		r.Method = method
		r.URL.Path = codexPrefix + "images/generations"
		w := httptest.NewRecorder()
		e.ServeHTTP(w, r)
		if w.Code != 404 {
			t.Error("proxy tunnel method allowed")
		}
	}
	if calls.Load() != 0 {
		t.Fatal("out-of-namespace request reached upstream")
	}
}

func TestImageWaitUsesSameRouteWithoutMutatingChatTransport(t *testing.T) {
	e, _ := testEngine(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(40 * time.Millisecond)
		_, _ = io.WriteString(w, `{"data":[]}`)
	}))
	e.routes[0].Transport.ResponseHeaderTimeout = time.Millisecond
	r := request(`{"prompt":"fixture"}`, "image-timeout-account")
	r.URL.Path = codexPrefix + "images/generations"
	w := httptest.NewRecorder()
	e.ServeHTTP(w, r)
	if w.Code != 200 {
		t.Fatalf("image used chat timeout: %d %s", w.Code, w.Body.String())
	}
	if e.routes[0].Transport.ResponseHeaderTimeout != time.Millisecond {
		t.Fatal("shared transport was mutated")
	}
}
