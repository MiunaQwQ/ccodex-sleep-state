package gateway

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
)

const compactFixtureRequest = `{"model":"gpt-6-astra","input":[{"type":"message","role":"user","content":[{"type":"input_text","text":"keep original"}]}],"instructions":"keep instructions","parallel_tool_calls":true,"reasoning":{"effort":"low"},"service_tier":"auto","prompt_cache_key":"fixture-key","text":{"verbosity":"low"}}`

func completeCompact(w http.ResponseWriter, opaque string) {
	w.Header().Set("Content-Type", "application/json")
	item := map[string]any{"type": "compaction", "id": "cmp-fixture", "encrypted_content": opaque, "future_field": "preserve"}
	_, _ = fmt.Fprintf(w, `{"object":"response.compaction","output":[%s]}`, mustJSON(item))
}

func mustJSON(value any) string {
	data, _ := json.Marshal(value)
	return string(data)
}

func TestNativeCompactionPassesThroughUnchanged(t *testing.T) {
	var calls atomic.Int32
	const opaque = "opaque-compaction-never-log-this"
	e, logs := testEngine(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		if r.URL.Path != "/backend-api/codex/responses/compact" || r.URL.RawQuery != "trace=a%2Fb" {
			t.Errorf("native compact path/query changed: %s?%s", r.URL.Path, r.URL.RawQuery)
		}
		if r.Header.Get("Authorization") != "Bearer native-account-token" || r.Header.Get("ChatGPT-Account-Id") != "fixture-account" {
			t.Error("authentication or account changed")
		}
		body, _ := io.ReadAll(r.Body)
		if string(body) != compactFixtureRequest {
			t.Errorf("native compact body changed: %s", body)
		}
		completeCompact(w, opaque)
	}))
	r := request(compactFixtureRequest, "native-account-token")
	r.URL.Path = "/backend-api/codex/responses/compact"
	r.URL.RawQuery = "trace=a%2Fb"
	r.Header.Set("ChatGPT-Account-Id", "fixture-account")
	w := httptest.NewRecorder()
	e.ServeHTTP(w, r)
	var result struct {
		Object string `json:"object"`
		Output []struct {
			Type      string `json:"type"`
			Encrypted string `json:"encrypted_content"`
			Future    string `json:"future_field"`
		} `json:"output"`
	}
	if json.Unmarshal(w.Body.Bytes(), &result) != nil || result.Object != "response.compaction" || len(result.Output) != 1 || result.Output[0].Encrypted != opaque || result.Output[0].Future != "preserve" {
		t.Fatalf("invalid native result %s", w.Body.String())
	}
	if w.Code != http.StatusOK || calls.Load() != 1 || w.Header().Get("X-Sleep-State-Compaction") != "" {
		t.Fatalf("status=%d calls=%d headers=%v", w.Code, calls.Load(), w.Header())
	}
	if strings.Contains(logs.String(), opaque) {
		t.Fatal("compaction ciphertext leaked into log")
	}
}

func TestNativeCompactionPreservesUpstreamFailure(t *testing.T) {
	const failure = `{"object":"response.compaction","status":"failed","error":{"code":"server_is_overloaded","message":"Selected model is at capacity."}}`
	e, _ := testEngine(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, failure)
	}))
	r := request(compactFixtureRequest, "native-failure-token")
	r.URL.Path = "/backend-api/codex/responses/compact"
	w := httptest.NewRecorder()
	e.ServeHTTP(w, r)
	if w.Code != http.StatusOK || w.Body.String() != failure {
		t.Fatalf("upstream failure was rewritten: status=%d body=%s", w.Code, w.Body.String())
	}
}

func TestRelayKeepsNativeCompaction(t *testing.T) {
	var calls atomic.Int32
	e, _ := testEngine(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		body, _ := io.ReadAll(r.Body)
		if r.URL.Path != "/backend-api/codex/responses/compact" || string(body) != compactFixtureRequest {
			t.Error("relay request was rewritten")
		}
		completeCompact(w, "relay-opaque")
	}))
	e.config.UpstreamKind = "relay"
	r := request(compactFixtureRequest, "relay-api-key-token")
	r.URL.Path = "/backend-api/codex/responses/compact"
	w := httptest.NewRecorder()
	e.ServeHTTP(w, r)
	if w.Code != http.StatusOK || calls.Load() != 1 {
		t.Fatalf("status=%d calls=%d", w.Code, calls.Load())
	}
}

func TestCompressedNativeCompactionKeepsEndpoint(t *testing.T) {
	for _, encoding := range []string{"gzip", "zstd"} {
		t.Run(encoding, func(t *testing.T) {
			var calls atomic.Int32
			e, _ := testEngine(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				calls.Add(1)
				body, _ := io.ReadAll(r.Body)
				if r.URL.Path != "/backend-api/codex/responses/compact" || string(body) != compactFixtureRequest || r.Header.Get("Content-Encoding") != "" {
					t.Error("compressed native request was rewritten")
				}
				completeCompact(w, "opaque-compressed-fixture")
			}))
			wire := encodeRequest(t, encoding, []byte(compactFixtureRequest))
			r := request(string(wire), "compressed-native-token")
			r.URL.Path = "/backend-api/codex/responses/compact"
			r.Header.Set("Content-Encoding", encoding)
			w := httptest.NewRecorder()
			e.ServeHTTP(w, r)
			if w.Code != http.StatusOK || calls.Load() != 1 || !strings.Contains(w.Body.String(), "response.compaction") {
				t.Fatalf("status=%d calls=%d", w.Code, calls.Load())
			}
		})
	}
}
