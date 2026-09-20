package gateway

import (
	"bytes"
	"compress/gzip"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/klauspost/compress/zstd"
)

func TestResponseModelStreamMetadata(t *testing.T) {
	for _, tc := range []struct {
		name, body, model string
		json              bool
	}{
		{"created", "data: {\"type\":\"response.created\",\"response\":{\"model\":\"gpt-6-sol\"}}\n\ndata: {\"type\":\"response.completed\"}\n\n", "gpt-6-sol", false},
		{"final overrides created", "data: {\"type\":\"response.created\",\"response\":{\"model\":\"initial\"}}\n\ndata: {\"type\":\"response.completed\",\"response\":{\"model\":\"final\"}}\n\n", "final", false},
		{"named multiline event", "event: response.completed\r\ndata: {\r\ndata: \"response\":{\"model\":\"gpt-6-astra\"}}\r\n\r\n", "gpt-6-astra", false},
		{"failure still declares model", "data: {\"type\":\"response.failed\",\"response\":{\"model\":\"gpt-6-sol\",\"error\":{\"code\":\"slow_down\"}}}\n\n", "gpt-6-sol", false},
		{"JSON", `{"object":"response","status":"completed","model":"gpt-6-sol"}`, "gpt-6-sol", true},
		{"compact JSON", `{"object":"response.compaction","model":"gpt-6-sol","output":[]}`, "gpt-6-sol", true},
		{"do not infer from request or prose", "data: {\"type\":\"response.output_text.delta\",\"model\":\"fake\",\"response\":{\"model\":\"fake\"}}\n\n", "", false},
		{"missing", "data: {\"type\":\"response.completed\"}\n\n", "", false},
		{"SSE with JSON MIME", "data: {\"type\":\"response.completed\",\"response\":{\"model\":\"gpt-6-sol\"}}\n\n", "gpt-6-sol", true},
		{"invalid type", `{"object":"response","model":{"name":"fake"}}`, "", true},
		{"control characters", `{"object":"response","model":"fake\nmodel"}`, "", true},
		{"bounded field", `{"object":"response","model":"` + strings.Repeat("x", 257) + `"}`, "", true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			b := &bootstrapStream{ReadCloser: io.NopCloser(strings.NewReader(tc.body)), jsonMode: tc.json}
			var out bytes.Buffer
			// Force metadata and SSE boundaries across reads.
			_, err := io.CopyBuffer(&out, struct{ io.Reader }{b}, make([]byte, 7))
			if err != nil || out.String() != tc.body || b.responseModel != tc.model {
				t.Fatalf("model=%q want=%q, forwarding unchanged=%v, err=%v", b.responseModel, tc.model, out.String() == tc.body, err)
			}
		})
	}
}

func TestResponseModelForwardingClearsPreviousAndPreservesIsolation(t *testing.T) {
	for _, encoding := range []string{"identity", "gzip", "zstd"} {
		t.Run(encoding, func(t *testing.T) {
			calls := 0
			body := "data: {\"type\":\"response.completed\",\"response\":{\"model\":\"gpt-6-sol\"}}\n\n"
			e, _ := testEngine(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				calls++
				if r.Method == "GET" {
					w.Write([]byte(`{"data":[]}`))
					return
				}
				w.Header().Set("Content-Type", "text/event-stream")
				w.Header().Set("Content-Encoding", encoding)
				var data bytes.Buffer
				switch encoding {
				case "gzip":
					z := gzip.NewWriter(&data)
					z.Write([]byte(body))
					z.Close()
				case "zstd":
					z, _ := zstd.NewWriter(&data)
					z.Write([]byte(body))
					z.Close()
				default:
					data.WriteString(body)
				}
				w.Write(data.Bytes())
			}))
			e.disabled.Store(true)
			for i, want := range []string{"gpt-6-sol", ""} {
				if i == 1 {
					body = "data: {\"type\":\"response.completed\"}\n\n"
				}
				result := &RequestOutcome{}
				w := httptest.NewRecorder()
				e.ServeHTTP(w, WithOutcome(request(generation, "model-observation"), result))
				if w.Code != 200 || w.Body.String() != body || result.Model != "gpt-6-astra" || result.ResponseModel != want || result.Result != "completed" {
					t.Fatalf("unexpected forwarding result: %+v", result)
				}
				data, _ := json.Marshal(e.Status())
				var status struct {
					Sessions []struct {
						Model        string
						LastResponse *ResponseModelObservation `json:"last_response"`
					}
				}
				json.Unmarshal(data, &status)
				if len(status.Sessions) != 1 || status.Sessions[0].Model != "gpt-6-astra" || status.Sessions[0].LastResponse == nil || status.Sessions[0].LastResponse.Model != want || !status.Sessions[0].LastResponse.Received {
					t.Fatal("session retained stale model or lost observation")
				}
			}
			s, _ := e.borrow(request(generation, "model-observation").Header)
			previous := s.lastResponse
			release(s)
			metadata := request("", "model-observation")
			metadata.Method = "GET"
			metadata.URL.Path = "/backend-api/codex/models"
			metadata.Body = nil
			e.ServeHTTP(httptest.NewRecorder(), metadata)
			if s.lastResponse != previous || calls != 3 {
				t.Fatal("metadata replaced reply model or extra probes were sent")
			}
			other, _ := e.borrow(request(generation, "another-account").Header)
			defer release(other)
			if other.lastResponse != nil {
				t.Fatal("response model leaked across accounts")
			}
		})
	}
}
