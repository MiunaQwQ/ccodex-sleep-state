package gateway

import (
	"compress/gzip"
	"encoding/json"
	"io"
	"net/http"
	"strings"

	"github.com/klauspost/compress/zstd"
)

type decodedBootstrapBody struct {
	io.Reader
	closeDecoder func()
	original     io.Closer
}

func (b *decodedBootstrapBody) Close() error {
	b.closeDecoder()
	return b.original.Close()
}

// Inspect actual SSE completion, not a MIME label. Some upstream responses
// retain compression despite Accept-Encoding: identity. Decode for forwarding
// and observation together so the client receives the same response content.
func observeBootstrapResponse(resp *http.Response) (*bootstrapStream, string) {
	original := resp.Body
	switch strings.ToLower(strings.TrimSpace(resp.Header.Get("Content-Encoding"))) {
	case "", "identity":
	case "gzip":
		decoder, err := gzip.NewReader(original)
		if err != nil {
			// The gzip header has been consumed: fail the body read rather than
			// forwarding a silently truncated compressed response.
			resp.Body = &failedBootstrapBody{ReadCloser: original, err: err}
			return nil, "invalid_encoding"
		}
		resp.Body = &decodedBootstrapBody{Reader: decoder, closeDecoder: func() { _ = decoder.Close() }, original: original}
	case "zstd":
		decoder, err := zstd.NewReader(original, zstd.WithDecoderConcurrency(1), zstd.WithDecoderLowmem(true), zstd.WithDecoderMaxMemory(64<<20), zstd.WithDecoderMaxWindow(64<<20))
		if err != nil {
			return nil, "invalid_encoding"
		}
		resp.Body = &decodedBootstrapBody{Reader: decoder, closeDecoder: decoder.Close, original: original}
	default:
		return nil, "unsupported_encoding"
	}
	if resp.Body != original {
		resp.Header.Del("Content-Encoding")
		resp.Header.Del("Content-Length")
		resp.ContentLength = -1
	}
	stream := &bootstrapStream{ReadCloser: resp.Body, jsonMode: strings.HasPrefix(strings.ToLower(resp.Header.Get("Content-Type")), "application/json")}
	resp.Body = stream
	return stream, "pending"
}

type failedBootstrapBody struct {
	io.ReadCloser
	err error
}

func (b *failedBootstrapBody) Read([]byte) (int, error) { return 0, b.err }

// Observe SSE completion while forwarding bytes unchanged. At most one event
// (1 MiB) is retained, never the full answer. Oversized/failed/truncated streams
// still reach the client but cannot bootstrap a state.
type bootstrapStream struct {
	io.ReadCloser
	responseModel             string
	jsonMode                  bool
	jsonData                  []byte
	jsonOversized             bool
	event                     []byte
	lineLength                int
	oversized, eof, completed bool
	failure                   error
}

func (b *bootstrapStream) inspect() {
	if !b.oversized {
		b.inspectResponseModel()
		done, err := probeStreamOutcome(b.event)
		b.completed = b.completed || done
		if err != nil {
			b.failure = err
		}
	}
	b.event = b.event[:0]
}

func (b *bootstrapStream) Read(p []byte) (int, error) {
	n, err := b.ReadCloser.Read(p)
	if b.jsonMode && !b.jsonOversized {
		if len(b.jsonData)+n <= 1<<20 {
			b.jsonData = append(b.jsonData, p[:n]...)
		} else {
			b.jsonOversized, b.jsonData = true, nil
		}
	}
	for _, ch := range p[:n] {
		if !b.oversized {
			if len(b.event) < 1<<20 {
				b.event = append(b.event, ch)
			} else {
				b.oversized = true
				b.event = nil
			}
		}
		if ch == '\n' {
			if b.lineLength == 0 {
				b.inspect()
			}
			b.lineLength = 0
		} else if ch != '\r' {
			b.lineLength++
		}
	}
	if err == io.EOF {
		b.inspect()
		b.eof = true
		if b.jsonMode && !b.jsonOversized {
			if model := declaredResponseModel(b.jsonData, ""); model != "" {
				b.responseModel = model
			}
			var response struct {
				Object string          `json:"object"`
				Status string          `json:"status"`
				Error  json.RawMessage `json:"error"`
				Output json.RawMessage `json:"output"`
			}
			if json.Unmarshal(b.jsonData, &response) == nil && (response.Object == "response" && response.Status == "completed" || response.Object == "response.compaction" && len(response.Output) > 0 && response.Status != "failed") && (len(response.Error) == 0 || string(response.Error) == "null") {
				b.completed = true
			}
			if len(response.Error) > 0 && string(response.Error) != "null" || response.Status == "failed" || response.Status == "incomplete" {
				payload, _ := json.Marshal(map[string]any{"type": "response.failed", "error": response.Error})
				_, b.failure = probeStreamOutcome(append(append([]byte("data: "), payload...), []byte("\n\n")...))
			}
			b.jsonData = nil
		}
	} else if err != nil {
		b.failure = err
	}
	return n, err
}

func (b *bootstrapStream) complete() bool {
	return b.eof && b.completed && !b.oversized && b.failure == nil
}

func (b *bootstrapStream) outcome() string {
	if b.failure != nil {
		return "failed"
	}
	if b.complete() {
		return "completed"
	}
	return "unverified"
}
