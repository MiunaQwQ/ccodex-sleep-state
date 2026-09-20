package gateway

import (
	"context"
	"io"
	"net/http/httptest"
	"strings"
	"testing"
)

type byteThenCancel struct{ data string }

func (r *byteThenCancel) Read(p []byte) (int, error) {
	if r.data == "" {
		return 0, context.Canceled
	}
	p[0] = r.data[0]
	r.data = r.data[1:]
	return 1, nil
}
func (r *byteThenCancel) Close() error { return nil }
func TestCompletionBoundaryWithSplitSSEAndTrailingCancellation(t *testing.T) {
	body := "data: {\"type\":\"response.output_text.delta\",\"delta\":\"示例\"}\r\n\r\ndata: {\"type\":\"response.completed\"}\r\n\r\n"
	stream := &bootstrapStream{ReadCloser: &byteThenCancel{data: body}}
	got, err := io.ReadAll(stream)
	if err != context.Canceled || string(got) != body {
		t.Fatal("wrong fixture")
	}
	if !stream.completionForwarded(int64(len(body))) || stream.completionForwarded(int64(len(body)-1)) {
		t.Fatal("terminal byte boundary is wrong")
	}
	if stream.complete() {
		t.Fatal("diagnostic fix relaxed state acquisition rules")
	}
	for _, payload := range []string{
		"data: {\"type\":\"response.failed\"}\n\ndata: {\"type\":\"response.completed\"}\n\n",
		"data: {\"type\":\"response.completed\"}\n\ndata: {\"type\":\"response.failed\"}\n\n",
	} {
		s := &bootstrapStream{ReadCloser: io.NopCloser(strings.NewReader(payload))}
		io.ReadAll(s)
		if s.protocolComplete() || s.completionForwarded(int64(len(payload))) {
			t.Fatal("upstream failure was hidden by completion")
		}
	}
}

// A socket may close when the proxy tries to write trailing SSE data after
// response.completed; that must not erase the completed reply.
type failTrailingWriter struct {
	*httptest.ResponseRecorder
	writes int
}

func (w *failTrailingWriter) Write(p []byte) (int, error) {
	w.writes++
	if w.writes > 1 {
		return 0, context.Canceled
	}
	return w.ResponseRecorder.Write(p)
}
func TestCompletionSurvivesTrailingWriteFailure(t *testing.T) {
	body := "data: {\"type\":\"response.completed\"}\n\n"
	stream := &bootstrapStream{ReadCloser: io.NopCloser(strings.NewReader(body))}
	io.ReadAll(stream)
	writer := &statusWriter{ResponseWriter: &failTrailingWriter{ResponseRecorder: httptest.NewRecorder()}}
	writer.Write([]byte(body))
	writer.Write([]byte(": trailing heartbeat\n\n"))
	if !writer.writeFailed || !stream.completionForwarded(writer.written) {
		t.Fatal("late socket close erased forwarded completion")
	}
}
