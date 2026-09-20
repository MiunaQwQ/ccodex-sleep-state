package gateway

import (
	"context"
	"net/http"
	"time"
)

// Metadata shared with the local request history. Never stores response prose.
type RequestOutcome struct {
	Kind                string    `json:"endpoint,omitempty"`
	Result              string    `json:"result,omitempty"`
	ErrorCode           string    `json:"error_code,omitempty"`
	RouteID             string    `json:"route_id,omitempty"`
	RouteLabel          string    `json:"route_label,omitempty"`
	Model               string    `json:"model,omitempty"`
	ResponseModel       string    `json:"response_model,omitempty"`
	StateInjected       bool      `json:"state_injected"`
	SessionID           string    `json:"ticket_session_id,omitempty"`
	DispatchedAt        time.Time `json:"dispatched_at,omitzero"`
	UpstreamAt          time.Time `json:"upstream_at,omitzero"`
	UpstreamStatus      int       `json:"upstream_status,omitempty"`
	CompletionObserved  bool      `json:"completion_observed"`
	CompletionForwarded bool      `json:"completion_forwarded"`
	TerminationReason   string    `json:"termination_reason,omitempty"`
}
type progressKey struct{}

// The gateway owns its mutable outcome. Only value copies are published to
// history, whose own mutex protects readers polling while a stream is active.
func WithProgress(r *http.Request, publish func(RequestOutcome)) *http.Request {
	return r.WithContext(context.WithValue(r.Context(), progressKey{}, publish))
}
func publishProgress(r *http.Request) {
	if publish, ok := r.Context().Value(progressKey{}).(func(RequestOutcome)); ok {
		publish(*outcomeFor(r))
	}
}

type progressTransport struct {
	base    http.RoundTripper
	request *http.Request
}

func (t progressTransport) RoundTrip(r *http.Request) (*http.Response, error) {
	outcomeFor(t.request).DispatchedAt = time.Now().UTC()
	publishProgress(t.request)
	return t.base.RoundTrip(r)
}

type outcomeKey struct{}

func WithOutcome(r *http.Request, result *RequestOutcome) *http.Request {
	return r.WithContext(context.WithValue(r.Context(), outcomeKey{}, result))
}
func outcomeFor(r *http.Request) *RequestOutcome {
	if result, ok := r.Context().Value(outcomeKey{}).(*RequestOutcome); ok {
		return result
	}
	return &RequestOutcome{}
}
