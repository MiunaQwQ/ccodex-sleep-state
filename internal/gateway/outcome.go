package gateway

import (
	"context"
	"net/http"
)

// Metadata shared with the local request history. Never stores response prose.
type RequestOutcome struct {
	Kind       string `json:"endpoint,omitempty"`
	Result     string `json:"result,omitempty"`
	ErrorCode  string `json:"error_code,omitempty"`
	RouteID    string `json:"route_id,omitempty"`
	RouteLabel string `json:"route_label,omitempty"`
	Model      string `json:"model,omitempty"`
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
