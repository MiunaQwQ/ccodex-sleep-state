package gateway

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"errors"
	"io"
	"log"
	"net/http"
	"net/http/httputil"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/gylive/ccodex-sleep-state/internal/proxyroute"
	"github.com/gylive/ccodex-sleep-state/internal/settings"
	"github.com/gylive/ccodex-sleep-state/internal/turnstate"
)

const maxRequestBytes = 16 << 20

var errShape = errors.New("upstream state outside configured baseline")

func (e *Engine) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Header.Get("Upgrade") != "" {
		fail(w, http.StatusUpgradeRequired, "http_sse_required", "This provider uses HTTP/SSE, not WebSocket.")
		return
	}
	compact := r.Method == http.MethodPost && r.URL.Path == "/backend-api/codex/responses/compact"
	generation := r.Method == http.MethodPost && (r.URL.Path == "/backend-api/codex/responses" || r.URL.Path == "/backend-api/codex/responses/compact")
	if !codexEndpoint(r) {
		fail(w, http.StatusNotFound, "unsupported_endpoint", "Endpoint is not exposed by this service.")
		return
	}
	e.requests.Add(1)
	e.foreground.Add(1)
	defer func() {
		if e.foreground.Add(-1) == 0 {
			e.signal()
		}
	}()
	e.lastRequest.Store(time.Now().Unix())
	if e.settings().IsRelay() && officialCredential(r.Header.Get("Authorization")) {
		fail(w, http.StatusUnauthorized, "official_credentials_on_relay", "检测到官方登录凭据，已阻止发送给中转站。请为当前中转配置独立 API key。")
		return
	}
	if len(e.routes) == 0 {
		fail(w, 503, "no_routes", "没有可用出口。请在管理面板添加本地代理、订阅或启用直连。")
		return
	}
	outcome := outcomeFor(r)
	outcome.Kind = endpointKind(r.URL.Path)
	outcome.Result = "unverified"
	model := e.settings().SelectedModel()
	if generation || (r.Body != nil && r.Body != http.NoBody) {
		slots := e.bodySlots
		waiting := &e.bodyWaiting
		if !generation {
			slots = e.featureSlots
			waiting = &e.featureWaiting
		}
		waiting.Add(1)
		select {
		case slots <- struct{}{}:
			waiting.Add(-1)
			defer func() { <-slots }()
		case <-r.Context().Done():
			waiting.Add(-1)
			fail(w, 503, "local_request_capacity", "本机请求等待已取消；请求尚未转发")
			return
		}

		// Only Responses needs decoding for model/compaction inspection. Other
		// Codex features include multipart image edits and opaque uploads; keep
		// their bytes and Content-Encoding unchanged.
		var body []byte
		var status int
		var err error
		if generation {
			body, status, err = requestBodyWithLimits(r, e.settings().RequestBytes(), e.settings().WindowBytes())
		} else {
			body, status, err = opaqueRequestBody(r, e.settings().RequestBytes())
		}
		if err != nil {
			code := "invalid_request_encoding"
			if status == http.StatusUnsupportedMediaType {
				code = "unsupported_encoding"
			}
			if status == http.StatusRequestEntityTooLarge {
				code = "request_too_large"
			}
			fail(w, status, code, err.Error())
			return
		}
		if generation {
			var input struct {
				Model string `json:"model"`
			}
			if json.Unmarshal(body, &input) != nil {
				fail(w, 400, "invalid_json", "Expected a JSON request.")
				return
			}
			if !settings.SupportedModel(input.Model) {
				fail(w, 400, "unsupported_model", "支持 gpt-6-astra、gpt-5.6-sol 和 gpt-5.6-terra；请在 Codex 中选择受支持的模型。")
				return
			}
			model = input.Model
			if !compact {
				compact = remoteCompactionV2(body)
			}
		}
		r.Body = io.NopCloser(bytes.NewReader(body))
		r.ContentLength = int64(len(body))
		if generation {
			r.Header.Del("Content-Encoding")
		}
		r.Header.Del("Content-Length")
		r.Header.Del("Transfer-Encoding")
		r.TransferEncoding = nil
	}
	if compact {
		outcome.Kind = "compact"
	}
	outcome.Model = model
	s, err := e.borrow(r.Header, model)
	if err != nil {
		fail(w, http.StatusUnauthorized, "authentication_required", "Log in with Codex before using this service.")
		return
	}
	defer release(s)
	if generation {
		s.mu.Lock()
		s.lastAI = time.Now()
		s.aiBusy++
		s.mu.Unlock()
		defer func() {
			s.mu.Lock()
			s.aiBusy--
			s.lastAI = time.Now()
			s.mu.Unlock()
		}()
	}
	if rejectRequest(w, s) {
		return
	}
	// Compaction can reuse an existing ticket, but must never wait for a
	// generation probe or apply response-state shape rules to its result.
	inject := generation && !compact && !e.disabled.Load()
	observeResponse := inject
	epoch := e.injectionEpoch.Load()
	if inject {
		s.mu.Lock()
		s.activated = true
		s.mu.Unlock()
	}
	e.syncRoutes(s, time.Now())
	snapshot, usable := s.state.Acquire(time.Now())
	if inject && !usable {
		if e.running.Load() {
			e.signal()
		} else {
			e.refresh(r.Context(), s, true)
		}
		snapshot, usable = s.state.Acquire(time.Now())
		e.signal()
	}
	if rejectRequest(w, s) {
		return
	}
	if inject && !usable && e.settings().StateFallback == "passthrough" {
		// The user's generation has not been sent yet. Forward it once without
		// injection; never replay it after an upstream response or account limit.
		inject = false
		w.Header().Set("X-Sleep-State-Mode", "fallback-passthrough")
	}
	if inject && !usable {
		message, seconds := e.unavailableMessage(s)
		w.Header().Set("Retry-After", strconv.Itoa(seconds))
		fail(w, 503, "state_unavailable", message)
		return
	}
	bootstrapResponse := observeResponse && !inject
	var responseToken turnstate.Token
	responseValue := ""
	var responseParseErr error
	var responseStream *bootstrapStream
	responseReceived := false
	bootstrapResult := ""
	route := e.fallbackRouteFor(s, time.Now())
	if pinned := e.effectivePinnedRoute(); pinned != "" {
		found := false
		for i := range e.routes {
			if e.routes[i].ID == pinned {
				route, found = i, true
				break
			}
		}
		if !found {
			fail(w, 503, "pinned_route_unavailable", "指定出口不存在，请在路由页面重新选择。")
			return
		}
	}
	compactTicket := compact && usable && !e.disabled.Load() && !e.settings().IsRelay()
	compactBound := compactTicket || (compact && usable && e.pool.Get(e.routes[snapshot.Route].ID).State != "failed")
	if inject && usable || compactBound {
		route = snapshot.Route
	}
	// A state and its source route form one immutable request snapshot. User
	// egress preferences apply only when no state is being injected.
	stateBound := inject && usable || compactTicket
	if !stateBound && !compactBound && (e.settings().EgressMode == "random" || e.settings().EgressMode == "fixed") {
		selected, err := e.selectEgress(snapshot.Route, false)
		if err != nil {
			fail(w, 503, "egress_unavailable", err.Error())
			return
		}
		route = selected
	}
	// Manual removal must apply even in legacy cycling mode and to compact/
	// metadata requests, not just to new state probes.
	poolEntry := e.pool.Get(e.routes[route].ID)
	boundCard := (inject || compact) && usable && route == snapshot.Route
	if poolEntry.State == "disabled" || (poolEntry.State == "failed" && !(boundCard && poolEntry.Reason == "network_failed")) {
		fail(w, 503, "pool_node_disabled", "所选出口已停用，请在代理池手动放回或选择其他出口")
		return
	}
	if !stateBound && e.settings().PoolEnabled && generation && !compact && e.settings().EgressMode == "random" && e.settings().PinnedRoute == "" {
		// Reserve before dispatch so concurrent requests cannot consume one random
		// node twice. A fixed user exit is explicitly reusable.
		allowUsed := e.settings().EgressMode != "random"
		if err := e.pool.Claim(e.routes[route].ID, allowUsed); err != nil {
			fail(w, 503, "pool_node_unavailable", err.Error())
			return
		}
	}
	target, _ := url.Parse(e.settings().Upstream)
	outcome.RouteID = e.routes[route].ID
	outcome.RouteLabel = e.routes[route].DisplayName
	started := time.Now()
	stateCheck := "not_injected"
	if compactTicket {
		stateCheck = "compact_ticket_unchecked"
	}
	var headerTimeout time.Duration
	if kind := endpointKind(r.URL.Path); kind == "image_generation" || kind == "image_edit" {
		headerTimeout = 10 * time.Minute
	}
	transport := e.routes[route].ClientTransport(headerTimeout, func(event proxyroute.ConnectionEvent) { e.recordConnectionEvent(route, event) })
	proxy := &httputil.ReverseProxy{
		Rewrite: func(pr *httputil.ProxyRequest) {
			pr.Out.URL.Scheme = target.Scheme
			pr.Out.URL.Host = target.Host
			pr.Out.URL.Path = strings.TrimRight(target.Path, "/") + strings.TrimPrefix(pr.In.URL.Path, "/backend-api/codex")
			pr.Out.URL.RawPath = strings.TrimRight(target.EscapedPath(), "/") + strings.TrimPrefix(pr.In.URL.EscapedPath(), "/backend-api/codex")
			pr.Out.Host = target.Host
			if e.settings().IsRelay() {
				pr.Out.Header.Del(turnstate.Header)
			}
			pr.Out.Header.Del("Cookie")
			pr.Out.Header.Del("Proxy-Authorization")
			if e.settings().IsRelay() {
				for key := range pr.Out.Header {
					if strings.HasPrefix(strings.ToLower(key), "chatgpt-") {
						pr.Out.Header.Del(key)
					}
				}
				pr.Out.Header.Del("Originator")
				pr.Out.Header.Del("Version")
			}
			if generation {
				pr.Out.Header.Set("Accept-Encoding", "identity")
			}
			if inject || compactTicket {
				pr.Out.Header.Set(turnstate.Header, snapshot.Token.Value)
				outcome.StateInjected = true
			}
			if bootstrapResponse {
				// Managed fallback has no usable card. A client may echo an old
				// response header, including the card we just invalidated; never
				// let that bypass the pool and be forwarded again.
				pr.Out.Header.Del(turnstate.Header)
				pr.Out.Header.Set("Accept-Encoding", "identity")
			}
		},
		Transport: transport, FlushInterval: -1, ErrorLog: log.New(io.Discard, "", 0),
		ModifyResponse: func(resp *http.Response) error {
			responseReceived = true
			resp.Header.Del("Set-Cookie")
			if e.settings().IsRelay() {
				resp.Header.Del(turnstate.Header)
			}
			if resp.StatusCode == 503 {
				resp.Header.Set("X-Sleep-State-Error-Source", "upstream")
			}
			// A feature-specific permission/quota error (e.g. image generation)
			// does not prove that chat credentials are rejected. Pass it through
			// without poisoning the chat collector's account/state bookkeeping.
			if generation {
				e.reject(s, resp.StatusCode, retryDelay(resp.Header.Get("Retry-After")), route)
			}
			if generation && resp.StatusCode >= 200 && resp.StatusCode < 300 {
				responseStream, bootstrapResult = observeBootstrapResponse(resp)
				if observeResponse {
					responseValue = resp.Header.Get(turnstate.Header)
					responseToken, responseParseErr = turnstate.Parse(responseValue)
					stateCheck = "header_missing"
					if responseValue != "" {
						stateCheck = "pending_response"
					}
				}
			}
			if resp.StatusCode >= 400 {
				outcome.Result = "failed"
				outcome.ErrorCode = "http_error"
			}
			if !generation && resp.StatusCode >= 200 && resp.StatusCode < 300 {
				outcome.Result = "http_success"
			}
			if inject && resp.StatusCode >= 500 {
				e.recordNode(s, route, "upstream_rejected", resp.StatusCode, 0, 0, "response")
			}
			if resp.StatusCode >= 500 {
				e.advanceUnavailableRoute(s, route, time.Now())
			}
			if observeResponse {
				if resp.StatusCode >= 400 {
					stateCheck = "upstream_error"
				}
				checked := snapshot
				if bootstrapResponse {
					checked = turnstate.Snapshot{Token: responseToken, Route: route}
				}
				e.recordResponseCheck(s, checked, stateCheck, resp.StatusCode, bootstrapResult)
			}
			return nil
		},
		ErrorHandler: func(w http.ResponseWriter, r *http.Request, err error) {
			if errors.Is(err, errShape) {
				fail(w, 503, "state_shape_changed", "响应头 state 不符合规则，正文已拦截；已尝试切换备用 state，供下一次请求使用。本次可能已计费，不自动重放，请查看主备状态。")
				return
			}
			if r.Context().Err() == nil {
				outcome.Result = "failed"
				outcome.ErrorCode = "network_failed"
				stateCheck = "network_failed"
				checked := snapshot
				if !inject {
					checked = turnstate.Snapshot{Route: route}
				}
				if observeResponse {
					e.recordStateCheck(s, checked, stateCheck, 0)
				}
				e.recordNode(s, route, "network_failed", 0, 0, 0, "response")
				e.failRoute(route)
			}
			fail(w, 502, "upstream_unavailable", "Upstream connection failed. Request was not replayed.")
		},
	}
	tracked := &statusWriter{ResponseWriter: w, status: 200}
	defer func() {
		if responseStream != nil {
			outcome.ResponseModel = responseStream.responseModel
		}
		if r.Context().Err() != nil {
			outcome.Result = "cancelled"
		} else if responseStream != nil {
			outcome.Result = responseStream.outcome()
			if failure, ok := responseStream.failure.(*probeStreamError); ok {
				outcome.ErrorCode = failure.kind
				if failure.status == 401 || failure.status == 403 || failure.status == 429 {
					e.reject(s, failure.status, 0, route)
				}
			}
		}
		if generation {
			s.mu.Lock()
			s.lastResponse = &ResponseModelObservation{Model: outcome.ResponseModel, At: time.Now().UTC(), Endpoint: outcome.Kind, Result: outcome.Result, Received: responseReceived, StateInjected: outcome.StateInjected}
			s.mu.Unlock()
		}
		if observeResponse && responseStream != nil {
			if !responseStream.complete() {
				if outcome.Result == "failed" {
					stateCheck = "upstream_error"
				} else if responseValue != "" {
					stateCheck = "header_unverified"
				}
				bootstrapResult = "incomplete"
			} else if responseValue != "" && !e.disabled.Load() && e.injectionEpoch.Load() == epoch && r.Context().Err() == nil {
				if s.state.Observe(responseValue, snapshot, time.Now()) {
					stateCheck = "header_rejected"
					result := "shape_mismatch"
					if responseParseErr != nil {
						result = "invalid_state_envelope"
					} else if responseToken.Blocks == s.policy.Blocks {
						result = "state_time_rejected"
					}
					e.recordNode(s, route, result, tracked.status, responseToken.Blocks, time.Since(started), "response")
					invalidated := false
					if inject {
						invalidated = e.invalidate(s, snapshot, route, result, responseToken.Blocks)
					}
					if responseParseErr == nil && (invalidated || bootstrapResponse) {
						e.pauseMismatchedRoute(s, route, responseToken.Blocks, time.Now(), started)
					}
				} else {
					stateCheck = "header_accepted"
					e.recordNode(s, route, "accepted", tracked.status, responseToken.Blocks, time.Since(started), "response")
					if bootstrapResponse {
						bootstrapResult = "not_needed"
						e.syncRoutes(s, time.Now())
						saved := s.state.Bootstrap(responseToken, route, time.Now())
						if saved {
							bootstrapResult = "saved_active"
							if e.settings().Collection.Cadence == "round" {
								e.collection.EndRound(s.backupKey, time.Now(), time.Duration(e.settings().CooldownSeconds)*time.Second)
							}
							e.persistState(s)
							e.log.Info("response_state_bootstrapped", "model", s.model, "route", e.routes[route].ID)
						}
					}
				}
			}
			checked := snapshot
			if bootstrapResponse {
				checked = turnstate.Snapshot{Token: responseToken, Route: route}
			}
			e.recordResponseCheck(s, checked, stateCheck, tracked.status, bootstrapResult)
		}
		e.log.Info("request_finished", "status", tracked.status, "duration_ms", time.Since(started).Milliseconds(), "route", e.routes[route].ID, "state_version", snapshot.Version, "model", s.model, "response_model", outcome.ResponseModel, "compaction", compact, "endpoint", outcome.Kind, "method", r.Method, "state_injected", outcome.StateInjected, "state_check", stateCheck, "result", outcome.Result, "error_code", outcome.ErrorCode)
	}()
	proxy.ServeHTTP(tracked, r)
	if !stateBound && e.settings().PoolEnabled && generation && !compact && e.settings().EgressMode == "random" && e.settings().PinnedRoute == "" {
		st, reason := "used", "request_dispatched"
		if tracked.status >= 400 {
			st, reason = "failed", "request_failed"
		}
		_ = e.pool.FinishProbe(e.routes[route].ID, st, reason)
	}

}
func rejectRequest(w http.ResponseWriter, s *session) bool {
	status, seconds := s.rejection()
	if status == 0 {
		return false
	}
	if status == 429 {
		w.Header().Set("Retry-After", strconv.Itoa(seconds))
		fail(w, status, "upstream_rate_limited", "Upstream requested a pause. No request or probe was sent; wait before retrying.")
	} else {
		fail(w, status, "upstream_auth_rejected", "Upstream rejected these credentials. Resolve login or access in Codex before retrying.")
	}
	return true
}

func fail(w http.ResponseWriter, status int, code, message string) {
	w.Header().Set("X-Sleep-State-Error-Source", "local")
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "no-store")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(map[string]any{"error": map[string]string{"type": "sleep_state_error", "code": code, "message": message}})
}

type statusWriter struct {
	http.ResponseWriter
	status int
	wrote  bool
}

func (w *statusWriter) WriteHeader(status int) {
	if !w.wrote {
		w.status = status
		w.wrote = true
		w.ResponseWriter.WriteHeader(status)
	}
}
func (w *statusWriter) Write(p []byte) (int, error) {
	if !w.wrote {
		w.WriteHeader(200)
	}
	return w.ResponseWriter.Write(p)
}
func (w *statusWriter) Flush() {
	if !w.wrote {
		w.WriteHeader(200)
	}
	_ = http.NewResponseController(w.ResponseWriter).Flush()
}
func (w *statusWriter) Unwrap() http.ResponseWriter { return w.ResponseWriter }

// ProtectLocal rejects browser-origin requests and DNS-rebinding hostnames.
// Management endpoints additionally require a separate random local token.
func ProtectLocal(host string, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !strings.EqualFold(r.Host, host) || r.Header.Get("Origin") != "" || r.Header.Get("Sec-Fetch-Site") != "" {
			fail(w, 403, "local_clients_only", "Browser and non-loopback host requests are not accepted.")
			return
		}
		next.ServeHTTP(w, r)
	})
}

// An OAuth access token must never be mistaken for a relay API key. Claims here
// are only a fail-closed leak check, not authentication or signature validation.
func officialCredential(auth string) bool {
	token := strings.TrimSpace(strings.TrimPrefix(auth, "Bearer "))
	parts := strings.Split(token, ".")
	if len(parts) != 3 {
		return false
	}
	payload, err := base64.RawURLEncoding.DecodeString(parts[1])
	if err != nil {
		return false
	}
	var claims map[string]json.RawMessage
	if json.Unmarshal(payload, &claims) != nil {
		return false
	}
	if _, ok := claims["https://api.openai.com/auth"]; ok {
		return true
	}
	var issuer string
	_ = json.Unmarshal(claims["iss"], &issuer)
	u, err := url.Parse(issuer)
	return err == nil && (u.Hostname() == "auth.openai.com" || u.Hostname() == "auth0.openai.com")
}

// Remote compaction v2 uses /responses with a final protocol item rather than
// /responses/compact. Match the actual item, not text that mentions its name.
func remoteCompactionV2(body []byte) bool {
	var request struct {
		Input []json.RawMessage `json:"input"`
	}
	if json.Unmarshal(body, &request) != nil || len(request.Input) == 0 {
		return false
	}
	var last map[string]json.RawMessage
	if json.Unmarshal(request.Input[len(request.Input)-1], &last) != nil || len(last) != 1 {
		return false
	}
	var kind string
	return json.Unmarshal(last["type"], &kind) == nil && kind == "compaction_trigger"
}
