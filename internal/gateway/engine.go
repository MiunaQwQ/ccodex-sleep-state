// Package gateway joins immutable request snapshots with a continuous serial collector.
// Authentication is borrowed from incoming Codex requests and kept only in RAM.
package gateway

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gylive/ccodex-sleep-state/internal/proxyroute"
	"github.com/gylive/ccodex-sleep-state/internal/routepool"
	"github.com/gylive/ccodex-sleep-state/internal/settings"
	"github.com/gylive/ccodex-sleep-state/internal/turnstate"
)

const idleLifetime = 30 * time.Minute

type credentialLimit struct {
	mu             sync.Mutex
	blocked        bool
	rejectedStatus int
	retryUntil     time.Time
}

type session struct {
	persistMu           sync.Mutex
	lastStateCheck      ResponseStateCheck
	lastProbeResult     *NodeObservation
	id                  string
	model               string
	policy              accountPolicy
	limit               *credentialLimit
	diagnostic          string
	observedBlocks      int
	lastProbe           time.Time
	mu                  sync.Mutex
	headers             http.Header
	state               *turnstate.Store
	lastUsed, nextProbe time.Time
	busy                int
	aiBusy              int
	lastAI              time.Time
	activated           bool
	cursor              int
	probing             chan struct{}
	queued              bool
	upstreamPause       time.Time
	nodes               map[string]NodeObservation
	backupKey           string
	nodePauses          map[int]time.Time
	retainedRoute       int
	retainedLast        bool
	fallbackRoute       int
}

type Engine struct {
	bodyWaiting    atomic.Int64
	featureWaiting atomic.Int64
	foreground     atomic.Int64
	collection     *CollectionBudget
	onDemand       atomic.Bool
	pool           *routepool.Store
	backup         *turnstate.BackupStore
	bodySlots      chan struct{}
	featureSlots   chan struct{}
	liveConfig     atomic.Pointer[settings.Config]
	requests       atomic.Uint64
	lastRequest    atomic.Int64
	injectionEpoch atomic.Uint64
	running        atomic.Bool
	disabled       atomic.Bool
	config         settings.Config
	routes         []proxyroute.Route
	log            *slog.Logger
	mu             sync.Mutex
	sessions       map[string]*session
	limits         map[string]*credentialLimit
	probeSlot      chan struct{}
	wake           chan struct{}
}

func New(c settings.Config, routes []proxyroute.Route, logger *slog.Logger, pools ...*routepool.Store) *Engine {
	e := &Engine{config: c, routes: routes, log: logger, sessions: make(map[string]*session), limits: make(map[string]*credentialLimit), probeSlot: make(chan struct{}, 1), wake: make(chan struct{}, 1)}
	e.pool, _ = routepool.Open("")
	e.collection, _ = OpenCollectionBudget("")
	if len(pools) > 0 && pools[0] != nil {
		e.pool = pools[0]
	}
	e.bodySlots = make(chan struct{}, 4)
	e.featureSlots = make(chan struct{}, 2)
	e.onDemand.Store(c.StateRefreshMode == "on_demand")
	e.disabled.Store(c.InjectionDisabled || c.IsRelay())
	return e
}

// SetStateBackup attaches the private state cache after the route pool is
// opened. It is separate from New so existing embedders and tests keep the
// original constructor contract.
func (e *Engine) SetStateBackup(store *turnstate.BackupStore) { e.backup = store }
func (e *Engine) borrow(h http.Header, models ...string) (*session, error) {
	model := e.settings().SelectedModel()
	if len(models) > 0 {
		model = models[0]
	}
	if !settings.SupportedModel(model) {
		return nil, errors.New("unsupported model")
	}
	auth := h.Get("Authorization")
	if !strings.HasPrefix(auth, "Bearer ") || len(strings.TrimSpace(strings.TrimPrefix(auth, "Bearer "))) < 8 || len(auth) > 16384 {
		return nil, errors.New("bearer authentication required")
	}
	sum := sha256.Sum256([]byte(auth + "\x00" + h.Get("ChatGPT-Account-Id")))
	credentialKey := hex.EncodeToString(sum[:])
	key := credentialKey + "\x00" + model
	e.mu.Lock()
	defer e.mu.Unlock()
	now := time.Now()
	for k, s := range e.sessions {
		s.mu.Lock()
		expired := s.busy == 0 && s.probing == nil && now.Sub(s.lastUsed) > idleLifetime && (!s.activated || s.state.Status(now).Usable)
		s.mu.Unlock()
		if expired {
			delete(e.sessions, k)
		}
	}
	// Discard idle, unrestricted credential bookkeeping; a rejected credential
	// stays remembered so changing models cannot reset an upstream rejection.
	for credential, limit := range e.limits {
		used := false
		for sessionKey := range e.sessions {
			if strings.HasPrefix(sessionKey, credential+"\x00") {
				used = true
				break
			}
		}
		limit.mu.Lock()
		removable := !used && !limit.blocked && !now.Before(limit.retryUntil)
		limit.mu.Unlock()
		if removable {
			delete(e.limits, credential)
		}
	}
	s := e.sessions[key]
	if s == nil {
		if len(e.sessions) >= 24 {
			return nil, errors.New("too many active credentials")
		}
		safe := make(http.Header)
		for _, k := range []string{"Authorization", "ChatGPT-Account-Id", "User-Agent", "Version", "Originator", "OpenAI-Beta"} {
			if v := h.Get(k); v != "" {
				safe.Set(k, v)
			}
		}
		limit := e.limits[credentialKey]
		if limit == nil {
			if len(e.limits) >= 8 {
				return nil, errors.New("too many active credentials")
			}
			limit = &credentialLimit{}
			e.limits[credentialKey] = limit
		}
		policy := policyFor(e.config, h)
		s = &session{id: rand.Text(), model: model, policy: policy, limit: limit, headers: safe, backupKey: stateBackupKey(key, e.settings().Upstream), state: turnstate.New(turnstate.Policy{Blocks: policy.Blocks, TTL: time.Duration(e.settings().TTLSeconds) * time.Second, Refresh: time.Duration(e.settings().RefreshSeconds) * time.Second})}
		s.state.HoldActive(e.onDemand.Load())
		s.lastAI = now
		if e.settings().PoolEnabled {
			s.state.SetStandbyLimit(e.settings().Collection.StandbyTarget)
		}
		e.restoreState(s)
		e.sessions[key] = s
	}
	s.mu.Lock()
	s.lastUsed = now
	s.busy++
	s.mu.Unlock()
	return s, nil
}
func release(s *session) { s.mu.Lock(); s.busy--; s.mu.Unlock() }

func stateBackupKey(sessionKey, upstream string) string {
	sum := sha256.Sum256([]byte(sessionKey + "\x00" + upstream))
	return hex.EncodeToString(sum[:])
}

func (e *Engine) restoreState(s *session) {
	if e.backup == nil || s.backupKey == "" {
		return
	}
	persisted, ok := e.backup.Load(s.backupKey)
	if !ok {
		return
	}
	byID := make(map[string]int, len(e.routes))
	for i, route := range e.routes {
		byID[route.ID] = i
	}
	count := s.state.Restore(persisted, time.Now(), func(id string) (int, bool) {
		index, ok := byID[id]
		return index, ok
	})
	e.syncRoutes(s, time.Now())
	if count > 0 {
		s.mu.Lock()
		s.diagnostic = "restored_backup"
		s.lastProbe = persisted.SavedAt
		s.mu.Unlock()
	}
}

func (e *Engine) persistState(s *session) {
	s.persistMu.Lock()
	defer s.persistMu.Unlock()
	if e.backup == nil || s.backupKey == "" {
		return
	}
	persisted := s.state.Export(time.Now(), func(route int) string {
		if route < 0 || route >= len(e.routes) {
			return ""
		}
		return e.routes[route].ID
	})
	_ = e.backup.Save(s.backupKey, persisted)
}

// Run does no work until a real Codex request supplies credentials. Idle
// credentials expire; no auth file is imported and no OAuth refresh is owned.
func (e *Engine) Run(ctx context.Context) {
	e.running.Store(true)
	defer e.running.Store(false)
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	e.signal()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		case <-e.wake:
		}
		if e.disabled.Load() {
			continue
		}
		work := e.backgroundWork(time.Now())
		for _, s := range work {
			s.mu.Lock()
			lastProbe := s.lastProbe
			s.mu.Unlock()
			if ctx.Err() == nil && e.needsCollection(s, time.Now()) && e.hasProbeCandidate() {
				e.refresh(ctx, s, false)
			}
			releaseWork(s)
			// Wake immediately only when another probe is due. Failed attempts are
			// paced by the shared persistent collection budget.
			s.mu.Lock()
			// Foreground generations and background collection are independent
			// requests. A completed probe wakes the collector even while Codex is
			// streaming a reply; the shared probe slot, account guard and budget
			// still bound the work.
			// Only completed work can immediately wake another batch. If all
			// remaining nodes are paused, wait for the ticker instead of spinning.
			due := s.probing == nil && s.lastProbe.After(lastProbe) && e.probeSchedule(s, time.Now()).WaitSeconds == 0
			s.mu.Unlock()
			status, _ := s.rejection()
			if due && status == 0 && e.needsCollection(s, time.Now()) && e.hasProbeCandidate() {
				e.signal()
			}
		}
	}
}

func releaseWork(s *session) { s.mu.Lock(); s.busy--; s.queued = false; s.mu.Unlock() }

func (e *Engine) needsCollection(s *session, now time.Time) bool {
	at, _ := e.collectionAt(s, now)
	return !at.IsZero() && !now.Before(at)
}

func (e *Engine) statePoolReady(s *session, now time.Time) bool {
	status := s.state.Status(now)
	if !status.Usable {
		return false
	}
	return status.Standby >= e.standbyTarget()
}

// One-use mode remains an explicit advanced option from upstream 0.4.
// An exhausted or disabled pool must wait for a configuration change instead
// of repeatedly waking a collector that has no eligible route.
func (e *Engine) hasProbeCandidate() bool {
	if e.pool.Err() != nil {
		return false
	}
	pinned := e.effectivePinnedRoute()
	for _, route := range e.routes {
		if pinned != "" && pinned != route.ID {
			continue
		}
		state := e.pool.Get(route.ID).State
		if state != "disabled" && state != "failed" && (pinned != "" || !e.settings().PoolEnabled || state == "available") {
			return true
		}
	}
	return false
}

func (e *Engine) signal() {
	select {
	case e.wake <- struct{}{}:
	default:
	}
}

// Pin selected work before releasing the session-map lock. Without this pin an
// idle-boundary request could remove the session (and its credential limit)
// between selection and refresh, leaving a probe attached to a stale guard.
func (e *Engine) backgroundWork(now time.Time) []*session {
	e.mu.Lock()
	defer e.mu.Unlock()
	var work []*session
	for key, s := range e.sessions {
		s.mu.Lock()
		idle := now.Sub(s.lastUsed) > idleLifetime
		available := s.probing == nil && !s.queued
		if idle && available && s.busy == 0 {
			delete(e.sessions, key)
		} else if !e.collectionIdleLocked(s, now) && available && s.activated {
			s.busy++
			s.queued = true
			work = append(work, s)
		}
		s.mu.Unlock()
	}
	sort.SliceStable(work, func(i, j int) bool {
		a, b := work[i].state.Status(now), work[j].state.Status(now)
		if a.Usable != b.Usable {
			return !a.Usable
		}
		nearA := a.Usable && a.Standby == 0 && a.RemainingSeconds <= e.settings().RefreshSeconds
		nearB := b.Usable && b.Standby == 0 && b.RemainingSeconds <= e.settings().RefreshSeconds
		if nearA != nearB {
			return nearA
		}
		if a.Standby != b.Standby {
			return a.Standby < b.Standby
		}
		return work[i].id < work[j].id
	})
	return work
}

func (e *Engine) refresh(ctx context.Context, s *session, bootstrap bool, only ...int) {
	e.refreshOnce(ctx, s, bootstrap, false, only...)
}

func (e *Engine) refreshOnce(ctx context.Context, s *session, bootstrap, manualRandom bool, only ...int) {
	epoch := e.injectionEpoch.Load()
	if e.disabled.Load() || e.pool.Err() != nil || (e.settings().PoolEnabled && e.statePoolReady(s, time.Now())) {
		return
	}
	s.mu.Lock()
	if pending := s.probing; pending != nil {
		s.mu.Unlock()
		select {
		case <-pending:
		case <-ctx.Done():
		}
		return
	}
	if status, _ := s.rejection(); status != 0 || e.probeSchedule(s, time.Now(), manualRandom).WaitSeconds > 0 {
		s.mu.Unlock()
		return
	}
	done := make(chan struct{})
	s.probing = done
	seekingActive := e.seekingActive(s, time.Now())
	s.mu.Unlock()
	attempts := 0
	roundCadence := e.settings().Collection.Cadence == "round"
	roundCooldown := time.Duration(e.settings().CooldownSeconds) * time.Second
	defer func() {
		if roundCadence && attempts > 0 {
			e.collection.EndRound(s.backupKey, time.Now(), roundCooldown)
		}
		s.mu.Lock()
		if roundCadence && attempts > 0 {
			s.nextProbe = e.collection.Status(s.backupKey, time.Now(), e.settings().Collection).NextAt
		}
		s.probing = nil
		close(done)
		s.mu.Unlock()
	}()
	select {
	case e.probeSlot <- struct{}{}:
		defer func() { <-e.probeSlot }()
	case <-ctx.Done():
		return
	}
	limit := min(e.settings().MaxProbes, len(e.routes))
	tried := make(map[int]bool)
	rotationStarted := false
	pinned := -1
	if pin := e.effectivePinnedRoute(); pin != "" {
		for i := range e.routes {
			if e.routes[i].ID == pin {
				pinned = i
				break
			}
		}
		if pinned < 0 {
			return
		}
		limit = 1
	}
	if len(only) > 0 {
		pinned = only[0]
		limit = 1
	}
	for i := 0; i < len(e.routes) && attempts < limit; i++ {
		// Foreground generations do not cancel a probe round. Finish the
		// already-dispatched probe and continue to the next selected node;
		// probeSlot, the account guard and the persisted budget remain active.
		if ctx.Err() != nil || e.disabled.Load() || e.injectionEpoch.Load() != epoch || (e.settings().PoolEnabled && e.statePoolReady(s, time.Now())) {
			return
		}
		s.mu.Lock()
		if seekingActive && !manualRandom && s.state.Status(time.Now()).Usable {
			s.mu.Unlock()
			return
		}
		if !bootstrap && len(only) == 0 && e.collectionIdleLocked(s, time.Now()) {
			s.mu.Unlock()
			return
		}
		if status, _ := s.rejection(); status != 0 {
			s.mu.Unlock()
			return
		}
		s.mu.Unlock()
		route := pinned
		if route < 0 {
			route = e.nextProbeInRotation(s, tried, !rotationStarted, time.Now())
			if route < 0 {
				break
			}
		}
		if pinned >= 0 {
			route = pinned
		}
		s.mu.Lock()
		paused := time.Now().Before(s.nodePauses[route])
		s.mu.Unlock()
		if paused {
			continue
		}
		if tried[route] {
			return
		}
		entry := e.pool.Get(e.routes[route].ID)
		if e.settings().PoolEnabled && len(only) == 0 && pinned < 0 && entry.State != "available" {
			continue
		}
		if entry.State == "disabled" || entry.State == "failed" {
			continue
		}
		seekingActive = seekingActive || e.seekingActive(s, time.Now())
		policy := e.settings().Collection
		if manualRandom {
			policy.Cadence = "round" // Skip local pacing only, never hourly/storage gates.
		}
		if until, reason := e.collection.ReserveRound(s.backupKey, time.Now(), policy, manualRandom || (roundCadence && attempts > 0), roundCooldown); !until.IsZero() {
			s.mu.Lock()
			s.nextProbe, s.diagnostic = until, reason
			s.mu.Unlock()
			return
		}
		if e.settings().PoolEnabled && len(only) == 0 && pinned < 0 {
			if err := e.pool.ClaimProbe(e.routes[route].ID, len(only) > 0); err != nil {
				tried[route] = true
				if e.pool.Err() != nil {
					return
				}
				continue
			}
		}
		if pinned < 0 {
			if err := e.collection.recordRotationAttempt(s.backupKey, e.routes[route].ID, time.Now()); err != nil {
				_ = e.pool.FinishProbe(e.routes[route].ID, "available", "rotation_storage_error")
				return
			}
			rotationStarted = true
		}
		attempts++
		tried[route] = true
		started := time.Now()
		e.log.Info("probe_started", "route", e.routes[route].ID, "model", s.model,
			"round_attempt", attempts, "foreground_requests", e.foreground.Load())
		token, status, retryAfter, err := e.probe(ctx, s.headers, e.routes[route], s.model)
		if ctx.Err() != nil {
			_ = e.pool.FinishProbe(e.routes[route].ID, "available", "probe_cancelled")
			return
		}
		if errors.Is(err, errProbeTransport) {
			e.failRoute(route)
			if len(only) == 0 && pinned == route {
				pinned = -1
				limit = min(e.settings().MaxProbes, len(e.routes))
			}
		}
		accepted := false
		duplicate := false
		for _, card := range e.cards(s, time.Now()) {
			if card.ID == token.Fingerprint {
				duplicate = true
				break
			}
		}
		if err == nil && !duplicate && !e.disabled.Load() && e.injectionEpoch.Load() == epoch {
			s.mu.Lock()
			entry := e.pool.Get(e.routes[route].ID)
			if entry.State != "failed" && entry.State != "disabled" {
				accepted = s.state.Offer(token, route, time.Now())
			}
			s.mu.Unlock()
			if accepted {
				e.persistState(s)
			}
		}
		result := probeReason(err)
		if err == nil {
			switch {
			case accepted:
				result = "accepted"
			case duplicate:
				result = "duplicate_state"
			case token.Blocks != s.policy.Blocks:
				result = "shape_mismatch"
			default:
				result = "state_time_rejected"
			}
		}
		// Record upstream decisions before persistence: disk failure must not
		// discard a 401/403/429 stop or allow a later node to retry it.
		rejectionStatus := status
		var streamFailure *probeStreamError
		if errors.As(err, &streamFailure) && streamFailure.status != 0 {
			rejectionStatus = streamFailure.status
		}
		if rejectionStatus == http.StatusUnauthorized || rejectionStatus == http.StatusForbidden || rejectionStatus == http.StatusTooManyRequests {
			s.mu.Lock()
			s.upstreamPause = time.Now().Add(max(30*time.Second, retryAfter))
			s.diagnostic = map[int]string{http.StatusUnauthorized: "upstream_rejected", http.StatusForbidden: "upstream_rejected", http.StatusTooManyRequests: "upstream_rate_limited"}[rejectionStatus]
			s.mu.Unlock()
		}
		// Probe failures belong to this model session only. A model that cannot
		// produce a state must never poison another model's usable state pool.
		e.collection.Complete(s.backupKey, time.Now(), accepted, e.settings().Collection, result, e.routes[route].ID)
		if e.settings().PoolEnabled {
			// A probe result is a health observation, not a permanent lease. Keep
			// successful and shape results eligible. A transport timeout is a
			// dead link for this run: remove it from automatic rotation until the
			// user explicitly puts it back from the node list.
			poolState := "available"
			if result == "network_failed" {
				poolState = "failed"
			}
			if err := e.pool.FinishProbe(e.routes[route].ID, poolState, result); err != nil {
				return
			}
		}
		s.mu.Lock()
		s.diagnostic, s.observedBlocks, s.lastProbe = result, token.Blocks, time.Now()
		// Only a still-usable successful state earns a cooldown. A concurrent 312
		// may have invalidated it already and must not be overwritten by this defer.
		if accepted && e.statePoolReady(s, time.Now()) {
			s.nextProbe = time.Now().Add(time.Duration(e.settings().CooldownSeconds) * time.Second)
		} else if accepted {
			s.nextProbe = time.Time{}
		} else {
			s.nextProbe = e.collection.Status(s.backupKey, time.Now(), e.settings().Collection).NextAt
		}
		s.mu.Unlock()
		e.recordNode(s, route, result, status, token.Blocks, time.Since(started), "probe")
		if result == "shape_mismatch" && token.Blocks == 11 {
			e.pauseMismatchedRoute(s, route, token.Blocks, time.Now(), started)
		} else if !accepted && (result == "network_failed" || status >= 500) {
			e.advanceUnavailableRoute(s, route, time.Now())
		}
		// Shape metadata explains a rejected probe without exposing its token.
		e.log.Info("probe_finished", "route", e.routes[route].ID, "status", status,
			"accepted", accepted, "result", result, "state_blocks", token.Blocks,
			"expected_blocks", s.policy.Blocks, "model", s.model,
			"duration_ms", time.Since(started).Milliseconds(), "foreground_requests", e.foreground.Load())
		// Preserve upstream account limits rather than converting them to a
		// generic "no state" error or moving on to another egress.
		if rejectionStatus == http.StatusUnauthorized || rejectionStatus == http.StatusForbidden {
			return
		}
		if rejectionStatus == http.StatusTooManyRequests {
			return
		}
		// Capacity and ordinary stream failures skip this candidate. Auth and
		// quota errors above still stop the round and respect Retry-After.
		if streamFailure != nil {
			continue
		}
		if accepted {
			if (seekingActive && s.state.Status(time.Now()).Usable) || (bootstrap && !e.settings().PoolEnabled) || !e.needsCollection(s, time.Now()) {
				return
			}
		} else if !roundCadence && result != "network_failed" {
			return
		}
	}
	if attempts == 0 {
		s.mu.Lock()
		s.diagnostic = "pool_exhausted"
		s.mu.Unlock()
	}
}

func (e *Engine) probe(parent context.Context, h http.Header, route proxyroute.Route, models ...string) (turnstate.Token, int, time.Duration, error) {
	model := e.settings().SelectedModel()
	if len(models) > 0 {
		model = models[0]
	}
	ctx, cancel := context.WithTimeout(parent, time.Duration(e.settings().ProbeSeconds)*time.Second)
	defer cancel()
	// Match the Codex Responses envelope used by the source gateway. Do not
	// force a lower reasoning effort than the upstream default for collection.
	body := map[string]any{
		"model": model, "instructions": "Reply with OK.",
		"input":  []any{map[string]any{"type": "message", "role": "user", "content": []any{map[string]any{"type": "input_text", "text": "Reply with OK."}}}},
		"stream": true, "store": false, "parallel_tool_calls": true,
		"include": []string{"reasoning.encrypted_content"},
	}
	encoded, _ := json.Marshal(body)
	req, _ := http.NewRequestWithContext(ctx, http.MethodPost, strings.TrimRight(e.settings().Upstream, "/")+"/responses", bytes.NewReader(encoded))
	req.Header = h.Clone()
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "text/event-stream")
	// A probe has no prior state and no conversation/session chain.
	client := &http.Client{Transport: route.Transport, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
	resp, err := client.Do(req)
	if err != nil {
		return turnstate.Token{}, 0, 0, errProbeTransport
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return turnstate.Token{}, resp.StatusCode, retryDelay(resp.Header.Get("Retry-After")), errProbeUpstream
	}
	token, parseErr := turnstate.Parse(resp.Header.Get(turnstate.Header))
	// Read the bounded stream before classifying the state header: a failed
	// response may omit state entirely, and must not trigger exit rotation.
	data, readErr := io.ReadAll(io.LimitReader(resp.Body, (1<<20)+1))
	if len(data) > 1<<20 {
		return token, resp.StatusCode, 0, errProbeIncomplete
	}
	finished, streamErr := probeStreamOutcome(data)
	if streamErr != nil {
		return token, resp.StatusCode, retryDelay(resp.Header.Get("Retry-After")), streamErr
	}
	if readErr != nil || !finished {
		if ctx.Err() != nil && parent.Err() == nil {
			return token, resp.StatusCode, 0, errProbeTransport
		}
		return token, resp.StatusCode, retryDelay(resp.Header.Get("Retry-After")), errProbeIncomplete
	}
	if resp.Header.Get(turnstate.Header) == "" {
		return turnstate.Token{}, resp.StatusCode, 0, errProbeMissingState
	}
	if parseErr != nil {
		return turnstate.Token{}, resp.StatusCode, 0, errProbeInvalidState
	}
	return token, resp.StatusCode, 0, nil
}

// These stable codes follow the official Codex SSE classifier, not response
// prose: openai/codex rust-v0.154.0 codex-rs/codex-api/src/sse/responses.rs.
// Unknown codes/messages never leave this parser or become guessed auth errors.
type probeStreamError struct {
	kind   string
	status int
}

func (e *probeStreamError) Error() string { return e.kind }

func probeStreamOutcome(data []byte) (bool, error) {
	finished := false
	var failure *probeStreamError
	var eventName string
	var payload []byte
	inspect := func() {
		if len(payload) == 0 {
			eventName = ""
			return
		}
		var event struct {
			Type  string `json:"type"`
			Code  string `json:"code"`
			Error struct {
				Code string `json:"code"`
			} `json:"error"`
			Response struct {
				Error struct {
					Code string `json:"code"`
				} `json:"error"`
			} `json:"response"`
		}
		if json.Unmarshal(payload, &event) == nil {
			kind := event.Type
			if kind == "" {
				kind = eventName
			}
			switch kind {
			case "response.completed":
				finished = true
			case "response.failed", "response.incomplete", "error":
				code := event.Response.Error.Code
				if code == "" {
					code = event.Error.Code
				}
				if code == "" {
					code = event.Code
				}
				current := &probeStreamError{kind: "response_failed"}
				switch code {
				case "server_is_overloaded", "slow_down":
					current.kind = "model_capacity"
				case "rate_limit_exceeded", "insufficient_quota":
					current.kind = "upstream_rate_limited"
					current.status = 429
				}
				// Keep a known account limit even if a malformed stream adds another
				// failure later. No later completed event can cancel any failure.
				if failure == nil || current.status != 0 {
					failure = current
				}
			}
		}
		payload = nil
		eventName = ""
	}
	walkSSE(data, func(name string, data []byte) {
		eventName, payload = name, data
		inspect()
	})
	if failure != nil {
		return false, failure
	}
	return finished, nil
}

func completed(data []byte) bool {
	finished, err := probeStreamOutcome(data)
	return finished && err == nil
}

// reject is shared by probes and generation responses. A valid active state
// must not let subsequent requests bypass an upstream auth or quota rejection.
func (e *Engine) reject(s *session, status int, delay time.Duration, route int) bool {
	if status != 401 && status != 403 && status != 429 {
		return false
	}
	s.mu.Lock()
	s.cursor = route
	s.mu.Unlock()
	l := s.limit
	l.mu.Lock()
	defer l.mu.Unlock()
	if status == 401 || status == 403 {
		l.blocked, l.rejectedStatus = true, status
	} else if !l.blocked {
		l.rejectedStatus = status
	}
	if delay < time.Duration(e.settings().CooldownSeconds)*time.Second {
		delay = time.Duration(e.settings().CooldownSeconds) * time.Second
	}
	until := time.Now().Add(delay)
	if until.After(l.retryUntil) {
		l.retryUntil = until
	}
	return true
}

func (s *session) rejection() (int, int) { return s.limit.rejection() }

func (l *credentialLimit) rejection() (int, int) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.blocked {
		return l.rejectedStatus, 0
	}
	if time.Now().Before(l.retryUntil) {
		return 429, int(time.Until(l.retryUntil).Seconds()) + 1
	}
	return 0, 0
}

func (e *Engine) Status() map[string]any {
	e.mu.Lock()
	defer e.mu.Unlock()
	type sessionStatus struct {
		ID string `json:"id"`
		turnstate.Status
		accountPolicy
		Model                string             `json:"model"`
		Diagnostic           string             `json:"diagnostic,omitempty"`
		DiagnosticMessage    string             `json:"diagnostic_message,omitempty"`
		ObservedBlocks       int                `json:"observed_blocks,omitempty"`
		ObservedLength       int                `json:"observed_length,omitempty"`
		CooldownSeconds      int                `json:"cooldown_seconds,omitempty"`
		LastProbe            time.Time          `json:"last_probe,omitempty"`
		Phase                string             `json:"phase"`
		RejectedStatus       int                `json:"rejected_status,omitempty"`
		RetryAfterSeconds    int                `json:"retry_after_seconds,omitempty"`
		Nodes                []NodeObservation  `json:"nodes"`
		Cards                []turnstate.Card   `json:"states"`
		Collection           CollectionStatus   `json:"collection"`
		LastStateCheck       ResponseStateCheck `json:"last_state_check"`
		LastProbeResult      *NodeObservation   `json:"last_probe_result,omitempty"`
		Events               []turnstate.Event  `json:"state_events"`
		ActiveRoute          string             `json:"active_route,omitempty"`
		UpstreamPauseSeconds int                `json:"upstream_pause_seconds,omitempty"`
	}
	states := make([]sessionStatus, 0, len(e.sessions))
	for _, s := range e.sessions {
		e.syncRoutes(s, time.Now())
		state := s.state.Status(time.Now())
		active, activeOK := s.state.Acquire(time.Now())
		activeRoute := ""
		if activeOK && active.Route >= 0 && active.Route < len(e.routes) {
			activeRoute = e.routes[active.Route].ID
		}
		status, retry := s.rejection()
		phase := "collecting"
		switch {
		case status == 401 || status == 403:
			phase = "auth_blocked"
		case status == 429:
			phase = "rate_limited"
		case e.disabled.Load():
			phase = "passthrough"
		case state.Usable:
			phase = "ready"
		default:
			s.mu.Lock()
			if time.Now().Before(s.upstreamPause) {
				phase = "upstream_paused"
			} else if s.probing == nil {
				phase = "search_queued"
			}
			s.mu.Unlock()
		}
		s.mu.Lock()
		cooldown := e.probeSchedule(s, time.Now()).WaitSeconds
		observedLength := 0
		if s.observedBlocks > 0 {
			observedLength = ((57 + 16*s.observedBlocks + 2) / 3) * 4
		}
		nodes := make([]NodeObservation, 0, len(s.nodes))
		for _, node := range s.nodes {
			node.PoolState = e.pool.Get(node.Route).State
			for i, route := range e.routes {
				if route.ID != node.Route {
					continue
				}
				node.PauseUntil = s.nodePauses[i]
				node.PauseSeconds = max(0, int(time.Until(node.PauseUntil).Seconds())+1)
				node.RetainedLast = s.retainedLast && s.retainedRoute == i && e.otherEligibleRouteLocked(s, i, time.Now()) < 0
			}
			nodes = append(nodes, node)
		}
		sort.Slice(nodes, func(i, j int) bool { return nodes[i].Route < nodes[j].Route })
		collection := e.collectionStatus(s, time.Now())
		cooldown = max(cooldown, collection.WaitSeconds)
		if collection.Reason == "pool_ready" {
			cooldown = 0
		}
		states = append(states, sessionStatus{Events: s.state.Events(), LastStateCheck: s.lastStateCheck, LastProbeResult: s.lastProbeResult, Cards: e.cards(s, time.Now()), Collection: collection, Nodes: nodes, ActiveRoute: activeRoute, UpstreamPauseSeconds: max(0, int(time.Until(s.upstreamPause).Seconds())+1), ID: s.id, Status: state, accountPolicy: s.policy, Model: s.model, Phase: phase, RejectedStatus: status, RetryAfterSeconds: retry, Diagnostic: s.diagnostic, DiagnosticMessage: diagnosticMessage(s.diagnostic, s.policy, s.observedBlocks), ObservedBlocks: s.observedBlocks, ObservedLength: observedLength, CooldownSeconds: cooldown, LastProbe: s.lastProbe})
		s.mu.Unlock()
	}
	modelOrder := map[string]int{}
	for i, model := range settings.SupportedModels() {
		modelOrder[model] = i
	}
	sort.Slice(states, func(i, j int) bool {
		if states[i].Model != states[j].Model {
			return modelOrder[states[i].Model] < modelOrder[states[j].Model]
		}
		return states[i].ID < states[j].ID
	})
	kind, reason := "official", "已开启注入；只有符合所选账号规则的 state 才会使用。长度是经验规则，不代表模型质量。"
	if e.settings().IsRelay() {
		kind, reason = "relay", "中转站使用普通转发，不采集或注入官方 turn-state。"
	} else if e.disabled.Load() {
		reason = "已关闭注入；请求按原样转发，不采集 state。"
	}
	storage := "memory"
	if e.backup != nil {
		storage = "memory+private-backup"
	}
	return map[string]any{"request_slots": map[string]any{"generation_active": len(e.bodySlots), "generation_waiting": e.bodyWaiting.Load(), "generation_limit": cap(e.bodySlots), "feature_active": len(e.featureSlots), "feature_waiting": e.featureWaiting.Load(), "feature_limit": cap(e.featureSlots)}, "pool_nodes": e.PoolStatus(), "foreground_requests": e.foreground.Load(), "max_probes_per_round": e.settings().MaxProbes, "probe_cooldown_seconds": e.settings().CooldownSeconds, "requests_total": e.requests.Load(), "last_request_unix": e.lastRequest.Load(), "upstream_kind": kind, "injection_reason": reason, "injection_enabled": !e.disabled.Load(), "model": e.settings().SelectedModel(), "supported_models": settings.SupportedModels(), "account_mode": e.settings().AccountMode, "routes": len(e.routes), "sessions": states, "state_storage": storage, "transport": "http-sse"}
}
func (e *Engine) Close() {
	for _, r := range e.routes {
		r.Close()
	}
}

func retryDelay(value string) time.Duration {
	if seconds, err := strconv.ParseInt(value, 10, 32); err == nil && seconds > 0 {
		return time.Duration(seconds) * time.Second
	}
	if at, err := http.ParseTime(value); err == nil {
		if delay := time.Until(at); delay > 0 {
			return delay
		}
	}
	return 0
}

// SetInjection changes future requests only; an in-flight generation keeps its
// immutable decision. Account rejections survive toggling injection.
func (e *Engine) SetInjection(enabled bool) {
	disabled := !enabled || e.settings().IsRelay()
	if e.disabled.Swap(disabled) != disabled {
		e.injectionEpoch.Add(1)
		e.signal()
	}
}
func (e *Engine) Restricted() bool {
	e.mu.Lock()
	defer e.mu.Unlock()
	for _, limit := range e.limits {
		if status, _ := limit.rejection(); status != 0 {
			return true
		}
	}
	return false
}

var (
	errProbeTransport    = errors.New("probe transport failed")
	errProbeUpstream     = errors.New("probe upstream rejected")
	errProbeMissingState = errors.New("probe missing state")
	errProbeInvalidState = errors.New("probe invalid state")
	errProbeIncomplete   = errors.New("probe did not complete")
)

func probeReason(err error) string {
	var streamFailure *probeStreamError
	if errors.As(err, &streamFailure) {
		return streamFailure.kind
	}
	switch {
	case err == nil:
		return "accepted"
	case errors.Is(err, errProbeTransport):
		return "network_failed"
	case errors.Is(err, errProbeUpstream):
		return "upstream_rejected"
	case errors.Is(err, errProbeMissingState):
		return "missing_state_header"
	case errors.Is(err, errProbeInvalidState):
		return "invalid_state_envelope"
	case errors.Is(err, errProbeIncomplete):
		return "incomplete_response"
	default:
		return "request_failed"
	}
}
func diagnosticMessage(code string, p accountPolicy, observed int) string {
	switch code {
	case "duplicate_state":
		return "上游返回已持有的同一张 state，没有增加备用；按失败间隔等待后再补采。"
	case "failure_interval":
		return "上次补采未成功，按失败间隔等待后再试下一个节点。"
	case "round_cooldown":
		return "本批采集结束，等待默认批次间隔后继续；没有主用时也遵守此等待，可手动随机采集一次。"
	case "missing_active":
		return "没有可用主用 state，按批次间隔寻找剩余节点；可手动随机采集一次，仍受上游暂停和总预算约束。"
	case "upstream_pause":
		return "上游要求等待，额外采集将在暂停结束后恢复；没有主用 state 也不能跳过此等待。"
	case "search_budget":
		return "连续失败次数已达到预算，暂停一段时间后自动恢复；仍遵守每小时总预算。"
	case "hourly_budget":
		return "额外采集已达到全程序每小时预算，等待额度恢复；已有可用 state 仍可正常使用。"
	case "budget_storage_error":
		return "采集预算记录无法保存，已停止额外采集；已有可用 state 仍可正常使用。"
	case "restored_backup":
		return "已从本机私有备份恢复合格 state；出现失效响应时会立即切换备用牌。"
	case "pool_exhausted":
		return "可用节点已耗尽。请到代理池补充节点，或选择已用/失败清单手动放回；不会自动重复使用。"
	case "accepted":
		return "已采到符合当前规则的 state；这不等于验证了模型质量。"
	case "shape_mismatch":
		return fmt.Sprintf("出口有响应，但 state 为 %d 块，当前规则要求 %d 块（约 %d 字符）。请先核对个人 / Team 选择；换出口不保证得到目标长度。", observed, p.Blocks, p.Length)
	case "state_time_rejected":
		return "state 已过期或时间异常；请检查本机时间；后台会继续尝试其它节点。"
	case "missing_state_header":
		return "上游没有返回 turn-state 请求头；这不是代理不通，也不代表该出口能采到目标长度。"
	case "invalid_state_envelope":
		return "上游返回的 state 格式无法识别；未注入该值。"
	case "model_capacity":
		return "当前节点返回模型繁忙或容量不足，继续尝试下一节点；仍遵守本轮次数、轮间间隔及采集总预算。"
	case "upstream_rate_limited":
		return "上游对请求或补采限流；已暂停额外探测，仍合格的 state 继续按原来源节点使用。正式 AI 请求收到限流时才会暂停该账号。"
	case "response_failed":
		return "上游在 HTTP 200 回复流中报告失败，本次未入库，继续尝试下一节点；明确的登录错误或限流仍会暂停。"
	case "incomplete_response":
		return "探测回复未完整结束，可能超时或被中断；不会把半截回复作为采集成功。"
	case "network_failed":
		return "节点连接失败或超时，已移入失败列表，不自动恢复；继续尝试下一节点，可在节点状态中手动放回。"
	case "upstream_rejected":
		return "上游拒绝了探测请求；401 / 403 会暂停该账号，429 只暂停补采，不会清除或阻断仍合格的 state。"
	default:
		return "尚无采集结果。收到该模型请求后才会开始连续采集。"
	}
}
func (e *Engine) unavailableMessage(s *session) (string, int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	schedule := e.probeSchedule(s, time.Now())
	seconds := max(1, schedule.WaitSeconds)
	diagnostic := s.diagnostic
	if schedule.WaitSeconds > 0 {
		diagnostic = schedule.Reason
	} else if diagnostic == "round_cooldown" || diagnostic == "success_cooldown" {
		diagnostic = "missing_active"
	}
	return "后台正在连续寻找合格 state；当前严格模式暂不转发正式请求，这是本地拦截。" + diagnosticMessage(diagnostic, s.policy, s.observedBlocks) + " 可在面板明确关闭注入，使用普通转发。", seconds
}

// RetryState is a bounded manual retry of an existing RAM-only session. The
// panel never supplies credentials, clears a usable state, or resets a limit.
func (e *Engine) RetryState(ctx context.Context, sessionID string, routeIDs ...string) error {
	if e.disabled.Load() {
		return errors.New("请先开启注入；普通转发模式不采集 state")
	}
	e.mu.Lock()
	var selected *session
	for _, s := range e.sessions {
		if s.id == sessionID && sessionID != "" {
			selected = s
			break
		}
	}
	if selected == nil {
		e.mu.Unlock()
		return errors.New("找不到该会话，请先在 Codex 中发送一条消息，再刷新面板")
	}
	s := selected
	s.mu.Lock()
	if s.probing != nil || s.queued {
		s.mu.Unlock()
		e.mu.Unlock()
		return errors.New("该会话正在采集，请等待本轮结束")
	}
	if !s.activated {
		s.mu.Unlock()
		e.mu.Unlock()
		return errors.New("该会话尚未开始采集，请先用此模型在 Codex 中发送一条消息")
	}
	if status, seconds := s.rejection(); status != 0 {
		s.mu.Unlock()
		e.mu.Unlock()
		if status == 429 {
			return fmt.Errorf("上游要求暂停，至少等待 %d 秒；重新采集不会重置限流", seconds)
		}
		return fmt.Errorf("上游返回 %d，请先在 Codex 处理登录或访问权限；不会更换出口重试", status)
	}
	if schedule := e.probeSchedule(s, time.Now()); schedule.WaitSeconds > 0 {
		seconds := schedule.WaitSeconds
		s.mu.Unlock()
		e.mu.Unlock()
		return fmt.Errorf("尚未到下次采集时间，请等待 %d 秒；手动采集也遵守失败间隔和总预算", seconds)
	}
	if _, usable := s.state.Acquire(time.Now()); usable && len(routeIDs) == 0 {
		s.mu.Unlock()
		e.mu.Unlock()
		return errors.New("当前 state 仍可用，无需重复采集；不会清除正在使用的状态")
	}
	s.busy++
	s.queued = true
	s.mu.Unlock()
	e.mu.Unlock()
	defer releaseWork(s)
	if len(routeIDs) > 0 && routeIDs[0] != "" {
		index := -1
		for i, r := range e.routes {
			if r.ID == routeIDs[0] {
				index = i
				break
			}
		}
		if index < 0 {
			return errors.New("节点已不存在，请刷新代理池")
		}
		if state := e.pool.Get(routeIDs[0]).State; state == "disabled" || state == "failed" {
			return errors.New("请先将已清理节点放回可用池")
		}
		e.refresh(ctx, s, false, index)
	} else {
		e.refresh(ctx, s, true)
	}
	if e.pool.Err() != nil {
		return e.pool.Err()
	}

	if ctx.Err() != nil {
		return errors.New("本轮采集已取消或超时，后台会继续采集")
	}
	if status, _ := s.rejection(); status != 0 {
		return fmt.Errorf("采集遇到上游 %d，已停止；请查看会话状态", status)
	}
	if len(routeIDs) > 0 {
		s.mu.Lock()
		diagnostic := s.diagnostic
		s.mu.Unlock()
		if diagnostic != "accepted" {
			return fmt.Errorf("单节点采集未成功：%s；已有主用 state 保留", diagnostic)
		}
	}
	if _, usable := s.state.Acquire(time.Now()); usable {
		return nil
	}
	message, _ := e.unavailableMessage(s)
	return errors.New(message)
}

func (e *Engine) SetRefreshMode(mode string) {
	e.mu.Lock()
	defer e.mu.Unlock()
	hold := mode == "on_demand"
	e.onDemand.Store(hold)
	for _, s := range e.sessions {
		s.state.HoldActive(hold)
	}
}
