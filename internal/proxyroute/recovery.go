package proxyroute

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"io"
	"net"
	"net/http"
	"strings"
	"sync"
	"syscall"
	"time"
)

// Only fixed categories leave this package; node addresses and credentials do not.
type connectionError struct{ category string }

func (e *connectionError) Error() string { return "proxy connection failed: " + e.category }

func ErrorCategory(err error) string {
	if err == nil {
		return ""
	}
	var safe *connectionError
	if errors.As(err, &safe) {
		return safe.category
	}
	if errors.Is(err, context.Canceled) {
		return "cancelled"
	}
	var dns *net.DNSError
	if errors.As(err, &dns) {
		return "dns"
	}
	var cert *tls.CertificateVerificationError
	var unknown x509.UnknownAuthorityError
	var record tls.RecordHeaderError
	if errors.As(err, &cert) || errors.As(err, &unknown) || errors.As(err, &record) {
		return "tls"
	}
	var timeout net.Error
	if errors.Is(err, context.DeadlineExceeded) || (errors.As(err, &timeout) && timeout.Timeout()) {
		return "timeout"
	}
	if errors.Is(err, syscall.ECONNRESET) {
		return "connection_reset"
	}
	if errors.Is(err, syscall.ECONNREFUSED) {
		return "connection_refused"
	}
	if errors.Is(err, io.ErrUnexpectedEOF) || errors.Is(err, io.EOF) {
		return "unexpected_eof"
	}
	if errors.Is(err, net.ErrClosed) || errors.Is(err, syscall.EPIPE) {
		return "connection_closed"
	}
	// Some proxy cores wrap protocol errors as strings. Match known categories,
	// never return the original string (which can include a subscription secret).
	s := strings.ToLower(err.Error())
	switch {
	case strings.Contains(s, "tls:"), strings.Contains(s, "x509:"):
		return "tls"
	case strings.Contains(s, "connection reset"):
		return "connection_reset"
	case strings.Contains(s, "connection refused"):
		return "connection_refused"
	case strings.Contains(s, "session closed"), strings.Contains(s, "closed connection"):
		return "connection_closed"
	case strings.Contains(s, "timeout"), strings.Contains(s, "timed out"):
		return "timeout"
	default:
		return "io_error"
	}
}

type ConnectionHealth struct {
	Generation          uint64    `json:"generation"`
	Recoveries          uint64    `json:"recoveries"`
	ConsecutiveFailures int       `json:"consecutive_failures"`
	LastError           string    `json:"last_error,omitempty"`
	LastErrorAt         time.Time `json:"last_error_at,omitzero"`
	LastRecoveryAt      time.Time `json:"last_recovery_at,omitzero"`
	RecoveryError       string    `json:"recovery_error,omitempty"`
}

type ConnectionEvent struct {
	Category, Stage            string
	Generation, NextGeneration uint64
	Recovered                  bool
	RecoveryError              string
}

type transportFactory func() (*http.Transport, func() error, error)
type connectionGeneration struct {
	id        uint64
	transport *http.Transport
	close     func() error
	users     int
	retired   bool
	dispose   sync.Once
}

func (g *connectionGeneration) shutdown() {
	g.dispose.Do(func() {
		g.transport.CloseIdleConnections()
		if g.close != nil {
			_ = g.close()
		}
	})
}

type routeRuntime struct {
	mu          sync.Mutex
	current     *connectionGeneration
	factory     transportFactory
	health      ConnectionHealth
	lastAttempt time.Time
	closed      bool
}

func newRuntime(factory transportFactory) (*routeRuntime, error) {
	tr, close, err := factory()
	if err != nil {
		return nil, err
	}
	return &routeRuntime{factory: factory, current: &connectionGeneration{id: 1, transport: tr, close: close}, health: ConnectionHealth{Generation: 1}}, nil
}

func (r Route) ConnectionHealth() ConnectionHealth {
	if r.runtime == nil {
		return ConnectionHealth{}
	}
	r.runtime.mu.Lock()
	defer r.runtime.mu.Unlock()
	return r.runtime.health
}

// ClientTransport leases the selected connection generation through body.Close.
// A replacement is used only by later requests; active streams retain their lease.
// A nonzero headerTimeout makes an isolated transport for long-running features.
func (r Route) ClientTransport(headerTimeout time.Duration, observe func(ConnectionEvent)) http.RoundTripper {
	return &recoveringTransport{route: r, headerTimeout: headerTimeout, observe: observe}
}

type recoveringTransport struct {
	route         Route
	headerTimeout time.Duration
	observe       func(ConnectionEvent)
}

func (t *recoveringTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	rt := t.route.runtime
	g := &connectionGeneration{transport: t.route.Transport}
	if rt != nil {
		rt.mu.Lock()
		if rt.closed {
			rt.mu.Unlock()
			return nil, net.ErrClosed
		}
		g = rt.current
		g.users++
		rt.mu.Unlock()
	}
	tr := g.transport
	if tr == nil {
		tr = baseTransport()
	}
	if t.headerTimeout != 0 {
		tr = tr.Clone()
		tr.ResponseHeaderTimeout = t.headerTimeout
	}
	var once sync.Once
	finish := func(err error, stage string) {
		once.Do(func() {
			if t.headerTimeout != 0 {
				tr.CloseIdleConnections()
			}
			if errors.Is(req.Context().Err(), context.Canceled) {
				err = context.Canceled
			}
			var event ConnectionEvent
			if rt != nil {
				event = rt.finish(g, err, stage)
			} else {
				event = ConnectionEvent{Category: ErrorCategory(err), Stage: stage}
			}
			if t.observe != nil && event.Category != "" && event.Category != "cancelled" {
				t.observe(event)
			}
		})
	}
	resp, err := tr.RoundTrip(req)
	if err != nil {
		finish(err, "connect")
		return resp, err
	}
	resp.Body = &connectionBody{ReadCloser: resp.Body, finish: finish}
	return resp, nil
}

type connectionBody struct {
	io.ReadCloser
	finish func(error, string)
	err    error
	mu     sync.Mutex
}

func (b *connectionBody) Read(p []byte) (int, error) {
	n, err := b.ReadCloser.Read(p)
	if err != nil && err != io.EOF {
		b.mu.Lock()
		b.err = err
		b.mu.Unlock()
	}
	return n, err
}
func (b *connectionBody) Close() error {
	err := b.ReadCloser.Close()
	b.mu.Lock()
	readErr := b.err
	b.mu.Unlock()
	if readErr != nil {
		b.finish(readErr, "read_body")
	} else {
		b.finish(err, "read_body")
	}
	return err
}

func (rt *routeRuntime) finish(g *connectionGeneration, err error, stage string) ConnectionEvent {
	rt.mu.Lock()
	event := ConnectionEvent{Category: ErrorCategory(err), Stage: stage, Generation: g.id, NextGeneration: rt.current.id}
	if !rt.closed && g == rt.current {
		now := time.Now()
		if err == nil {
			rt.health.ConsecutiveFailures = 0
		} else if event.Category != "cancelled" {
			rt.health.ConsecutiveFailures++
			rt.health.LastError, rt.health.LastErrorAt = event.Category, now
			// This limits reconstruction storms, not requests or manual tickets.
			if rt.health.ConsecutiveFailures >= 2 && now.Sub(rt.lastAttempt) >= 10*time.Second {
				rt.lastAttempt = now
				tr, close, buildErr := rt.factory() // Construction performs no network request.
				if buildErr != nil {
					rt.health.RecoveryError = "rebuild_failed"
					event.RecoveryError = "rebuild_failed"
				} else {
					g.retired = true
					g.transport.CloseIdleConnections()
					rt.current = &connectionGeneration{id: g.id + 1, transport: tr, close: close}
					rt.health.Generation, rt.health.Recoveries = g.id+1, rt.health.Recoveries+1
					rt.health.ConsecutiveFailures, rt.health.RecoveryError = 0, ""
					rt.health.LastRecoveryAt = now
					event.Recovered, event.NextGeneration = true, g.id+1
				}
			}
		}
	}
	g.users--
	dispose := g.retired && g.users == 0
	rt.mu.Unlock()
	if dispose {
		g.shutdown()
	}
	return event
}

func (rt *routeRuntime) close() {
	rt.mu.Lock()
	rt.closed = true
	g := rt.current
	g.retired = true
	dispose := g.users == 0
	rt.mu.Unlock()
	g.transport.CloseIdleConnections()
	if dispose {
		g.shutdown()
	}
}
