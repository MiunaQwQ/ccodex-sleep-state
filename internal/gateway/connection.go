package gateway

import "github.com/gylive/ccodex-sleep-state/internal/proxyroute"

func (e *Engine) recordConnectionEvent(route int, event proxyroute.ConnectionEvent) {
	e.logConnectionEvent(e.routes[route].ID, event)
}

func (e *Engine) logConnectionEvent(routeID string, event proxyroute.ConnectionEvent) {
	e.log.Warn("route_connection_error", "route", routeID, "error_class", event.Category,
		"stage", event.Stage, "connection_generation", event.Generation,
		"connection_rebuilt", event.Recovered, "next_generation", event.NextGeneration,
		"recovery_error", event.RecoveryError)
}
