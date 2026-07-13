package command

import "cache-custom/internal/tenant"

// Context carries per-connection state across commands on one TCP session.
type Context struct {
	// Quit is set by handlers that should close the connection after the reply.
	Quit bool
	// Tenant is set by AUTH; nil means unauthenticated.
	Tenant *tenant.Tenant
}
