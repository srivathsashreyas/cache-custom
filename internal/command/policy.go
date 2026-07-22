package command

import (
	"fmt"
	"strings"

	"cache-custom/internal/protocol"
	"cache-custom/internal/tenant"
)

// Commands allowed without AUTH when RequireAuth is true.
var unauthenticatedAllow = map[string]struct{}{
	"AUTH":        {},
	"PING":        {},
	"ECHO":        {},
	"QUIT":        {},
	"COMMAND":     {},
	"INFO":        {},
	"TENANTSTATS": {},
}

// Policy is server-wide security / command policy (M7).
type Policy struct {
	// RequireAuth when true: data and most commands need AUTH (or default tenant).
	// When false (local profile): DefaultTenant is auto-bound for data commands.
	RequireAuth bool
	// DefaultTenant used when RequireAuth is false and the connection has not AUTHed.
	DefaultTenant *tenant.Tenant
	// Deny is upper-case command names that always return an error (even when authenticated).
	// AUTH, PING, QUIT cannot be denied.
	Deny map[string]struct{}
}

// neverDeny commands cannot be restricted via DenyCommands.
var neverDeny = map[string]struct{}{
	"AUTH": {},
	"PING": {},
	"QUIT": {},
}

// NewPolicy builds a Policy from config knobs.
func NewPolicy(requireAuth bool, defaultTenant *tenant.Tenant, deny []string) *Policy {
	p := &Policy{
		RequireAuth:   requireAuth,
		DefaultTenant: defaultTenant,
		Deny:          make(map[string]struct{}, len(deny)),
	}
	for _, d := range deny {
		key := strings.ToUpper(strings.TrimSpace(d))
		if key == "" {
			continue
		}
		if _, skip := neverDeny[key]; skip {
			continue
		}
		p.Deny[key] = struct{}{}
	}
	return p
}

// checkPolicy runs before the handler. Returns a non-empty error value if blocked.
func (p *Policy) checkPolicy(ctx *Context, name string) protocol.Value {
	if p == nil {
		return protocol.Value{}
	}
	if _, denied := p.Deny[name]; denied {
		return protocol.ErrorValue(fmt.Sprintf("ERR command '%s' is disabled by server configuration", strings.ToLower(name)))
	}
	if !p.RequireAuth {
		// Local / open profile: auto-bind default tenant when unset.
		if ctx != nil && ctx.Tenant == nil && p.DefaultTenant != nil {
			ctx.Tenant = p.DefaultTenant
		}
		return protocol.Value{}
	}
	// Auth required: allow unauthenticated only for the allowlist.
	if ctx == nil || ctx.Tenant == nil {
		if _, ok := unauthenticatedAllow[name]; ok {
			return protocol.Value{}
		}
		return protocol.ErrorValue("NOAUTH Authentication required.")
	}
	return protocol.Value{}
}
