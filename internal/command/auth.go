package command

import (
	"strings"

	"cache-custom/internal/protocol"
	"cache-custom/internal/tenant"
)

// RegisterAuth wires AUTH against the tenant registry.
func RegisterAuth(r *Registry, tenants *tenant.Registry) {
	r.Register("AUTH", func(ctx *Context, args []string) protocol.Value {
		// ACL-style: AUTH <username> <password>
		if len(args) != 3 {
			return protocol.ErrorValue("ERR wrong number of arguments for 'auth' command")
		}
		user, pass := args[1], args[2]
		t, err := tenants.Authenticate(user, pass)
		if err != nil {
			msg := err.Error()
			if strings.HasPrefix(msg, "WRONGPASS") {
				return protocol.ErrorValue(msg)
			}
			return protocol.ErrorValue(msg)
		}
		ctx.Tenant = t
		return protocol.SimpleStringValue("OK")
	})
}

// requireDB returns the authenticated tenant store, or a NOAUTH/disabled error value.
func requireDB(ctx *Context) (*tenant.Tenant, protocol.Value) {
	if ctx == nil || ctx.Tenant == nil {
		return nil, protocol.ErrorValue("NOAUTH Authentication required.")
	}
	if ctx.Tenant.Status != tenant.StatusActive {
		return nil, protocol.ErrorValue("ERR tenant is disabled")
	}
	if ctx.Tenant.DB == nil {
		return nil, protocol.ErrorValue("ERR tenant store unavailable")
	}
	return ctx.Tenant, protocol.Value{}
}
