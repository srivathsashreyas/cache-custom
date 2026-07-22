// Package command provides a Redis-style command registry and M1 handlers.
package command

import (
	"fmt"
	"strings"
	"sync"

	"cache-custom/internal/protocol"
	"cache-custom/internal/tenant"
)

// Handler executes a command. args[0] is the command name.
type Handler func(ctx *Context, args []string) protocol.Value

// Registry maps uppercase command names to handlers.
type Registry struct {
	mu       sync.RWMutex
	handlers map[string]Handler
	// names preserves registration order for COMMAND.
	names []string
	// Policy optional security policy (M7); nil = no extra checks beyond handlers.
	Policy *Policy
	// Runtime optional live data for INFO (M8).
	Runtime *RuntimeInfo
}

// NewRegistry returns an empty registry.
func NewRegistry() *Registry {
	return &Registry{handlers: make(map[string]Handler)}
}

// SetPolicy installs a security policy used by Dispatch.
func (r *Registry) SetPolicy(p *Policy) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.Policy = p
}

// SetRuntime installs live INFO/metrics sources. Safe to call before or after RegisterDefaults.
func (r *Registry) SetRuntime(rt *RuntimeInfo) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.Runtime = rt
}

// Register adds a command handler. Name is matched case-insensitively.
func (r *Registry) Register(name string, h Handler) {
	key := strings.ToUpper(name)
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, exists := r.handlers[key]; !exists {
		r.names = append(r.names, key)
	}
	r.handlers[key] = h
}

// Dispatch runs the command in args. Unknown commands return -ERR.
func (r *Registry) Dispatch(ctx *Context, args []string) protocol.Value {
	if len(args) == 0 {
		return protocol.ErrorValue("ERR empty command")
	}
	name := strings.ToUpper(args[0])
	// Redis subscribe-mode: only (P)SUB/(P)UNSUB/PING/QUIT.
	if ctx != nil && ctx.InSubscribeMode() {
		switch name {
		case "SUBSCRIBE", "PSUBSCRIBE", "UNSUBSCRIBE", "PUNSUBSCRIBE", "PING", "QUIT":
		default:
			return protocol.ErrorValue(fmt.Sprintf(
				"ERR Can't execute '%s': only (P|S)SUBSCRIBE / (P|S)UNSUBSCRIBE / PING / QUIT are allowed in this context",
				args[0],
			))
		}
	}
	r.mu.RLock()
	pol := r.Policy
	h, ok := r.handlers[name]
	r.mu.RUnlock()
	if !ok {
		return protocol.ErrorValue(fmt.Sprintf("ERR unknown command '%s'", args[0]))
	}
	if errv := pol.checkPolicy(ctx, name); errv.Type == protocol.Error {
		return errv
	}
	return h(ctx, args)
}

// Names returns registered command names in registration order.
func (r *Registry) Names() []string {
	r.mu.RLock()
	defer r.mu.RUnlock()
	out := make([]string, len(r.names))
	copy(out, r.names)
	return out
}

// RegisterDefaults registers connectivity / health commands (no data plane).
// Pass tenants for INFO tenant stats; nil yields server section only.
// If r.Runtime is set later via SetRuntime, INFO uses the richer RuntimeInfo;
// until then a RuntimeInfo{Tenants: tenants} is used.
func RegisterDefaults(r *Registry, tenants *tenant.Registry) {
	r.Register("PING", ping)
	r.Register("ECHO", echo)
	r.Register("QUIT", quit)
	r.Register("COMMAND", makeCommandHandler(r))
	// INFO reads r.Runtime dynamically so SetRuntime after registration still works.
	r.Register("INFO", func(ctx *Context, args []string) protocol.Value {
		r.mu.RLock()
		rt := r.Runtime
		r.mu.RUnlock()
		if rt == nil {
			rt = &RuntimeInfo{Tenants: tenants}
		} else if rt.Tenants == nil && tenants != nil {
			// shallow copy so we don't mutate shared Runtime under lock
			cp := *rt
			cp.Tenants = tenants
			rt = &cp
		}
		return makeInfoHandler(rt)(ctx, args)
	})
}

// ping: no arg → +PONG; one arg → bulk echo of that arg (Redis-compatible).
func ping(ctx *Context, args []string) protocol.Value {
	if len(args) > 2 {
		return protocol.ErrorValue("ERR wrong number of arguments for 'ping' command")
	}
	// In subscribe mode Redis returns a multi-bulk pong.
	if ctx != nil && ctx.InSubscribeMode() {
		msg := ""
		if len(args) == 2 {
			msg = args[1]
		}
		return protocol.ArrayValue(
			protocol.BulkStringValue("pong"),
			protocol.BulkStringValue(msg),
		)
	}
	switch len(args) {
	case 1:
		return protocol.SimpleStringValue("PONG")
	case 2:
		return protocol.BulkStringValue(args[1])
	default:
		return protocol.ErrorValue("ERR wrong number of arguments for 'ping' command")
	}
}

func echo(ctx *Context, args []string) protocol.Value {
	if len(args) != 2 {
		return protocol.ErrorValue("ERR wrong number of arguments for 'echo' command")
	}
	return protocol.BulkStringValue(args[1])
}

func quit(ctx *Context, args []string) protocol.Value {
	if len(args) != 1 {
		return protocol.ErrorValue("ERR wrong number of arguments for 'quit' command")
	}
	ctx.Quit = true
	return protocol.SimpleStringValue("OK")
}

func makeCommandHandler(r *Registry) Handler {
	return func(ctx *Context, args []string) protocol.Value {
		// COMMAND with no args: list names as bulk strings (minimal Redis-like).
		if len(args) == 1 {
			names := r.Names()
			elems := make([]protocol.Value, len(names))
			for i, n := range names {
				elems[i] = protocol.BulkStringValue(n)
			}
			return protocol.ArrayValue(elems...)
		}
		// COMMAND COUNT
		if len(args) == 2 && strings.EqualFold(args[1], "COUNT") {
			return protocol.IntegerValue(int64(len(r.Names())))
		}
		return protocol.ErrorValue("ERR unknown subcommand or wrong number of arguments for 'command'")
	}
}


