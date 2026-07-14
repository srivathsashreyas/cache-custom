// Package command provides a Redis-style command registry and M1 handlers.
package command

import (
	"fmt"
	"runtime"
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
}

// NewRegistry returns an empty registry.
func NewRegistry() *Registry {
	return &Registry{handlers: make(map[string]Handler)}
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
	r.mu.RLock()
	h, ok := r.handlers[name]
	r.mu.RUnlock()
	if !ok {
		return protocol.ErrorValue(fmt.Sprintf("ERR unknown command '%s'", args[0]))
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
func RegisterDefaults(r *Registry, tenants *tenant.Registry) {
	r.Register("PING", ping)
	r.Register("ECHO", echo)
	r.Register("QUIT", quit)
	r.Register("COMMAND", makeCommandHandler(r))
	r.Register("INFO", makeInfoHandler(tenants))
}

// ping: no arg → +PONG; one arg → bulk echo of that arg (Redis-compatible).
func ping(ctx *Context, args []string) protocol.Value {
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

func makeInfoHandler(tenants *tenant.Registry) Handler {
	return func(ctx *Context, args []string) protocol.Value {
		if len(args) > 2 {
			return protocol.ErrorValue("ERR wrong number of arguments for 'info' command")
		}
		section := "default"
		if len(args) == 2 {
			section = strings.ToLower(args[1])
		}

		var b strings.Builder
		writeServer := func() {
			b.WriteString("# Server\r\n")
			b.WriteString("redis_mode:standalone\r\n")
			b.WriteString("tcp_port:9001\r\n")
			b.WriteString(fmt.Sprintf("go_version:%s\r\n", runtime.Version()))
			b.WriteString("executable:cache-custom\r\n")
			b.WriteString("\r\n")
		}
		writeTenants := func() {
			if tenants == nil {
				return
			}
			b.WriteString("# Tenants\r\n")
			b.WriteString(fmt.Sprintf("tenant_count:%d\r\n", tenants.Len()))
			for _, t := range tenants.All() {
				used, max, keys, hits, misses, evictions := t.DB.Stats()
				status := "active"
				if t.Status != tenant.StatusActive {
					status = "disabled"
				}
				prefix := "tenant_" + t.Name + "_"
				b.WriteString(fmt.Sprintf("%sstatus:%s\r\n", prefix, status))
				b.WriteString(fmt.Sprintf("%sused_memory:%d\r\n", prefix, used))
				b.WriteString(fmt.Sprintf("%smax_memory:%d\r\n", prefix, max))
				b.WriteString(fmt.Sprintf("%skeys:%d\r\n", prefix, keys))
				b.WriteString(fmt.Sprintf("%shits:%d\r\n", prefix, hits))
				b.WriteString(fmt.Sprintf("%smisses:%d\r\n", prefix, misses))
				b.WriteString(fmt.Sprintf("%sevictions:%d\r\n", prefix, evictions))
				b.WriteString(fmt.Sprintf("%sstrategy:%d\r\n", prefix, int(t.Strategy)))
				b.WriteString(fmt.Sprintf("%seviction_policy:%s\r\n", prefix, t.Policy))
				b.WriteString(fmt.Sprintf("%sshards:%d\r\n", prefix, t.Shards))
			}
			// Bound tenant summary (if any).
			if ctx != nil && ctx.Tenant != nil {
				b.WriteString(fmt.Sprintf("current_tenant:%s\r\n", ctx.Tenant.Name))
			}
			b.WriteString("\r\n")
		}

		switch section {
		case "server":
			writeServer()
		case "tenants":
			writeTenants()
		case "default", "all":
			writeServer()
			writeTenants()
		default:
			return protocol.BulkStringValue("")
		}
		return protocol.BulkStringValue(b.String())
	}
}
