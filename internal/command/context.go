package command

import (
	"cache-custom/internal/protocol"
	"cache-custom/internal/pubsub"
	"cache-custom/internal/tenant"
)

// Writer is a connection write path shared by command replies and Pub/Sub pushes.
type Writer interface {
	WriteValue(v protocol.Value) error
}

// Context carries per-connection state across commands on one TCP session.
type Context struct {
	// Quit is set by handlers that should close the connection after the reply.
	Quit bool
	// Tenant is set by AUTH (or local-profile default bind); nil means unauthenticated.
	Tenant *tenant.Tenant
	// BoundTenant is true when this connection holds a MaxClients slot via AUTH.
	// Auto-bound DefaultTenant does not set this (local profile shared default).
	BoundTenant bool
	// Writer is set by the server for this connection (command + push replies).
	Writer Writer
	// PubSub is created lazily on first SUBSCRIBE/PSUBSCRIBE for the bound tenant.
	PubSub *pubsub.Client
	// Multi holds extra RESP replies for one command (e.g. multi-channel SUBSCRIBE).
	// When non-empty, the server writes these instead of the single Dispatch return.
	Multi []protocol.Value
}

// InSubscribeMode reports Redis subscribe-mode (any active channel/pattern sub).
func (ctx *Context) InSubscribeMode() bool {
	return ctx != nil && ctx.PubSub != nil && ctx.PubSub.InSubscribeMode()
}

// AppendMulti queues an additional reply for this command.
func (ctx *Context) AppendMulti(v protocol.Value) {
	ctx.Multi = append(ctx.Multi, v)
}
