package command

import (
	"fmt"
	"strings"

	"cache-custom/internal/protocol"
	"cache-custom/internal/pubsub"
	"cache-custom/internal/tenant"
)

// RegisterPubSub wires tenant-scoped Pub/Sub commands.
func RegisterPubSub(r *Registry) {
	r.Register("SUBSCRIBE", cmdSubscribe)
	r.Register("UNSUBSCRIBE", cmdUnsubscribe)
	r.Register("PSUBSCRIBE", cmdPSubscribe)
	r.Register("PUNSUBSCRIBE", cmdPUnsubscribe)
	r.Register("PUBLISH", cmdPublish)
	r.Register("PUBSUB", cmdPubSub)
}

func requireTenant(ctx *Context) (*tenant.Tenant, protocol.Value) {
	if ctx == nil || ctx.Tenant == nil {
		return nil, protocol.ErrorValue("NOAUTH Authentication required.")
	}
	if ctx.Tenant.Status != tenant.StatusActive {
		return nil, protocol.ErrorValue("ERR tenant is disabled")
	}
	if ctx.Tenant.PubSub == nil {
		return nil, protocol.ErrorValue("ERR pubsub unavailable")
	}
	return ctx.Tenant, protocol.Value{}
}

func ensurePubSubClient(ctx *Context, ten *tenant.Tenant) (*pubsub.Client, protocol.Value) {
	if ctx.Writer == nil {
		return nil, protocol.ErrorValue("ERR connection writer unavailable")
	}
	if ctx.PubSub == nil {
		ctx.PubSub = ten.PubSub.NewClient(writerDeliverer{w: ctx.Writer})
	}
	return ctx.PubSub, protocol.Value{}
}

type writerDeliverer struct{ w Writer }

func (d writerDeliverer) Deliver(v protocol.Value) error {
	return d.w.WriteValue(v)
}

func cmdSubscribe(ctx *Context, args []string) protocol.Value {
	if len(args) < 2 {
		return protocol.ErrorValue("ERR wrong number of arguments for 'subscribe' command")
	}
	ten, errv := requireTenant(ctx)
	if ten == nil {
		return errv
	}
	cli, errv := ensurePubSubClient(ctx, ten)
	if cli == nil {
		return errv
	}
	conf := cli.Subscribe(args[1:]...)
	return emitConfirms(ctx, conf)
}

func cmdUnsubscribe(ctx *Context, args []string) protocol.Value {
	ten, errv := requireTenant(ctx)
	if ten == nil {
		return errv
	}
	if ctx.PubSub == nil {
		// Redis still returns unsubscribe confirms with count 0.
		ctx.AppendMulti(pubsub.SubConfirm{Kind: "unsubscribe", Name: "", Count: 0}.ToValue())
		return ctx.Multi[0]
	}
	var chans []string
	if len(args) > 1 {
		chans = args[1:]
	}
	conf := ctx.PubSub.Unsubscribe(chans...)
	if len(conf) == 0 {
		ctx.AppendMulti(pubsub.SubConfirm{Kind: "unsubscribe", Name: "", Count: ctx.PubSub.SubCount()}.ToValue())
		return ctx.Multi[0]
	}
	return emitConfirms(ctx, conf)
}

func cmdPSubscribe(ctx *Context, args []string) protocol.Value {
	if len(args) < 2 {
		return protocol.ErrorValue("ERR wrong number of arguments for 'psubscribe' command")
	}
	ten, errv := requireTenant(ctx)
	if ten == nil {
		return errv
	}
	cli, errv := ensurePubSubClient(ctx, ten)
	if cli == nil {
		return errv
	}
	conf := cli.PSubscribe(args[1:]...)
	return emitConfirms(ctx, conf)
}

func cmdPUnsubscribe(ctx *Context, args []string) protocol.Value {
	ten, errv := requireTenant(ctx)
	if ten == nil {
		return errv
	}
	if ctx.PubSub == nil {
		ctx.AppendMulti(pubsub.SubConfirm{Kind: "punsubscribe", Name: "", Count: 0}.ToValue())
		return ctx.Multi[0]
	}
	var pats []string
	if len(args) > 1 {
		pats = args[1:]
	}
	conf := ctx.PubSub.PUnsubscribe(pats...)
	if len(conf) == 0 {
		ctx.AppendMulti(pubsub.SubConfirm{Kind: "punsubscribe", Name: "", Count: ctx.PubSub.SubCount()}.ToValue())
		return ctx.Multi[0]
	}
	return emitConfirms(ctx, conf)
}

func emitConfirms(ctx *Context, conf []pubsub.SubConfirm) protocol.Value {
	if len(conf) == 0 {
		return protocol.ErrorValue("ERR wrong number of arguments for subscribe command")
	}
	for _, c := range conf {
		ctx.AppendMulti(c.ToValue())
	}
	return conf[len(conf)-1].ToValue()
}

func cmdPublish(ctx *Context, args []string) protocol.Value {
	if len(args) != 3 {
		return protocol.ErrorValue("ERR wrong number of arguments for 'publish' command")
	}
	ten, errv := requireTenant(ctx)
	if ten == nil {
		return errv
	}
	n := ten.PubSub.Publish(args[1], args[2])
	return protocol.IntegerValue(int64(n))
}

func cmdPubSub(ctx *Context, args []string) protocol.Value {
	if len(args) < 2 {
		return protocol.ErrorValue("ERR wrong number of arguments for 'pubsub' command")
	}
	ten, errv := requireTenant(ctx)
	if ten == nil {
		return errv
	}
	sub := strings.ToUpper(args[1])
	switch sub {
	case "CHANNELS":
		pat := ""
		if len(args) >= 3 {
			pat = args[2]
		}
		if len(args) > 3 {
			return protocol.ErrorValue("ERR wrong number of arguments for 'pubsub|channels' command")
		}
		chs := ten.PubSub.Channels(pat)
		elems := make([]protocol.Value, len(chs))
		for i, ch := range chs {
			elems[i] = protocol.BulkStringValue(ch)
		}
		return protocol.ArrayValue(elems...)
	case "NUMSUB":
		chans := args[2:]
		counts := ten.PubSub.NumSub(chans...)
		// Redis: flat array [chan, count, chan, count, ...]
		elems := make([]protocol.Value, 0, len(chans)*2)
		for i, ch := range chans {
			elems = append(elems, protocol.BulkStringValue(ch), protocol.IntegerValue(counts[i]))
		}
		return protocol.ArrayValue(elems...)
	case "NUMPAT":
		if len(args) != 2 {
			return protocol.ErrorValue("ERR wrong number of arguments for 'pubsub|numpat' command")
		}
		return protocol.IntegerValue(ten.PubSub.NumPat())
	default:
		return protocol.ErrorValue(fmt.Sprintf("ERR unknown subcommand or wrong number of arguments for '%s'", args[1]))
	}
}
