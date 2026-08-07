package command

import "cache-custom/internal/protocol"

// RegisterSetCommands wires M11 set commands.
func RegisterSetCommands(r *Registry) {
	r.Register("SADD", func(ctx *Context, args []string) protocol.Value {
		if len(args) < 3 {
			return protocol.ErrorValue("ERR wrong number of arguments for 'sadd' command")
		}
		ten, errv := requireDB(ctx)
		if ten == nil {
			return errv
		}
		n, err := ten.DB.SAdd(args[1], args[2:])
		if err != nil {
			return storeErr(err)
		}
		return protocol.IntegerValue(n)
	})
	r.Register("SREM", func(ctx *Context, args []string) protocol.Value {
		if len(args) < 3 {
			return protocol.ErrorValue("ERR wrong number of arguments for 'srem' command")
		}
		ten, errv := requireDB(ctx)
		if ten == nil {
			return errv
		}
		n, err := ten.DB.SRem(args[1], args[2:])
		if err != nil {
			return storeErr(err)
		}
		return protocol.IntegerValue(n)
	})
	r.Register("SISMEMBER", func(ctx *Context, args []string) protocol.Value {
		if len(args) != 3 {
			return protocol.ErrorValue("ERR wrong number of arguments for 'sismember' command")
		}
		ten, errv := requireDB(ctx)
		if ten == nil {
			return errv
		}
		n, err := ten.DB.SIsMember(args[1], args[2])
		if err != nil {
			return storeErr(err)
		}
		return protocol.IntegerValue(n)
	})
	r.Register("SMEMBERS", func(ctx *Context, args []string) protocol.Value {
		if len(args) != 2 {
			return protocol.ErrorValue("ERR wrong number of arguments for 'smembers' command")
		}
		ten, errv := requireDB(ctx)
		if ten == nil {
			return errv
		}
		ms, err := ten.DB.SMembers(args[1])
		if err != nil {
			return storeErr(err)
		}
		out := make([]protocol.Value, len(ms))
		for i, s := range ms {
			out[i] = protocol.BulkStringValue(s)
		}
		return protocol.ArrayValue(out...)
	})
	r.Register("SCARD", func(ctx *Context, args []string) protocol.Value {
		if len(args) != 2 {
			return protocol.ErrorValue("ERR wrong number of arguments for 'scard' command")
		}
		ten, errv := requireDB(ctx)
		if ten == nil {
			return errv
		}
		n, err := ten.DB.SCard(args[1])
		if err != nil {
			return storeErr(err)
		}
		return protocol.IntegerValue(n)
	})
	r.Register("SINTER", func(ctx *Context, args []string) protocol.Value {
		if len(args) < 2 {
			return protocol.ErrorValue("ERR wrong number of arguments for 'sinter' command")
		}
		ten, errv := requireDB(ctx)
		if ten == nil {
			return errv
		}
		ms, err := ten.DB.SInter(args[1:])
		if err != nil {
			return storeErr(err)
		}
		out := make([]protocol.Value, len(ms))
		for i, s := range ms {
			out[i] = protocol.BulkStringValue(s)
		}
		return protocol.ArrayValue(out...)
	})
	r.Register("SUNION", func(ctx *Context, args []string) protocol.Value {
		if len(args) < 2 {
			return protocol.ErrorValue("ERR wrong number of arguments for 'sunion' command")
		}
		ten, errv := requireDB(ctx)
		if ten == nil {
			return errv
		}
		ms, err := ten.DB.SUnion(args[1:])
		if err != nil {
			return storeErr(err)
		}
		out := make([]protocol.Value, len(ms))
		for i, s := range ms {
			out[i] = protocol.BulkStringValue(s)
		}
		return protocol.ArrayValue(out...)
	})
	r.Register("SDIFF", func(ctx *Context, args []string) protocol.Value {
		if len(args) < 2 {
			return protocol.ErrorValue("ERR wrong number of arguments for 'sdiff' command")
		}
		ten, errv := requireDB(ctx)
		if ten == nil {
			return errv
		}
		ms, err := ten.DB.SDiff(args[1:])
		if err != nil {
			return storeErr(err)
		}
		out := make([]protocol.Value, len(ms))
		for i, s := range ms {
			out[i] = protocol.BulkStringValue(s)
		}
		return protocol.ArrayValue(out...)
	})
}
