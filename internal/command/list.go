package command

import (
	"strconv"

	"cache-custom/internal/protocol"
)

// RegisterListCommands wires M11 list commands.
func RegisterListCommands(r *Registry) {
	r.Register("LPUSH", func(ctx *Context, args []string) protocol.Value {
		if len(args) < 3 {
			return protocol.ErrorValue("ERR wrong number of arguments for 'lpush' command")
		}
		ten, errv := requireDB(ctx)
		if ten == nil {
			return errv
		}
		n, err := ten.DB.LPush(args[1], args[2:])
		if err != nil {
			return storeErr(err)
		}
		return protocol.IntegerValue(n)
	})
	r.Register("RPUSH", func(ctx *Context, args []string) protocol.Value {
		if len(args) < 3 {
			return protocol.ErrorValue("ERR wrong number of arguments for 'rpush' command")
		}
		ten, errv := requireDB(ctx)
		if ten == nil {
			return errv
		}
		n, err := ten.DB.RPush(args[1], args[2:])
		if err != nil {
			return storeErr(err)
		}
		return protocol.IntegerValue(n)
	})
	r.Register("LPOP", func(ctx *Context, args []string) protocol.Value {
		if len(args) != 2 {
			return protocol.ErrorValue("ERR wrong number of arguments for 'lpop' command")
		}
		ten, errv := requireDB(ctx)
		if ten == nil {
			return errv
		}
		v, ok, err := ten.DB.LPop(args[1])
		if err != nil {
			return storeErr(err)
		}
		if !ok {
			return protocol.NullValue()
		}
		return protocol.BulkStringValue(v)
	})
	r.Register("RPOP", func(ctx *Context, args []string) protocol.Value {
		if len(args) != 2 {
			return protocol.ErrorValue("ERR wrong number of arguments for 'rpop' command")
		}
		ten, errv := requireDB(ctx)
		if ten == nil {
			return errv
		}
		v, ok, err := ten.DB.RPop(args[1])
		if err != nil {
			return storeErr(err)
		}
		if !ok {
			return protocol.NullValue()
		}
		return protocol.BulkStringValue(v)
	})
	r.Register("LLEN", func(ctx *Context, args []string) protocol.Value {
		if len(args) != 2 {
			return protocol.ErrorValue("ERR wrong number of arguments for 'llen' command")
		}
		ten, errv := requireDB(ctx)
		if ten == nil {
			return errv
		}
		n, err := ten.DB.LLen(args[1])
		if err != nil {
			return storeErr(err)
		}
		return protocol.IntegerValue(n)
	})
	r.Register("LRANGE", func(ctx *Context, args []string) protocol.Value {
		if len(args) != 4 {
			return protocol.ErrorValue("ERR wrong number of arguments for 'lrange' command")
		}
		ten, errv := requireDB(ctx)
		if ten == nil {
			return errv
		}
		start, err1 := strconv.ParseInt(args[2], 10, 64)
		stop, err2 := strconv.ParseInt(args[3], 10, 64)
		if err1 != nil || err2 != nil {
			return protocol.ErrorValue("ERR value is not an integer or out of range")
		}
		elems, err := ten.DB.LRange(args[1], start, stop)
		if err != nil {
			return storeErr(err)
		}
		out := make([]protocol.Value, len(elems))
		for i, s := range elems {
			out[i] = protocol.BulkStringValue(s)
		}
		return protocol.ArrayValue(out...)
	})
}
