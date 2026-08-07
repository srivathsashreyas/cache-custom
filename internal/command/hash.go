package command

import (
	"strconv"

	"cache-custom/internal/protocol"
)

// RegisterHashCommands wires M11 hash commands.
func RegisterHashCommands(r *Registry) {
	r.Register("HSET", func(ctx *Context, args []string) protocol.Value {
		// HSET key field value [field value ...] → even number of field/value tokens after key.
		if len(args) < 4 || (len(args)-2)%2 != 0 {
			return protocol.ErrorValue("ERR wrong number of arguments for 'hset' command")
		}
		ten, errv := requireDB(ctx)
		if ten == nil {
			return errv
		}
		// HSET key field value [field value ...]
		pairs := make([][2]string, 0, (len(args)-2)/2)
		for i := 2; i+1 < len(args); i += 2 {
			pairs = append(pairs, [2]string{args[i], args[i+1]})
		}
		n, err := ten.DB.HSet(args[1], pairs)
		if err != nil {
			return storeErr(err)
		}
		return protocol.IntegerValue(n)
	})

	r.Register("HGET", func(ctx *Context, args []string) protocol.Value {
		if len(args) != 3 {
			return protocol.ErrorValue("ERR wrong number of arguments for 'hget' command")
		}
		ten, errv := requireDB(ctx)
		if ten == nil {
			return errv
		}
		v, ok, err := ten.DB.HGet(args[1], args[2])
		if err != nil {
			return storeErr(err)
		}
		if !ok {
			return protocol.NullValue()
		}
		return protocol.BulkStringValue(v)
	})

	r.Register("HMGET", func(ctx *Context, args []string) protocol.Value {
		if len(args) < 3 {
			return protocol.ErrorValue("ERR wrong number of arguments for 'hmget' command")
		}
		ten, errv := requireDB(ctx)
		if ten == nil {
			return errv
		}
		vals, oks, err := ten.DB.HMGet(args[1], args[2:])
		if err != nil {
			return storeErr(err)
		}
		out := make([]protocol.Value, len(vals))
		for i := range vals {
			if oks[i] {
				out[i] = protocol.BulkStringValue(vals[i])
			} else {
				out[i] = protocol.NullValue()
			}
		}
		return protocol.ArrayValue(out...)
	})

	r.Register("HDEL", func(ctx *Context, args []string) protocol.Value {
		if len(args) < 3 {
			return protocol.ErrorValue("ERR wrong number of arguments for 'hdel' command")
		}
		ten, errv := requireDB(ctx)
		if ten == nil {
			return errv
		}
		n, err := ten.DB.HDel(args[1], args[2:])
		if err != nil {
			return storeErr(err)
		}
		return protocol.IntegerValue(n)
	})

	r.Register("HGETALL", func(ctx *Context, args []string) protocol.Value {
		if len(args) != 2 {
			return protocol.ErrorValue("ERR wrong number of arguments for 'hgetall' command")
		}
		ten, errv := requireDB(ctx)
		if ten == nil {
			return errv
		}
		flat, err := ten.DB.HGetAll(args[1])
		if err != nil {
			return storeErr(err)
		}
		out := make([]protocol.Value, len(flat))
		for i, s := range flat {
			out[i] = protocol.BulkStringValue(s)
		}
		return protocol.ArrayValue(out...)
	})

	r.Register("HEXISTS", func(ctx *Context, args []string) protocol.Value {
		if len(args) != 3 {
			return protocol.ErrorValue("ERR wrong number of arguments for 'hexists' command")
		}
		ten, errv := requireDB(ctx)
		if ten == nil {
			return errv
		}
		n, err := ten.DB.HExists(args[1], args[2])
		if err != nil {
			return storeErr(err)
		}
		return protocol.IntegerValue(n)
	})

	r.Register("HINCRBY", func(ctx *Context, args []string) protocol.Value {
		if len(args) != 4 {
			return protocol.ErrorValue("ERR wrong number of arguments for 'hincrby' command")
		}
		ten, errv := requireDB(ctx)
		if ten == nil {
			return errv
		}
		d, err := strconv.ParseInt(args[3], 10, 64)
		if err != nil {
			return protocol.ErrorValue("ERR value is not an integer or out of range")
		}
		n, err := ten.DB.HIncrBy(args[1], args[2], d)
		if err != nil {
			return storeErr(err)
		}
		return protocol.IntegerValue(n)
	})
}
