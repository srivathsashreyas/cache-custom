package command

import (
	"strconv"
	"strings"
	"time"

	"cache-custom/internal/protocol"
	"cache-custom/internal/store"
)

// RegisterStringCommands wires T0 string/key commands against the AUTH-bound tenant store.
func RegisterStringCommands(r *Registry) {
	r.Register("GET", func(ctx *Context, args []string) protocol.Value {
		if len(args) != 2 {
			return protocol.ErrorValue("ERR wrong number of arguments for 'get' command")
		}
		ten, errv := requireDB(ctx)
		if ten == nil {
			return errv
		}
		val, ok, err := ten.DB.GetString(args[1])
		if err != nil {
			return storeErr(err)
		}
		if !ok {
			return protocol.NullValue()
		}
		return protocol.BulkStringValue(val)
	})

	r.Register("SET", makeSetHandler())

	r.Register("DEL", func(ctx *Context, args []string) protocol.Value {
		if len(args) < 2 {
			return protocol.ErrorValue("ERR wrong number of arguments for 'del' command")
		}
		ten, errv := requireDB(ctx)
		if ten == nil {
			return errv
		}
		return protocol.IntegerValue(ten.DB.Del(args[1:]...))
	})

	r.Register("EXISTS", func(ctx *Context, args []string) protocol.Value {
		if len(args) < 2 {
			return protocol.ErrorValue("ERR wrong number of arguments for 'exists' command")
		}
		ten, errv := requireDB(ctx)
		if ten == nil {
			return errv
		}
		return protocol.IntegerValue(ten.DB.Exists(args[1:]...))
	})

	r.Register("MGET", func(ctx *Context, args []string) protocol.Value {
		if len(args) < 2 {
			return protocol.ErrorValue("ERR wrong number of arguments for 'mget' command")
		}
		ten, errv := requireDB(ctx)
		if ten == nil {
			return errv
		}
		out := make([]protocol.Value, 0, len(args)-1)
		for _, k := range args[1:] {
			v, ok, err := ten.DB.GetString(k)
			if err != nil {
				return storeErr(err)
			}
			if ok {
				out = append(out, protocol.BulkStringValue(v))
			} else {
				out = append(out, protocol.NullValue())
			}
		}
		return protocol.ArrayValue(out...)
	})

	r.Register("MSET", func(ctx *Context, args []string) protocol.Value {
		if len(args) < 3 || (len(args)-1)%2 != 0 {
			return protocol.ErrorValue("ERR wrong number of arguments for 'mset' command")
		}
		ten, errv := requireDB(ctx)
		if ten == nil {
			return errv
		}
		for i := 1; i < len(args); i += 2 {
			if _, err := ten.DB.Set(args[i], args[i+1], store.SetOptions{}); err != nil {
				return protocol.ErrorValue("OOM command not allowed when used memory > 'maxmemory'")
			}
		}
		return protocol.SimpleStringValue("OK")
	})

	r.Register("INCR", func(ctx *Context, args []string) protocol.Value {
		if len(args) != 2 {
			return protocol.ErrorValue("ERR wrong number of arguments for 'incr' command")
		}
		ten, errv := requireDB(ctx)
		if ten == nil {
			return errv
		}
		n, err := ten.DB.IncrBy(args[1], 1)
		if err != nil {
			return incrErr(err)
		}
		return protocol.IntegerValue(n)
	})
	r.Register("DECR", func(ctx *Context, args []string) protocol.Value {
		if len(args) != 2 {
			return protocol.ErrorValue("ERR wrong number of arguments for 'decr' command")
		}
		ten, errv := requireDB(ctx)
		if ten == nil {
			return errv
		}
		n, err := ten.DB.IncrBy(args[1], -1)
		if err != nil {
			return incrErr(err)
		}
		return protocol.IntegerValue(n)
	})
	r.Register("INCRBY", func(ctx *Context, args []string) protocol.Value {
		if len(args) != 3 {
			return protocol.ErrorValue("ERR wrong number of arguments for 'incrby' command")
		}
		ten, errv := requireDB(ctx)
		if ten == nil {
			return errv
		}
		d, err := strconv.ParseInt(args[2], 10, 64)
		if err != nil {
			return protocol.ErrorValue("ERR value is not an integer or out of range")
		}
		n, err := ten.DB.IncrBy(args[1], d)
		if err != nil {
			return incrErr(err)
		}
		return protocol.IntegerValue(n)
	})
	r.Register("DECRBY", func(ctx *Context, args []string) protocol.Value {
		if len(args) != 3 {
			return protocol.ErrorValue("ERR wrong number of arguments for 'decrby' command")
		}
		ten, errv := requireDB(ctx)
		if ten == nil {
			return errv
		}
		d, err := strconv.ParseInt(args[2], 10, 64)
		if err != nil {
			return protocol.ErrorValue("ERR value is not an integer or out of range")
		}
		n, err := ten.DB.IncrBy(args[1], -d)
		if err != nil {
			return incrErr(err)
		}
		return protocol.IntegerValue(n)
	})

	r.Register("EXPIRE", func(ctx *Context, args []string) protocol.Value {
		if len(args) != 3 {
			return protocol.ErrorValue("ERR wrong number of arguments for 'expire' command")
		}
		ten, errv := requireDB(ctx)
		if ten == nil {
			return errv
		}
		sec, err := strconv.ParseInt(args[2], 10, 64)
		if err != nil {
			return protocol.ErrorValue("ERR value is not an integer or out of range")
		}
		return protocol.IntegerValue(ten.DB.Expire(args[1], time.Duration(sec)*time.Second))
	})
	r.Register("PEXPIRE", func(ctx *Context, args []string) protocol.Value {
		if len(args) != 3 {
			return protocol.ErrorValue("ERR wrong number of arguments for 'pexpire' command")
		}
		ten, errv := requireDB(ctx)
		if ten == nil {
			return errv
		}
		ms, err := strconv.ParseInt(args[2], 10, 64)
		if err != nil {
			return protocol.ErrorValue("ERR value is not an integer or out of range")
		}
		return protocol.IntegerValue(ten.DB.Expire(args[1], time.Duration(ms)*time.Millisecond))
	})
	r.Register("TTL", func(ctx *Context, args []string) protocol.Value {
		if len(args) != 2 {
			return protocol.ErrorValue("ERR wrong number of arguments for 'ttl' command")
		}
		ten, errv := requireDB(ctx)
		if ten == nil {
			return errv
		}
		return protocol.IntegerValue(ten.DB.TTL(args[1]))
	})
	r.Register("PTTL", func(ctx *Context, args []string) protocol.Value {
		if len(args) != 2 {
			return protocol.ErrorValue("ERR wrong number of arguments for 'pttl' command")
		}
		ten, errv := requireDB(ctx)
		if ten == nil {
			return errv
		}
		return protocol.IntegerValue(ten.DB.PTTL(args[1]))
	})
	r.Register("PERSIST", func(ctx *Context, args []string) protocol.Value {
		if len(args) != 2 {
			return protocol.ErrorValue("ERR wrong number of arguments for 'persist' command")
		}
		ten, errv := requireDB(ctx)
		if ten == nil {
			return errv
		}
		return protocol.IntegerValue(ten.DB.Persist(args[1]))
	})
	r.Register("TYPE", func(ctx *Context, args []string) protocol.Value {
		if len(args) != 2 {
			return protocol.ErrorValue("ERR wrong number of arguments for 'type' command")
		}
		ten, errv := requireDB(ctx)
		if ten == nil {
			return errv
		}
		return protocol.SimpleStringValue(ten.DB.Type(args[1]))
	})
	r.Register("DBSIZE", func(ctx *Context, args []string) protocol.Value {
		if len(args) != 1 {
			return protocol.ErrorValue("ERR wrong number of arguments for 'dbsize' command")
		}
		ten, errv := requireDB(ctx)
		if ten == nil {
			return errv
		}
		return protocol.IntegerValue(ten.DB.DBSize())
	})
}

func incrErr(err error) protocol.Value {
	if err == store.ErrOOM {
		return protocol.ErrorValue("OOM command not allowed when used memory > 'maxmemory'")
	}
	return protocol.ErrorValue("ERR value is not an integer or out of range")
}

func makeSetHandler() Handler {
	return func(ctx *Context, args []string) protocol.Value {
		if len(args) < 3 {
			return protocol.ErrorValue("ERR wrong number of arguments for 'set' command")
		}
		ten, errv := requireDB(ctx)
		if ten == nil {
			return errv
		}
		key, val := args[1], args[2]
		opt := store.SetOptions{}
		for i := 3; i < len(args); i++ {
			sw := strings.ToUpper(args[i])
			switch sw {
			case "NX":
				opt.NX = true
			case "XX":
				opt.XX = true
			case "KEEPTTL":
				opt.KeepTTL = true
			case "EX":
				if i+1 >= len(args) {
					return protocol.ErrorValue("ERR syntax error")
				}
				sec, err := strconv.ParseInt(args[i+1], 10, 64)
				if err != nil || sec <= 0 {
					return protocol.ErrorValue("ERR invalid expire time in 'set' command")
				}
				opt.EX = time.Duration(sec) * time.Second
				i++
			case "PX":
				if i+1 >= len(args) {
					return protocol.ErrorValue("ERR syntax error")
				}
				ms, err := strconv.ParseInt(args[i+1], 10, 64)
				if err != nil || ms <= 0 {
					return protocol.ErrorValue("ERR invalid expire time in 'set' command")
				}
				opt.PX = time.Duration(ms) * time.Millisecond
				i++
			default:
				return protocol.ErrorValue("ERR syntax error")
			}
		}
		if opt.NX && opt.XX {
			return protocol.ErrorValue("ERR syntax error")
		}
		ok, err := ten.DB.Set(key, val, opt)
		if err != nil {
			return protocol.ErrorValue("OOM command not allowed when used memory > 'maxmemory'")
		}
		if !ok {
			return protocol.NullValue()
		}
		return protocol.SimpleStringValue("OK")
	}
}
