package command

import (
	"strconv"
	"strings"

	"cache-custom/internal/protocol"
	"cache-custom/internal/store"
)

// RegisterZSetCommands wires M11 sorted-set commands.
func RegisterZSetCommands(r *Registry) {
	r.Register("ZADD", func(ctx *Context, args []string) protocol.Value {
		// ZADD key score member [score member ...]
		if len(args) < 4 || (len(args)-2)%2 != 0 {
			return protocol.ErrorValue("ERR wrong number of arguments for 'zadd' command")
		}
		ten, errv := requireDB(ctx)
		if ten == nil {
			return errv
		}
		pairs := make([]store.ZMember, 0, (len(args)-2)/2)
		for i := 2; i+1 < len(args); i += 2 {
			sc, err := strconv.ParseFloat(args[i], 64)
			if err != nil {
				return protocol.ErrorValue("ERR value is not a valid float")
			}
			pairs = append(pairs, store.ZMember{Score: sc, Member: args[i+1]})
		}
		n, err := ten.DB.ZAdd(args[1], pairs)
		if err != nil {
			return storeErr(err)
		}
		return protocol.IntegerValue(n)
	})

	r.Register("ZREM", func(ctx *Context, args []string) protocol.Value {
		if len(args) < 3 {
			return protocol.ErrorValue("ERR wrong number of arguments for 'zrem' command")
		}
		ten, errv := requireDB(ctx)
		if ten == nil {
			return errv
		}
		n, err := ten.DB.ZRem(args[1], args[2:])
		if err != nil {
			return storeErr(err)
		}
		return protocol.IntegerValue(n)
	})

	r.Register("ZSCORE", func(ctx *Context, args []string) protocol.Value {
		if len(args) != 3 {
			return protocol.ErrorValue("ERR wrong number of arguments for 'zscore' command")
		}
		ten, errv := requireDB(ctx)
		if ten == nil {
			return errv
		}
		sc, ok, err := ten.DB.ZScore(args[1], args[2])
		if err != nil {
			return storeErr(err)
		}
		if !ok {
			return protocol.NullValue()
		}
		return protocol.BulkStringValue(strconv.FormatFloat(sc, 'f', -1, 64))
	})

	r.Register("ZRANK", func(ctx *Context, args []string) protocol.Value {
		if len(args) != 3 {
			return protocol.ErrorValue("ERR wrong number of arguments for 'zrank' command")
		}
		ten, errv := requireDB(ctx)
		if ten == nil {
			return errv
		}
		rank, ok, err := ten.DB.ZRank(args[1], args[2])
		if err != nil {
			return storeErr(err)
		}
		if !ok {
			return protocol.NullValue()
		}
		return protocol.IntegerValue(rank)
	})

	r.Register("ZINCRBY", func(ctx *Context, args []string) protocol.Value {
		if len(args) != 4 {
			return protocol.ErrorValue("ERR wrong number of arguments for 'zincrby' command")
		}
		ten, errv := requireDB(ctx)
		if ten == nil {
			return errv
		}
		incr, err := strconv.ParseFloat(args[2], 64)
		if err != nil {
			return protocol.ErrorValue("ERR value is not a valid float")
		}
		n, err := ten.DB.ZIncrBy(args[1], incr, args[3])
		if err != nil {
			return storeErr(err)
		}
		return protocol.BulkStringValue(strconv.FormatFloat(n, 'f', -1, 64))
	})

	r.Register("ZRANGE", func(ctx *Context, args []string) protocol.Value {
		// ZRANGE key start stop [WITHSCORES]
		if len(args) < 4 || len(args) > 5 {
			return protocol.ErrorValue("ERR wrong number of arguments for 'zrange' command")
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
		withScores := false
		if len(args) == 5 {
			if !strings.EqualFold(args[4], "WITHSCORES") {
				return protocol.ErrorValue("ERR syntax error")
			}
			withScores = true
		}
		ms, err := ten.DB.ZRange(args[1], start, stop, withScores)
		if err != nil {
			return storeErr(err)
		}
		return zmembersToReply(ms, withScores)
	})

	r.Register("ZRANGEBYSCORE", func(ctx *Context, args []string) protocol.Value {
		// ZRANGEBYSCORE key min max [WITHSCORES] [LIMIT offset count]
		if len(args) < 4 {
			return protocol.ErrorValue("ERR wrong number of arguments for 'zrangebyscore' command")
		}
		ten, errv := requireDB(ctx)
		if ten == nil {
			return errv
		}
		withScores := false
		limitOffset, limitCount := int64(0), int64(-1)
		i := 4
		for i < len(args) {
			sw := strings.ToUpper(args[i])
			switch sw {
			case "WITHSCORES":
				withScores = true
				i++
			case "LIMIT":
				if i+2 >= len(args) {
					return protocol.ErrorValue("ERR syntax error")
				}
				off, err1 := strconv.ParseInt(args[i+1], 10, 64)
				cnt, err2 := strconv.ParseInt(args[i+2], 10, 64)
				if err1 != nil || err2 != nil {
					return protocol.ErrorValue("ERR value is not an integer or out of range")
				}
				limitOffset, limitCount = off, cnt
				i += 3
			default:
				return protocol.ErrorValue("ERR syntax error")
			}
		}
		ms, err := ten.DB.ZRangeByScore(args[1], args[2], args[3], withScores, limitOffset, limitCount)
		if err != nil {
			return storeErr(err)
		}
		return zmembersToReply(ms, withScores)
	})
}

func zmembersToReply(ms []store.ZMember, withScores bool) protocol.Value {
	if !withScores {
		out := make([]protocol.Value, len(ms))
		for i, m := range ms {
			out[i] = protocol.BulkStringValue(m.Member)
		}
		return protocol.ArrayValue(out...)
	}
	out := make([]protocol.Value, 0, len(ms)*2)
	for _, m := range ms {
		out = append(out,
			protocol.BulkStringValue(m.Member),
			protocol.BulkStringValue(strconv.FormatFloat(m.Score, 'f', -1, 64)),
		)
	}
	return protocol.ArrayValue(out...)
}
