package command

import (
	"testing"
	"time"

	"cache-custom/internal/protocol"
	"cache-custom/internal/store"
	"cache-custom/internal/tenant"
)

func testReg(t *testing.T) (*Registry, *Context) {
	t.Helper()
	db := store.New(store.Config{MaxMemory: 64 << 20, ShardCount: 4, Strategy: store.StrategyGlobalTrack})
	t.Cleanup(db.Close)
	ten := &tenant.Tenant{Name: "t1", DB: db}
	reg := NewRegistry()
	RegisterStringCommands(reg)
	RegisterHashCommands(reg)
	RegisterListCommands(reg)
	RegisterSetCommands(reg)
	RegisterZSetCommands(reg)
	ctx := &Context{Tenant: ten}
	return reg, ctx
}

func dispatch(reg *Registry, ctx *Context, args ...string) protocol.Value {
	return reg.Dispatch(ctx, args)
}

func TestHashBasics(t *testing.T) {
	reg, ctx := testReg(t)
	v := dispatch(reg, ctx, "HSET", "h", "a", "1", "b", "2")
	if v.Type != protocol.Integer || v.Int != 2 {
		t.Fatalf("HSET new fields: %+v", v)
	}
	v = dispatch(reg, ctx, "HSET", "h", "a", "9")
	if v.Type != protocol.Integer || v.Int != 0 {
		t.Fatalf("HSET update: %+v", v)
	}
	v = dispatch(reg, ctx, "HGET", "h", "a")
	if v.Type != protocol.BulkString || v.Str != "9" {
		t.Fatalf("HGET: %+v", v)
	}
	v = dispatch(reg, ctx, "HMGET", "h", "a", "missing", "b")
	if v.Type != protocol.Array || len(v.Array) != 3 {
		t.Fatalf("HMGET: %+v", v)
	}
	if v.Array[0].Str != "9" || v.Array[1].Type != protocol.Null || v.Array[2].Str != "2" {
		t.Fatalf("HMGET elems: %+v", v.Array)
	}
	v = dispatch(reg, ctx, "HEXISTS", "h", "b")
	if v.Int != 1 {
		t.Fatalf("HEXISTS: %+v", v)
	}
	v = dispatch(reg, ctx, "HINCRBY", "h", "a", "1")
	if v.Int != 10 {
		t.Fatalf("HINCRBY: %+v", v)
	}
	v = dispatch(reg, ctx, "HDEL", "h", "a", "b")
	if v.Int != 2 {
		t.Fatalf("HDEL: %+v", v)
	}
	v = dispatch(reg, ctx, "TYPE", "h")
	if v.Str != "none" {
		t.Fatalf("key should be gone: %+v", v)
	}
}

func TestListBasics(t *testing.T) {
	reg, ctx := testReg(t)
	v := dispatch(reg, ctx, "RPUSH", "l", "a", "b", "c")
	if v.Int != 3 {
		t.Fatalf("RPUSH: %+v", v)
	}
	v = dispatch(reg, ctx, "LPUSH", "l", "z")
	if v.Int != 4 {
		t.Fatalf("LPUSH: %+v", v)
	}
	v = dispatch(reg, ctx, "LRANGE", "l", "0", "-1")
	if len(v.Array) != 4 || v.Array[0].Str != "z" || v.Array[3].Str != "c" {
		t.Fatalf("LRANGE: %+v", v)
	}
	v = dispatch(reg, ctx, "LPOP", "l")
	if v.Str != "z" {
		t.Fatalf("LPOP: %+v", v)
	}
	v = dispatch(reg, ctx, "RPOP", "l")
	if v.Str != "c" {
		t.Fatalf("RPOP: %+v", v)
	}
	v = dispatch(reg, ctx, "LLEN", "l")
	if v.Int != 2 {
		t.Fatalf("LLEN: %+v", v)
	}
}

func TestSetBasics(t *testing.T) {
	reg, ctx := testReg(t)
	v := dispatch(reg, ctx, "SADD", "s", "a", "b", "a")
	if v.Int != 2 {
		t.Fatalf("SADD: %+v", v)
	}
	v = dispatch(reg, ctx, "SISMEMBER", "s", "a")
	if v.Int != 1 {
		t.Fatalf("SISMEMBER: %+v", v)
	}
	v = dispatch(reg, ctx, "SCARD", "s")
	if v.Int != 2 {
		t.Fatalf("SCARD: %+v", v)
	}
	dispatch(reg, ctx, "SADD", "s2", "b", "c")
	v = dispatch(reg, ctx, "SINTER", "s", "s2")
	if len(v.Array) != 1 || v.Array[0].Str != "b" {
		t.Fatalf("SINTER: %+v", v)
	}
	v = dispatch(reg, ctx, "SUNION", "s", "s2")
	if len(v.Array) != 3 {
		t.Fatalf("SUNION: %+v", v)
	}
	v = dispatch(reg, ctx, "SDIFF", "s", "s2")
	if len(v.Array) != 1 || v.Array[0].Str != "a" {
		t.Fatalf("SDIFF: %+v", v)
	}
	v = dispatch(reg, ctx, "SREM", "s", "a", "b")
	if v.Int != 2 {
		t.Fatalf("SREM: %+v", v)
	}
}

func TestZSetBasics(t *testing.T) {
	reg, ctx := testReg(t)
	v := dispatch(reg, ctx, "ZADD", "z", "1", "one", "2", "two", "3", "three")
	if v.Int != 3 {
		t.Fatalf("ZADD: %+v", v)
	}
	v = dispatch(reg, ctx, "ZADD", "z", "1.5", "one")
	if v.Int != 0 {
		t.Fatalf("ZADD update: %+v", v)
	}
	v = dispatch(reg, ctx, "ZSCORE", "z", "one")
	if v.Str != "1.5" {
		t.Fatalf("ZSCORE: %+v", v)
	}
	v = dispatch(reg, ctx, "ZRANK", "z", "two")
	if v.Int != 1 {
		t.Fatalf("ZRANK: %+v", v)
	}
	v = dispatch(reg, ctx, "ZRANGE", "z", "0", "-1", "WITHSCORES")
	if len(v.Array) != 6 {
		t.Fatalf("ZRANGE WITHSCORES: %+v", v)
	}
	v = dispatch(reg, ctx, "ZRANGEBYSCORE", "z", "1", "2")
	if len(v.Array) != 2 {
		t.Fatalf("ZRANGEBYSCORE: %+v", v)
	}
	v = dispatch(reg, ctx, "ZINCRBY", "z", "0.5", "two")
	if v.Str != "2.5" {
		t.Fatalf("ZINCRBY: %+v", v)
	}
	v = dispatch(reg, ctx, "ZREM", "z", "one")
	if v.Int != 1 {
		t.Fatalf("ZREM: %+v", v)
	}
}

func TestWrongType(t *testing.T) {
	reg, ctx := testReg(t)

	// Seed one key of each type.
	dispatch(reg, ctx, "SET", "str", "v")
	dispatch(reg, ctx, "HSET", "hash", "f", "1")
	dispatch(reg, ctx, "RPUSH", "list", "a")
	dispatch(reg, ctx, "SADD", "set", "m")
	dispatch(reg, ctx, "ZADD", "zset", "1", "m")

	// For each type-key, a command from every other type family must WRONGTYPE.
	// Also GET on non-string keys.
	cases := []struct {
		label string
		args  []string
	}{
		// string key
		{"HGET on string", []string{"HGET", "str", "f"}},
		{"LPUSH on string", []string{"LPUSH", "str", "x"}},
		{"SADD on string", []string{"SADD", "str", "x"}},
		{"ZADD on string", []string{"ZADD", "str", "1", "x"}},
		// hash key
		{"GET on hash", []string{"GET", "hash"}},
		{"LPOP on hash", []string{"LPOP", "hash"}},
		{"SADD on hash", []string{"SADD", "hash", "x"}},
		{"ZSCORE on hash", []string{"ZSCORE", "hash", "m"}},
		// list key
		{"GET on list", []string{"GET", "list"}},
		{"HGET on list", []string{"HGET", "list", "f"}},
		{"SCARD on list", []string{"SCARD", "list"}},
		{"ZRANK on list", []string{"ZRANK", "list", "m"}},
		// set key
		{"GET on set", []string{"GET", "set"}},
		{"HDEL on set", []string{"HDEL", "set", "f"}},
		{"LLEN on set", []string{"LLEN", "set"}},
		{"ZREM on set", []string{"ZREM", "set", "m"}},
		// zset key
		{"GET on zset", []string{"GET", "zset"}},
		{"HSET on zset", []string{"HSET", "zset", "f", "1"}},
		{"RPUSH on zset", []string{"RPUSH", "zset", "x"}},
		{"SREM on zset", []string{"SREM", "zset", "m"}},
	}
	for _, tc := range cases {
		v := dispatch(reg, ctx, tc.args...)
		if v.Type != protocol.Error || len(v.Str) < 9 || v.Str[:9] != "WRONGTYPE" {
			t.Fatalf("%s: expected WRONGTYPE, got %+v", tc.label, v)
		}
	}

	// TYPE labels
	for key, want := range map[string]string{
		"str": "string", "hash": "hash", "list": "list", "set": "set", "zset": "zset",
	} {
		v := dispatch(reg, ctx, "TYPE", key)
		if v.Str != want {
			t.Fatalf("TYPE %s: want %s got %+v", key, want, v)
		}
	}
}

func TestSInterWrongTypeAfterEmpty(t *testing.T) {
	// Intersection empties early, but a later non-set key must still WRONGTYPE.
	reg, ctx := testReg(t)
	dispatch(reg, ctx, "SADD", "a", "1")
	dispatch(reg, ctx, "SADD", "b", "2") // disjoint → empty inter after a∩b
	dispatch(reg, ctx, "SET", "c", "not-a-set")
	v := dispatch(reg, ctx, "SINTER", "a", "b", "c")
	if v.Type != protocol.Error || len(v.Str) < 9 || v.Str[:9] != "WRONGTYPE" {
		t.Fatalf("SINTER with wrongtype after empty inter: got %+v", v)
	}
}

func TestStructureTTL(t *testing.T) {
	reg, ctx := testReg(t)
	dispatch(reg, ctx, "HSET", "h", "a", "1")
	dispatch(reg, ctx, "EXPIRE", "h", "1")
	time.Sleep(1100 * time.Millisecond)
	v := dispatch(reg, ctx, "HGET", "h", "a")
	if v.Type != protocol.Null {
		t.Fatalf("expected expired hash null, got %+v", v)
	}
}

func TestStructurePersistenceRoundTrip(t *testing.T) {
	db := store.New(store.Config{MaxMemory: 1 << 20, ShardCount: 2, Strategy: store.StrategyGlobalTrack})
	defer db.Close()
	_, _ = db.HSet("h", [][2]string{{"f", "v"}})
	_, _ = db.RPush("l", []string{"a", "b"})
	_, _ = db.SAdd("s", []string{"x"})
	_, _ = db.ZAdd("z", []store.ZMember{{Member: "m", Score: 1}})
	recs := db.ExportAll()
	db2 := store.New(store.Config{MaxMemory: 1 << 20, ShardCount: 2, Strategy: store.StrategyGlobalTrack})
	defer db2.Close()
	if err := db2.LoadRecords(recs); err != nil {
		t.Fatal(err)
	}
	if db2.Type("h") != "hash" || db2.Type("l") != "list" || db2.Type("s") != "set" || db2.Type("z") != "zset" {
		t.Fatalf("types: h=%s l=%s s=%s z=%s", db2.Type("h"), db2.Type("l"), db2.Type("s"), db2.Type("z"))
	}
	v, ok, err := db2.HGet("h", "f")
	if err != nil || !ok || v != "v" {
		t.Fatalf("HGet restore: %v %v %v", v, ok, err)
	}
}
