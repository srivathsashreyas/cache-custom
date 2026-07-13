package command

import (
	"testing"
	"time"

	"cache-custom/internal/protocol"
	"cache-custom/internal/store"
)

func regWithDB(t *testing.T) (*Registry, *store.DB) {
	t.Helper()
	db := store.New(store.Config{MaxMemory: 1 << 20, ShardCount: 4, Strategy: store.StrategyGlobalTrack})
	t.Cleanup(db.Close)
	r := NewRegistry()
	RegisterDefaults(r)
	RegisterStringCommands(r, db)
	return r, db
}

func TestStringCommandsGetSet(t *testing.T) {
	r, _ := regWithDB(t)
	v := r.Dispatch(&Context{}, []string{"SET", "k", "v"})
	if v.Type != protocol.SimpleString || v.Str != "OK" {
		t.Fatalf("%+v", v)
	}
	v = r.Dispatch(&Context{}, []string{"GET", "k"})
	if v.Type != protocol.BulkString || v.Str != "v" {
		t.Fatalf("%+v", v)
	}
	v = r.Dispatch(&Context{}, []string{"GET", "missing"})
	if v.Type != protocol.Null {
		t.Fatalf("%+v", v)
	}
}

func TestStringCommandsTTL(t *testing.T) {
	r, _ := regWithDB(t)
	r.Dispatch(&Context{}, []string{"SET", "t", "v", "PX", "200"})
	v := r.Dispatch(&Context{}, []string{"PTTL", "t"})
	if v.Type != protocol.Integer || v.Int <= 0 {
		t.Fatalf("%+v", v)
	}
	time.Sleep(250 * time.Millisecond)
	v = r.Dispatch(&Context{}, []string{"GET", "t"})
	if v.Type != protocol.Null {
		t.Fatalf("expected null after expire %+v", v)
	}
}

func TestStringCommandsIncr(t *testing.T) {
	r, _ := regWithDB(t)
	v := r.Dispatch(&Context{}, []string{"INCR", "n"})
	if v.Type != protocol.Integer || v.Int != 1 {
		t.Fatalf("%+v", v)
	}
	v = r.Dispatch(&Context{}, []string{"INCRBY", "n", "10"})
	if v.Int != 11 {
		t.Fatalf("%+v", v)
	}
}

func TestStringCommandsMSetMGet(t *testing.T) {
	r, _ := regWithDB(t)
	r.Dispatch(&Context{}, []string{"MSET", "a", "1", "b", "2"})
	v := r.Dispatch(&Context{}, []string{"MGET", "a", "b", "c"})
	if v.Type != protocol.Array || len(v.Array) != 3 {
		t.Fatalf("%+v", v)
	}
	if v.Array[0].Str != "1" || v.Array[1].Str != "2" || v.Array[2].Type != protocol.Null {
		t.Fatalf("%+v", v)
	}
}

func TestSetNX(t *testing.T) {
	r, _ := regWithDB(t)
	r.Dispatch(&Context{}, []string{"SET", "x", "1"})
	v := r.Dispatch(&Context{}, []string{"SET", "x", "2", "NX"})
	if v.Type != protocol.Null {
		t.Fatalf("%+v", v)
	}
}
