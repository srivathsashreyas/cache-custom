package command

import (
	"strings"
	"testing"
	"time"

	"cache-custom/internal/protocol"
	"cache-custom/internal/store"
	"cache-custom/internal/tenant"
)

func regWithTenants(t *testing.T) (*Registry, *tenant.Registry) {
	t.Helper()
	tenants, err := tenant.NewRegistry([]tenant.Config{
		{Name: "App1", Password: "p1", MaxMemory: 1 << 20, Strategy: store.StrategyGlobalTrack, ShardCount: 2},
		{Name: "App2", Password: "p2", MaxMemory: 1 << 20, Strategy: store.StrategySteal, ShardCount: 2},
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(tenants.Close)
	r := NewRegistry()
	RegisterDefaults(r, tenants)
	RegisterAuth(r, tenants)
	RegisterStringCommands(r)
	return r, tenants
}

func auth(t *testing.T, r *Registry, user, pass string) *Context {
	t.Helper()
	ctx := &Context{}
	v := r.Dispatch(ctx, []string{"AUTH", user, pass})
	if v.Type != protocol.SimpleString || v.Str != "OK" {
		t.Fatalf("auth: %+v", v)
	}
	return ctx
}

func TestNoAuthRejected(t *testing.T) {
	r, _ := regWithTenants(t)
	v := r.Dispatch(&Context{}, []string{"GET", "k"})
	if v.Type != protocol.Error || v.Str != "NOAUTH Authentication required." {
		t.Fatalf("%+v", v)
	}
}

func TestStringCommandsGetSet(t *testing.T) {
	r, _ := regWithTenants(t)
	ctx := auth(t, r, "App1", "p1")
	v := r.Dispatch(ctx, []string{"SET", "k", "v"})
	if v.Type != protocol.SimpleString || v.Str != "OK" {
		t.Fatalf("%+v", v)
	}
	v = r.Dispatch(ctx, []string{"GET", "k"})
	if v.Type != protocol.BulkString || v.Str != "v" {
		t.Fatalf("%+v", v)
	}
	v = r.Dispatch(ctx, []string{"GET", "missing"})
	if v.Type != protocol.Null {
		t.Fatalf("%+v", v)
	}
}

func TestTenantIsolationViaCommands(t *testing.T) {
	r, _ := regWithTenants(t)
	a := auth(t, r, "App1", "p1")
	b := auth(t, r, "App2", "p2")
	r.Dispatch(a, []string{"SET", "shared", "from-a"})
	r.Dispatch(b, []string{"SET", "shared", "from-b"})
	va := r.Dispatch(a, []string{"GET", "shared"})
	vb := r.Dispatch(b, []string{"GET", "shared"})
	if va.Str != "from-a" || vb.Str != "from-b" {
		t.Fatalf("a=%+v b=%+v", va, vb)
	}
}

func TestAuthWrongPassword(t *testing.T) {
	r, _ := regWithTenants(t)
	v := r.Dispatch(&Context{}, []string{"AUTH", "App1", "nope"})
	if v.Type != protocol.Error {
		t.Fatalf("%+v", v)
	}
}

func TestStringCommandsTTL(t *testing.T) {
	r, _ := regWithTenants(t)
	ctx := auth(t, r, "App1", "p1")
	r.Dispatch(ctx, []string{"SET", "t", "v", "PX", "200"})
	v := r.Dispatch(ctx, []string{"PTTL", "t"})
	if v.Type != protocol.Integer || v.Int <= 0 {
		t.Fatalf("%+v", v)
	}
	time.Sleep(250 * time.Millisecond)
	v = r.Dispatch(ctx, []string{"GET", "t"})
	if v.Type != protocol.Null {
		t.Fatalf("expected null after expire %+v", v)
	}
}

func TestStringCommandsIncr(t *testing.T) {
	r, _ := regWithTenants(t)
	ctx := auth(t, r, "App1", "p1")
	v := r.Dispatch(ctx, []string{"INCR", "n"})
	if v.Type != protocol.Integer || v.Int != 1 {
		t.Fatalf("%+v", v)
	}
	v = r.Dispatch(ctx, []string{"INCRBY", "n", "10"})
	if v.Int != 11 {
		t.Fatalf("%+v", v)
	}
}

func TestStringCommandsMSetMGet(t *testing.T) {
	r, _ := regWithTenants(t)
	ctx := auth(t, r, "App1", "p1")
	r.Dispatch(ctx, []string{"MSET", "a", "1", "b", "2"})
	v := r.Dispatch(ctx, []string{"MGET", "a", "b", "c"})
	if v.Type != protocol.Array || len(v.Array) != 3 {
		t.Fatalf("%+v", v)
	}
	if v.Array[0].Str != "1" || v.Array[1].Str != "2" || v.Array[2].Type != protocol.Null {
		t.Fatalf("%+v", v)
	}
}

func TestSetNX(t *testing.T) {
	r, _ := regWithTenants(t)
	ctx := auth(t, r, "App1", "p1")
	r.Dispatch(ctx, []string{"SET", "x", "1"})
	v := r.Dispatch(ctx, []string{"SET", "x", "2", "NX"})
	if v.Type != protocol.Null {
		t.Fatalf("%+v", v)
	}
}

func TestInfoTenants(t *testing.T) {
	r, _ := regWithTenants(t)
	ctx := auth(t, r, "App1", "p1")
	r.Dispatch(ctx, []string{"SET", "k", "v"})
	v := r.Dispatch(ctx, []string{"INFO", "tenants"})
	if v.Type != protocol.BulkString {
		t.Fatalf("%+v", v)
	}
	if !containsAll(v.Str, "tenant_count:2", "tenant_App1_keys:", "current_tenant:App1") {
		t.Fatalf("body: %q", v.Str)
	}
}

func containsAll(s string, parts ...string) bool {
	for _, p := range parts {
		if !strings.Contains(s, p) {
			return false
		}
	}
	return true
}
