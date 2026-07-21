package command

import (
	"testing"

	"cache-custom/internal/protocol"
	"cache-custom/internal/store"
	"cache-custom/internal/tenant"
)

func testTenants(t *testing.T) *tenant.Registry {
	t.Helper()
	r, err := tenant.NewRegistry([]tenant.Config{
		{Name: "App1", Password: "p1", MaxMemory: 1 << 20, ShardCount: 2},
		{Name: "App2", Password: "p2", MaxMemory: 1 << 20, ShardCount: 2, MaxClients: 1},
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(r.Close)
	return r
}

func TestPolicyRequireAuthBlocksData(t *testing.T) {
	tenants := testTenants(t)
	reg := NewRegistry()
	reg.SetPolicy(NewPolicy(true, nil, nil))
	RegisterDefaults(reg, tenants)
	RegisterAuth(reg, tenants)
	RegisterStringCommands(reg)

	ctx := &Context{}
	v := reg.Dispatch(ctx, []string{"GET", "k"})
	if v.Type != protocol.Error || v.Str != "NOAUTH Authentication required." {
		t.Fatalf("expected NOAUTH, got %+v", v)
	}
	// Allowlist still works.
	v = reg.Dispatch(ctx, []string{"PING"})
	if v.Type != protocol.SimpleString || v.Str != "PONG" {
		t.Fatalf("PING: %+v", v)
	}
	v = reg.Dispatch(ctx, []string{"AUTH", "App1", "p1"})
	if v.Type != protocol.SimpleString || v.Str != "OK" {
		t.Fatalf("AUTH: %+v", v)
	}
	if !ctx.BoundTenant {
		t.Fatal("expected BoundTenant after AUTH")
	}
	v = reg.Dispatch(ctx, []string{"SET", "k", "v"})
	if v.Type != protocol.SimpleString || v.Str != "OK" {
		t.Fatalf("SET: %+v", v)
	}
	ReleaseTenantConn(ctx)
}

func TestPolicyLocalDefaultTenant(t *testing.T) {
	tenants := testTenants(t)
	def := tenants.All()[0]
	reg := NewRegistry()
	reg.SetPolicy(NewPolicy(false, def, nil))
	RegisterDefaults(reg, tenants)
	RegisterStringCommands(reg)

	ctx := &Context{}
	v := reg.Dispatch(ctx, []string{"SET", "localkey", "1"})
	if v.Type != protocol.SimpleString || v.Str != "OK" {
		t.Fatalf("SET without AUTH: %+v", v)
	}
	if ctx.Tenant != def {
		t.Fatal("expected auto-bind to default tenant")
	}
	if ctx.BoundTenant {
		t.Fatal("auto-bind should not consume MaxClients slot")
	}
}

func TestPolicyDenyCommands(t *testing.T) {
	tenants := testTenants(t)
	reg := NewRegistry()
	reg.SetPolicy(NewPolicy(true, nil, []string{"FLUSHDB", "ping"})) // ping cannot be denied
	RegisterDefaults(reg, tenants)
	RegisterAuth(reg, tenants)
	RegisterStringCommands(reg)
	// register a dummy flushdb
	reg.Register("FLUSHDB", func(ctx *Context, args []string) protocol.Value {
		return protocol.SimpleStringValue("OK")
	})

	ctx := &Context{}
	_ = reg.Dispatch(ctx, []string{"AUTH", "App1", "p1"})
	v := reg.Dispatch(ctx, []string{"FLUSHDB"})
	if v.Type != protocol.Error || v.Str == "" {
		t.Fatalf("expected deny error, got %+v", v)
	}
	// PING not deniable
	v = reg.Dispatch(ctx, []string{"PING"})
	if v.Type != protocol.SimpleString || v.Str != "PONG" {
		t.Fatalf("PING should work: %+v", v)
	}
	ReleaseTenantConn(ctx)
}

func TestTenantMaxClients(t *testing.T) {
	tenants := testTenants(t)
	reg := NewRegistry()
	reg.SetPolicy(NewPolicy(true, nil, nil))
	RegisterAuth(reg, tenants)

	ctx1 := &Context{}
	v := reg.Dispatch(ctx1, []string{"AUTH", "App2", "p2"})
	if v.Type != protocol.SimpleString {
		t.Fatalf("first AUTH: %+v", v)
	}
	ctx2 := &Context{}
	v = reg.Dispatch(ctx2, []string{"AUTH", "App2", "p2"})
	if v.Type != protocol.Error || v.Str != "ERR max number of clients reached for tenant" {
		t.Fatalf("second AUTH expected max clients, got %+v", v)
	}
	ReleaseTenantConn(ctx1)
	v = reg.Dispatch(ctx2, []string{"AUTH", "App2", "p2"})
	if v.Type != protocol.SimpleString {
		t.Fatalf("AUTH after release: %+v", v)
	}
	ReleaseTenantConn(ctx2)
}

func TestReauthSameTenant(t *testing.T) {
	tenants := testTenants(t)
	reg := NewRegistry()
	RegisterAuth(reg, tenants)
	ctx := &Context{}
	_ = reg.Dispatch(ctx, []string{"AUTH", "App2", "p2"})
	v := reg.Dispatch(ctx, []string{"AUTH", "App2", "p2"})
	if v.Type != protocol.SimpleString {
		t.Fatalf("re-auth: %+v", v)
	}
	if tenants.All()[1].ConnCount() != 1 {
		t.Fatalf("conn count want 1 got %d", tenants.All()[1].ConnCount())
	}
	ReleaseTenantConn(ctx)
}

func TestEvictionPolicyStillWorks(t *testing.T) {
	// sanity: store package still linked
	_ = store.PolicyAllKeysLRU
}
