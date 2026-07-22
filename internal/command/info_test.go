package command

import (
	"strings"
	"testing"
	"time"

	"cache-custom/internal/metrics"
	"cache-custom/internal/protocol"
	"cache-custom/internal/tenant"
)

func TestINFOSections(t *testing.T) {
	tenants := testTenants(t)
	coll := metrics.New()
	coll.ObserveCommand("App1", "GET", time.Millisecond, false)

	reg := NewRegistry()
	reg.SetRuntime(&RuntimeInfo{
		Tenants:          tenants,
		PersistMode:      "snapshot",
		PersistDir:       "data-snapshot",
		AOFFsync:         "everysec",
		LastSaveUnix:     func() int64 { return 1700000000 },
		ConnectedClients: func() int { return 3 },
		MaxClients:       100,
		Addr:             ":9001",
		StartTime:        time.Now().Add(-time.Minute),
		TLSEnabled:       false,
		Profile:          "protected",
		RequireAuth:      true,
		Metrics:          coll,
	})
	RegisterDefaults(reg, tenants)

	ctx := &Context{}
	for _, section := range []string{"server", "clients", "memory", "stats", "persistence", "tenants", "all"} {
		v := reg.Dispatch(ctx, []string{"INFO", section})
		if v.Type != protocol.BulkString {
			t.Fatalf("%s: type %+v", section, v)
		}
		if section != "all" && !strings.Contains(strings.ToLower(v.Str), "# "+section[:1]) {
			// section headers are # Server etc — check more carefully below
		}
		if v.Str == "" && section != "" {
			// empty only for unknown
		}
	}

	v := reg.Dispatch(ctx, []string{"INFO", "server"})
	if !strings.Contains(v.Str, "# Server") || !strings.Contains(v.Str, "require_auth:true") {
		t.Fatalf("server section: %q", v.Str)
	}
	v = reg.Dispatch(ctx, []string{"INFO", "clients"})
	if !strings.Contains(v.Str, "connected_clients:3") || !strings.Contains(v.Str, "maxclients:100") {
		t.Fatalf("clients: %q", v.Str)
	}
	v = reg.Dispatch(ctx, []string{"INFO", "persistence"})
	if !strings.Contains(v.Str, "persistence_mode:snapshot") || !strings.Contains(v.Str, "rdb_last_save_time:1700000000") {
		t.Fatalf("persistence: %q", v.Str)
	}
	v = reg.Dispatch(ctx, []string{"INFO", "stats"})
	if !strings.Contains(v.Str, "total_commands_processed:") {
		t.Fatalf("stats: %q", v.Str)
	}
	v = reg.Dispatch(ctx, []string{"INFO", "tenants"})
	if !strings.Contains(v.Str, "tenant_App1_") || !strings.Contains(v.Str, "tenant_count:2") {
		t.Fatalf("tenants: %q", v.Str)
	}
	v = reg.Dispatch(ctx, []string{"INFO"})
	for _, h := range []string{"# Server", "# Clients", "# Memory", "# Stats", "# Persistence", "# Tenants"} {
		if !strings.Contains(v.Str, h) {
			t.Fatalf("default INFO missing %s\n%s", h, v.Str)
		}
	}
}

func TestTENANTSTATS(t *testing.T) {
	tenants := testTenants(t)
	coll := metrics.New()
	reg := NewRegistry()
	RegisterDefaults(reg, tenants)
	RegisterAuth(reg, tenants)
	RegisterTenantStats(reg, tenants, coll)

	// unauthenticated: all tenants
	ctx := &Context{}
	v := reg.Dispatch(ctx, []string{"TENANTSTATS"})
	if v.Type != protocol.Array || len(v.Array) != 2 {
		t.Fatalf("want 2 tenants, got %+v", v)
	}

	// authenticated: only bound tenant
	_ = reg.Dispatch(ctx, []string{"AUTH", "App1", "p1"})
	v = reg.Dispatch(ctx, []string{"TENANTSTATS"})
	if v.Type != protocol.Array || len(v.Array) != 1 {
		t.Fatalf("want 1 tenant after AUTH, got %+v", v)
	}
	ReleaseTenantConn(ctx)
}

// ensure testTenants exists (policy_test.go)
var _ = tenant.StatusActive
