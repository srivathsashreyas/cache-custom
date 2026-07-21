package main

import (
	"os"
	"path/filepath"
	"testing"
)

func TestReadConfigBareArrayRequiresAuth(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "c.json")
	raw := `[{"Name":"A","Password":"p","AppId":1,"MaxMemory":1000}]`
	if err := os.WriteFile(path, []byte(raw), 0o644); err != nil {
		t.Fatal(err)
	}
	sc, err := readConfig(path)
	if err != nil {
		t.Fatal(err)
	}
	if sc.Security.Profile != "local" {
		t.Fatalf("profile %q", sc.Security.Profile)
	}
	if !resolveRequireAuth(sc.Security) {
		t.Fatal("bare array should still require AUTH")
	}
}

func TestReadConfigProtectedDefault(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "c.json")
	raw := `{
	  "Tenants":[{"Name":"A","Password":"p","AppId":1,"MaxMemory":1000}],
	  "Persistence":{"Mode":"none"}
	}`
	if err := os.WriteFile(path, []byte(raw), 0o644); err != nil {
		t.Fatal(err)
	}
	sc, err := readConfig(path)
	if err != nil {
		t.Fatal(err)
	}
	if sc.Security.Profile != "protected" {
		t.Fatalf("profile %q", sc.Security.Profile)
	}
	if !resolveRequireAuth(sc.Security) {
		t.Fatal("protected should require AUTH")
	}
}

func TestReadConfigLocalOpen(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "c.json")
	raw := `{
	  "Security":{"Profile":"local","MaxClients":10,"DenyCommands":["FLUSHDB"]},
	  "Tenants":[{"Name":"A","Password":"p","AppId":1,"MaxMemory":1000,"MaxClients":2}]
	}`
	if err := os.WriteFile(path, []byte(raw), 0o644); err != nil {
		t.Fatal(err)
	}
	sc, err := readConfig(path)
	if err != nil {
		t.Fatal(err)
	}
	if resolveRequireAuth(sc.Security) {
		t.Fatal("local profile should not require AUTH by default")
	}
	if sc.Security.MaxClients != 10 {
		t.Fatal(sc.Security.MaxClients)
	}
	if sc.Tenants[0].MaxClients != 2 {
		t.Fatal(sc.Tenants[0].MaxClients)
	}
}

func TestTLSPairValidation(t *testing.T) {
	sc := ServerConfig{
		Security: SecurityConfig{TLSCertFile: "a.pem"},
		Tenants:  []TenantConfig{{Name: "A", Password: "p"}},
	}
	if err := validate(sc); err == nil {
		t.Fatal("expected cert/key pair error")
	}
}

func TestRequireAuthOverride(t *testing.T) {
	f := false
	sec := SecurityConfig{Profile: "protected", RequireAuth: &f}
	if resolveRequireAuth(sec) {
		t.Fatal("explicit false should win")
	}
	tr := true
	sec = SecurityConfig{Profile: "local", RequireAuth: &tr}
	if !resolveRequireAuth(sec) {
		t.Fatal("explicit true should win")
	}
}

func TestCapTenantMaxClients(t *testing.T) {
	sc := ServerConfig{
		Security: SecurityConfig{MaxClients: 10},
		Tenants: []TenantConfig{
			{Name: "A", Password: "p", MaxClients: 50},
			{Name: "B", Password: "p", MaxClients: 5},
			{Name: "C", Password: "p", MaxClients: 0}, // unlimited at tenant layer
			{Name: "D", Password: "p", MaxClients: 10},
		},
	}
	capTenantMaxClients(&sc)
	if sc.Tenants[0].MaxClients != 10 {
		t.Fatalf("A: want capped 10, got %d", sc.Tenants[0].MaxClients)
	}
	if sc.Tenants[1].MaxClients != 5 {
		t.Fatalf("B: want unchanged 5, got %d", sc.Tenants[1].MaxClients)
	}
	if sc.Tenants[2].MaxClients != 0 {
		t.Fatalf("C: unlimited should stay 0, got %d", sc.Tenants[2].MaxClients)
	}
	if sc.Tenants[3].MaxClients != 10 {
		t.Fatalf("D: want 10, got %d", sc.Tenants[3].MaxClients)
	}
}

func TestCapTenantMaxClientsNoGlobal(t *testing.T) {
	sc := ServerConfig{
		Security: SecurityConfig{MaxClients: 0},
		Tenants:  []TenantConfig{{Name: "A", Password: "p", MaxClients: 50}},
	}
	capTenantMaxClients(&sc)
	if sc.Tenants[0].MaxClients != 50 {
		t.Fatalf("no global limit: tenant should stay 50, got %d", sc.Tenants[0].MaxClients)
	}
}

func TestReadConfigCapsTenantMaxClients(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "c.json")
	raw := `{
	  "Security":{"Profile":"protected","MaxClients":3},
	  "Tenants":[{"Name":"A","Password":"p","AppId":1,"MaxMemory":1000,"MaxClients":99}]
	}`
	if err := os.WriteFile(path, []byte(raw), 0o644); err != nil {
		t.Fatal(err)
	}
	sc, err := readConfig(path)
	if err != nil {
		t.Fatal(err)
	}
	if sc.Tenants[0].MaxClients != 3 {
		t.Fatalf("want capped 3, got %d", sc.Tenants[0].MaxClients)
	}
}
