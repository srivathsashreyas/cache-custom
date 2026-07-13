package tenant

import (
	"testing"
	"time"

	"cache-custom/internal/store"
)

func sampleConfigs() []Config {
	return []Config{
		{Name: "App1", Password: "p1", AppID: 1, MaxMemory: 1 << 20, Strategy: store.StrategyGlobalTrack, ShardCount: 2},
		{Name: "App2", Password: "p2", AppID: 2, MaxMemory: 4096, Strategy: store.StrategyShardBudget, ShardCount: 2, MaxTTL: time.Hour},
	}
}

func TestAuthenticate(t *testing.T) {
	r, err := NewRegistry(sampleConfigs())
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()

	ten, err := r.Authenticate("App1", "p1")
	if err != nil || ten.Name != "App1" {
		t.Fatalf("%v %+v", err, ten)
	}
	if _, err := r.Authenticate("App1", "wrong"); err == nil {
		t.Fatal("expected wrong pass")
	}
	if _, err := r.Authenticate("nope", "p1"); err == nil {
		t.Fatal("expected missing user")
	}
}

func TestIsolation(t *testing.T) {
	r, err := NewRegistry(sampleConfigs())
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()

	a, _ := r.Authenticate("App1", "p1")
	b, _ := r.Authenticate("App2", "p2")
	if _, err := a.DB.Set("k", "from-a", store.SetOptions{}); err != nil {
		t.Fatal(err)
	}
	if _, err := b.DB.Set("k", "from-b", store.SetOptions{}); err != nil {
		t.Fatal(err)
	}
	va, ok := a.DB.Get("k")
	if !ok || va != "from-a" {
		t.Fatalf("a: %v %v", va, ok)
	}
	vb, ok := b.DB.Get("k")
	if !ok || vb != "from-b" {
		t.Fatalf("b: %v %v", vb, ok)
	}
}

func TestDisabledTenant(t *testing.T) {
	cfgs := sampleConfigs()
	cfgs[0].Disabled = true
	r, err := NewRegistry(cfgs)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	if _, err := r.Authenticate("App1", "p1"); err == nil {
		t.Fatal("disabled should fail AUTH")
	}
}

func TestDuplicateName(t *testing.T) {
	_, err := NewRegistry([]Config{
		{Name: "x", Password: "a", MaxMemory: 100},
		{Name: "x", Password: "b", MaxMemory: 100},
	})
	if err == nil {
		t.Fatal("expected duplicate error")
	}
}
