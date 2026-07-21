package main

import (
	"flag"
	"fmt"
	"log"
	"time"

	"cache-custom/internal/command"
	"cache-custom/internal/persist"
	"cache-custom/internal/server"
	"cache-custom/internal/store"
	"cache-custom/internal/tenant"
)

func main() {
	addr := flag.String("addr", ":9001", "TCP listen address for RESP")
	configPath := flag.String("config", "config.json", "path to config file")
	flag.Parse()

	sc, err := readConfig(*configPath)
	if err != nil {
		log.Fatal("Error reading config file:", err)
	}

	tenants, err := tenant.NewRegistry(toTenantConfigs(sc.Tenants))
	if err != nil {
		log.Fatal(err)
	}
	defer tenants.Close()

	eng := persist.New(persistConfigFrom(sc.Persistence))
	if err := eng.Open(); err != nil {
		log.Fatal("persist open:", err)
	}
	defer eng.Close()

	if err := eng.Load(tenants); err != nil {
		log.Fatal("persist load:", err)
	}
	eng.AttachSinks(tenants)
	eng.StartPeriodicSnapshots(tenants)

	requireAuth := resolveRequireAuth(sc.Security)
	var defaultTenant *tenant.Tenant
	if !requireAuth {
		all := tenants.All()
		if len(all) > 0 {
			defaultTenant = all[0]
		}
	}
	pol := command.NewPolicy(requireAuth, defaultTenant, sc.Security.DenyCommands)

	reg := command.NewRegistry()
	reg.SetPolicy(pol)
	command.RegisterDefaults(reg, tenants)
	command.RegisterAuth(reg, tenants)
	command.RegisterStringCommands(reg)
	command.RegisterPubSub(reg)
	command.RegisterPersist(reg, eng, tenants)

	tlsCfg, err := loadTLSConfig(sc.Security)
	if err != nil {
		log.Fatal("tls:", err)
	}
	var idle time.Duration
	if sc.Security.IdleTimeoutSec > 0 {
		idle = time.Duration(sc.Security.IdleTimeoutSec) * time.Second
	}
	opts := server.Options{
		TLSConfig:   tlsCfg,
		MaxClients:  sc.Security.MaxClients,
		IdleTimeout: idle,
	}
	srv := server.NewWithOptions(*addr, reg, opts)

	profile := sc.Security.Profile
	if profile == "" {
		profile = "protected"
	}
	tlsOn := tlsCfg != nil
	fmt.Printf("Server is listening on %s (RESP2) tls=%v profile=%s require_auth=%v\n",
		*addr, tlsOn, profile, requireAuth)
	fmt.Printf("Persistence mode=%s dir=%s\n", eng.Mode(), sc.Persistence.Dir)
	if sc.Security.MaxClients > 0 {
		fmt.Printf("MaxClients global=%d\n", sc.Security.MaxClients)
	}
	if len(sc.Security.DenyCommands) > 0 {
		fmt.Printf("Denied commands: %v\n", sc.Security.DenyCommands)
	}
	fmt.Printf("Loaded %d tenant(s); AUTH <Name> <Password> for tenant bind\n", tenants.Len())
	for _, t := range tenants.All() {
		mc := "unlimited"
		if t.MaxClients > 0 {
			mc = fmt.Sprintf("%d", t.MaxClients)
		}
		fmt.Printf("  - %s appId=%d maxmemory=%d strategy=%d policy=%s shards=%d max_clients=%s keys≈%d\n",
			t.Name, t.AppID, t.MaxMemory, int(t.Strategy), t.Policy, t.Shards, mc, t.DB.DBSize())
	}
	if !requireAuth && defaultTenant != nil {
		fmt.Printf("Local profile: unauthenticated data commands bind to tenant %q\n", defaultTenant.Name)
	}
	if err := srv.ListenAndServe(); err != nil {
		log.Fatal(err)
	}
}

func toTenantConfigs(cfgs []TenantConfig) []tenant.Config {
	out := make([]tenant.Config, 0, len(cfgs))
	for _, c := range cfgs {
		sc := c.ShardCount
		if sc < 1 {
			sc = 4
		}
		st := store.Strategy(c.ShardingStrategy)
		if st < store.StrategyGlobalTrack || st > store.StrategyShardBudget {
			st = store.StrategyGlobalTrack
		}
		var maxTTL time.Duration
		if c.MaxTTL > 0 {
			maxTTL = time.Duration(c.MaxTTL) * time.Second
		}
		out = append(out, tenant.Config{
			Name:       c.Name,
			Password:   c.Password,
			AppID:      c.AppId,
			MaxMemory:  c.MaxMemory,
			MaxTTL:     maxTTL,
			ShardCount: sc,
			Strategy:   st,
			Policy:     evictionFromConfig(c),
			MaxClients: c.MaxClients,
			Disabled:   c.Disabled,
		})
	}
	return out
}
