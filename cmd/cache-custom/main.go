package main

import (
	"flag"
	"fmt"
	"log"
	"time"

	"cache-custom/internal/command"
	"cache-custom/internal/server"
	"cache-custom/internal/store"
	"cache-custom/internal/tenant"
)

func main() {
	addr := flag.String("addr", ":9001", "TCP listen address for RESP")
	configPath := flag.String("config", "config.json", "path to tenant config file")
	flag.Parse()

	configs, err := readConfig(*configPath)
	if err != nil {
		log.Fatal("Error reading config file:", err)
	}
	if len(configs) == 0 {
		log.Fatal("config must define at least one tenant")
	}

	tenants, err := tenant.NewRegistry(toTenantConfigs(configs))
	if err != nil {
		log.Fatal(err)
	}
	defer tenants.Close()

	reg := command.NewRegistry()
	command.RegisterDefaults(reg, tenants)
	command.RegisterAuth(reg, tenants)
	command.RegisterStringCommands(reg)

	srv := server.New(*addr, reg)
	fmt.Printf("Server is listening on %s (RESP2)\n", *addr)
	fmt.Printf("Loaded %d tenant(s); AUTH <Name> <Password> required for data commands\n", tenants.Len())
	for _, t := range tenants.All() {
		fmt.Printf("  - %s appId=%d maxmemory=%d strategy=%d policy=%s shards=%d\n",
			t.Name, t.AppID, t.MaxMemory, int(t.Strategy), t.Policy, t.Shards)
	}
	if err := srv.ListenAndServe(); err != nil {
		log.Fatal(err)
	}
}

func toTenantConfigs(cfgs []Config) []tenant.Config {
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
			Disabled:   c.Disabled,
		})
	}
	return out
}
