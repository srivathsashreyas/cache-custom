package main

import (
	"flag"
	"fmt"
	"log"
	"time"

	"cache-custom/internal/command"
	"cache-custom/internal/server"
	"cache-custom/internal/store"
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

	// M2: single default keyspace from the first tenant config (AUTH multi-tenant is M3).
	cfg0 := configs[0]
	db := store.New(storeConfigFrom(cfg0))
	defer db.Close()

	reg := command.NewRegistry()
	command.RegisterDefaults(reg)
	command.RegisterStringCommands(reg, db)

	srv := server.New(*addr, reg)
	fmt.Printf("Server is listening on %s (RESP2)\n", *addr)
	fmt.Printf("Default keyspace from tenant %q (AppId=%d); strategy=%d shards=%d maxmemory=%d\n",
		cfg0.Name, cfg0.AppId, cfg0.ShardingStrategy, cfg0.ShardCount, cfg0.MaxMemory)
	fmt.Printf("Loaded %d tenant config(s); AUTH multi-tenant binding is a later milestone\n", len(configs))
	if err := srv.ListenAndServe(); err != nil {
		log.Fatal(err)
	}
}

func storeConfigFrom(c Config) store.Config {
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
	return store.Config{
		MaxMemory:      c.MaxMemory,
		ShardCount:     sc,
		Strategy:       st,
		MaxTTL:         maxTTL,
		ExpiryInterval: time.Second,
	}
}
