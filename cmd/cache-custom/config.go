package main

import (
	"encoding/json"
	"errors"
	"os"
)

// Config is one tenant entry in config.json.
type Config struct {
	// Name is the AUTH username.
	Name string
	// Password is the AUTH secret (required).
	Password string
	AppId    uint64
	// MaxMemory is the tenant memory budget in bytes.
	MaxMemory uint64
	Lru       bool
	Lfu       bool
	// MaxTTL is the ceiling for per-key TTL in seconds (0 = no ceiling).
	MaxTTL int64
	// ShardCount is the number of intra-node shards (default 4).
	ShardCount int
	// ShardingStrategy: 1=global eviction track, 2=steal across shards, 3=per-shard budget.
	ShardingStrategy int
	// Disabled rejects AUTH for this tenant when true.
	Disabled bool
}

func readConfig(path string) ([]Config, error) {
	f, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var configs []Config
	if err := json.Unmarshal(f, &configs); err != nil {
		return nil, err
	}
	for i := range configs {
		if configs[i].Name == "" {
			return nil, errors.New("each tenant requires Name")
		}
		if configs[i].Password == "" {
			return nil, errors.New("each tenant requires Password")
		}
		if configs[i].Lru && configs[i].Lfu {
			return nil, errors.New("only one caching policy can be set for a tenant")
		}
		if configs[i].ShardCount < 0 {
			return nil, errors.New("ShardCount must be >= 0")
		}
		if configs[i].ShardingStrategy != 0 &&
			(configs[i].ShardingStrategy < 1 || configs[i].ShardingStrategy > 3) {
			return nil, errors.New("ShardingStrategy must be 1, 2, or 3")
		}
	}
	return configs, nil
}
