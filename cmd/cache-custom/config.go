package main

import (
	"encoding/json"
	"errors"
	"os"
	"time"

	"cache-custom/internal/persist"
	"cache-custom/internal/store"
)

// TenantConfig is one tenant entry.
type TenantConfig struct {
	Name             string
	Password         string
	AppId            uint64
	MaxMemory        uint64
	EvictionPolicy   string
	MaxTTL           int64
	ShardCount       int
	ShardingStrategy int
	Disabled         bool
}

// PersistConfig is server-level durability settings.
type PersistConfig struct {
	Mode                 string // none | snapshot | aof | snapshot+aof
	Dir                  string
	AOFFsync             string // always | everysec | no
	SnapshotIntervalSec  int    // 0 = manual only
}

// ServerConfig is the on-disk config file (object form).
type ServerConfig struct {
	Persistence PersistConfig
	Tenants     []TenantConfig
}

// Config is an alias used by older call sites (tenant only).
type Config = TenantConfig

func readConfig(path string) (ServerConfig, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return ServerConfig{}, err
	}
	// Backward compatible: bare tenant array.
	var arr []TenantConfig
	if err := json.Unmarshal(raw, &arr); err == nil && len(arr) > 0 {
		return ServerConfig{Tenants: arr, Persistence: PersistConfig{Mode: "none"}}, validate(ServerConfig{Tenants: arr})
	}
	var sc ServerConfig
	if err := json.Unmarshal(raw, &sc); err != nil {
		return ServerConfig{}, err
	}
	return sc, validate(sc)
}

func validate(sc ServerConfig) error {
	if len(sc.Tenants) == 0 {
		return errors.New("at least one tenant required")
	}
	if sc.Persistence.Mode != "" {
		if _, ok := persist.ParseMode(sc.Persistence.Mode); !ok {
			return errors.New("invalid Persistence.Mode")
		}
	}
	for i := range sc.Tenants {
		c := &sc.Tenants[i]
		if c.Name == "" {
			return errors.New("each tenant requires Name")
		}
		if c.Password == "" {
			return errors.New("each tenant requires Password")
		}
		if c.EvictionPolicy != "" {
			if _, ok := store.ParseEvictionPolicy(c.EvictionPolicy); !ok {
				return errors.New("invalid EvictionPolicy for tenant " + c.Name)
			}
		}
		if c.ShardCount < 0 {
			return errors.New("ShardCount must be >= 0")
		}
		if c.ShardingStrategy != 0 && (c.ShardingStrategy < 1 || c.ShardingStrategy > 3) {
			return errors.New("ShardingStrategy must be 1, 2, or 3")
		}
	}
	return nil
}

func evictionFromConfig(c TenantConfig) store.EvictionPolicy {
	p, _ := store.ParseEvictionPolicy(c.EvictionPolicy)
	return p
}

func persistConfigFrom(pc PersistConfig) persist.Config {
	mode, _ := persist.ParseMode(pc.Mode)
	cfg := persist.Config{
		Mode:  mode,
		Dir:   pc.Dir,
		Fsync: persist.ParseFsync(pc.AOFFsync),
	}
	if pc.SnapshotIntervalSec > 0 {
		cfg.SnapshotInterval = time.Duration(pc.SnapshotIntervalSec) * time.Second
	}
	return cfg
}
