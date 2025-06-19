package main

import (
	"encoding/json"
	"errors"
	"os"
	"time"
)

// define the config for each tenant
// only one type of cache can be used (LRU/LFU)
type Config struct {
	//App name
	Name string
	//AppId is the unique identifier for the tenant
	AppId uint64
	//capacity of the cache (bytes) for the tenant
	MaxMemory uint64
	//true if the cache should be LRU
	Lru bool
	//true if the cache should be LFU
	Lfu bool
	//set this to a high value if you do not want to have a 'practical' TTL
	MaxTTL time.Duration
}

// read the config file
func readConfig(path string) ([]Config, error) {
	//read the config file from the given path
	f, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}

	//unmarshal the config file into a slice of Config struct
	var configs []Config
	err = json.Unmarshal(f, &configs)
	if err != nil {
		return nil, err
	}

	//check the caching policy for each tenant
	for i := range configs {
		if configs[i].Lru && configs[i].Lfu {
			return nil, errors.New("only one caching policy can be set for a tenant")
		}
	}

	return configs, nil
}

// load the config for each tenant into the tenantCache map
func loadConfig(tenantCache map[uint64]*AppCache, configs []Config) {
	for _, config := range configs {
		//configure the tenant cache with the given config
		tenantCache[config.AppId] = &AppCache{
			Name:    config.Name,
			KeyMap:  make(map[string]uint64),
			TTLHeap: TTLHeap{TTLs: []TTL{}},
			MaxTTL:  config.MaxTTL,
		}
		tenantCache[config.AppId].TTLHeap.KeyIdx = make(map[uint64]int)

		//set the appropriate cache type based on the config
		if config.Lru {
			tenantCache[config.AppId].Lru = ConstructorLRU(config.MaxMemory)
		} else {
			tenantCache[config.AppId].Lfu = ConstructorLFU(config.MaxMemory)
		}
	}
}
