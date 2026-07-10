package main

import (
	"errors"
	"flag"
	"fmt"
	"log"
	"runtime"
	"sync"
	"time"

	"cache-custom/internal/command"
	"cache-custom/internal/server"
)

// defines the operations applicable to all cache types (in this case, LRU and LFU)
type Cache interface {
	Get(key uint64) string
	Delete(key uint64, ttlHeap *TTLHeap)
	Put(key uint64, value string, clientKey string, ttlHeap *TTLHeap, keyMap map[string]uint64) string
	Stats() (uint64, uint64, uint64)
}

type AppCache struct {
	//Name of the tenant, i.e, App Name
	Name string

	// map client key (string) to the key used in the cache (uint64)
	KeyMap map[string]uint64

	// lru cache
	// key - value retrieved from keyMap for a given client key
	// value - user input value for the given client key
	Lru *LRUCache

	// lfu cache
	// key - value retrieved from keyMap for a given client key
	// value - user input value for the given client key
	Lfu *LFUCache

	//mutex lock (for set/get operations)
	Mu sync.Mutex

	//TTL heap to keep track of the keys and their timestamps
	TTLHeap TTLHeap

	//Max TTL
	MaxTTL time.Duration
}

// global mutex lock to ensure that keyMap and freedKeys are accessed/modified in a thread-safe manner
var gmu sync.Mutex

// counter to keep track of the next key to be used in the cache
var keyCounter uint64 = 1

// stores list of freed cache keys (key counter) when a key is deleted so that they can be reused later
var freedKeys []uint64

// ticker to keep periodically checking the cache for expired keys
var ticker *time.Ticker

// map appId (tenantId) to AppCache
var tenantCache map[uint64]*AppCache

// baseline heap memory (at startup)
var baselineAlloc uint64

// retrieve the size of data (in bytes) to be inserted into the cache
func entrySize(clientKey string, value string) uint64 {
	//calculate the size of the entry in bytes
	//size of the entry = size of clientKey + size of value
	return uint64(len(clientKey)) + uint64(len(value))
}

// retrieve heap alloc information
func readMemAlloc() uint64 {
	//force garbage collection to collect memory that isn't referenced but not yet deallocated
	//to ultimately get accurate memory usage statistics
	runtime.GC()

	//read memory stats from the runtime
	var m runtime.MemStats
	runtime.ReadMemStats(&m)

	//return heap alloc information
	return m.Alloc
}

// retrieve the appropriate cache for a given tenant
func getCache(tenantId uint64) (Cache, error) {
	if _, ok := tenantCache[tenantId]; !ok {
		return nil, errors.New("tenant does not exist")
	}
	if tenantCache[tenantId].Lru != nil {
		return tenantCache[tenantId].Lru, nil
	}
	return tenantCache[tenantId].Lfu, nil
}

// periodically check and evict expired keys from the cache for all tenants
func ttlCheck() {
	for range ticker.C {
		for tenant := range tenantCache {
			//lock the next section for a given tenant
			tenantCache[tenant].Mu.Lock()
			//retrieve the min element from the ttl heap
			minTTL, heapLen := getMinTTL(&tenantCache[tenant].TTLHeap)

			//retrieve the cache (LRU/LFU per configuration) for the given tenant
			//this is easier than explicitly checking in each operation if the cache type is LRU or LFU
			tcache, err := getCache(tenant)
			if err != nil {
				log.Printf("Error retrieving cache for tenant when performing ttl check %d: %v\n", tenant, err)
				tenantCache[tenant].Mu.Unlock()
				continue
			}

			//pop all elements from the heap that have expired
			//by checking if the difference between the current time and the timestamp of the min element is greater than ttl seconds
			//make sure the second condition is configurable in future (currently set to 10 seconds)
			for heapLen > 0 && time.Since(minTTL.Timestamp) > tenantCache[tenant].MaxTTL*time.Second {
				//delete the key from the cache (the cache Delete operation also takes care of removing the min element from the heap)
				tcache.Delete(minTTL.Key, &tenantCache[tenant].TTLHeap)

				//delete the key from the keyMap
				delete(tenantCache[tenant].KeyMap, minTTL.ClientKey)

				//add the key to the freedKeys list (lock this section globally)
				gmu.Lock()
				freedKeys = append(freedKeys, minTTL.Key)
				gmu.Unlock()

				//retrieve the next min element from the heap
				minTTL, heapLen = getMinTTL(&tenantCache[tenant].TTLHeap)
			}
			tenantCache[tenant].Mu.Unlock()
		}
	}
}

func main() {
	addr := flag.String("addr", ":9001", "TCP listen address for RESP")
	configPath := flag.String("config", "config.json", "path to tenant config file")
	flag.Parse()

	//find heap alloc baseline
	baselineAlloc = readMemAlloc()

	//initialize the tenant cache (retained for upcoming data-plane milestones)
	tenantCache = make(map[uint64]*AppCache)

	//read config file and subsequently load the config into the tenantCache map
	configs, err := readConfig(*configPath)
	if err != nil {
		log.Fatal("Error reading config file:", err)
	}
	//load the config into the tenantCache map
	loadConfig(tenantCache, configs)

	//periodically check the cache for expired keys
	//make the ticker time configurable in the future
	ticker = time.NewTicker(3 * time.Second)
	go ttlCheck()

	// M1 wire surface: RESP2 connectivity commands only (GET/SET arrive later).
	reg := command.NewRegistry()
	command.RegisterDefaults(reg)

	srv := server.New(*addr, reg)
	fmt.Printf("Server is listening on %s (RESP2)\n", *addr)
	fmt.Printf("Loaded %d tenant config(s); data commands land in a later milestone\n", len(configs))
	// Blocks until the listener fails; graceful shutdown can wrap Close later.
	if err := srv.ListenAndServe(); err != nil {
		log.Fatal(err)
	}
}
