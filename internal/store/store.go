// Package store implements a sharded in-memory string keyspace with TTL and eviction.
package store

import (
	"errors"
	"hash/fnv"
	"sync"
	"sync/atomic"
	"time"
)

// EntryOverhead is charged per key toward maxmemory (key+value bytes + this).
const EntryOverhead = 24

// Strategy is a per-tenant intra-node sharding / eviction mode.
// "Global" always means within one tenant, never across tenants.
type Strategy int

const (
	// StrategyGlobalTrack shards data but tracks eviction order globally (tenant-wide maxmemory).
	StrategyGlobalTrack Strategy = 1
	// StrategySteal shards data; eviction runs only when tenant maxmemory is hit.
	// Prefer victims in the target shard; if it is empty, steal from other shards.
	StrategySteal Strategy = 2
	// StrategyShardBudget gives each shard maxmemory/N; eviction is local to the shard only.
	StrategyShardBudget Strategy = 3
)

// ErrOOM is returned when a write cannot fit under the configured strategy.
var ErrOOM = errors.New("OOM command not allowed when used memory > 'maxmemory'")

// Config configures a DB instance (one logical tenant keyspace for M2).
type Config struct {
	MaxMemory      uint64
	ShardCount     int
	Strategy       Strategy
	MaxTTL         time.Duration // 0 = no ceiling on per-key TTL
	ExpiryInterval time.Duration // periodic sweep; default 1s
}

// DB is a concurrent string store.
//
// Concurrency model (no single meta lock on the hot path):
//   - each shard has its own mu (map + local LRU + shard used)
//   - tenant-wide used memory is atomic
//   - strategy 1 only: gMu protects the global LRU list (brief critical sections)
//   - expMu protects the expiry heap only
type DB struct {
	cfg    Config
	shards []*shard

	used atomic.Uint64

	// Strategy 1: tenant-global LRU order (not held across shard map ops longer than needed).
	gMu   sync.Mutex
	gHead *entry
	gTail *entry

	expMu  sync.Mutex
	expH   []expItem
	expIdx map[string]int

	stop chan struct{}
	wg   sync.WaitGroup

	hits      atomic.Uint64
	misses    atomic.Uint64
	evictions atomic.Uint64
}

type entry struct {
	key       string
	value     string
	expiresAt time.Time // zero => no expiry
	size      uint64

	// Separate DLL links for local (per-shard) and global (strategy 1) LRU lists.
	lPrev, lNext *entry
	gPrev, gNext *entry
}

type shard struct {
	mu     sync.Mutex
	data   map[string]*entry
	used   uint64
	budget uint64 // strategy 3
	head   *entry // local LRU head = least recently used
	tail   *entry
}

// New creates a DB and starts the periodic expiry worker.
func New(cfg Config) *DB {
	if cfg.ShardCount < 1 {
		cfg.ShardCount = 1
	}
	if cfg.Strategy < StrategyGlobalTrack || cfg.Strategy > StrategyShardBudget {
		cfg.Strategy = StrategyGlobalTrack
	}
	if cfg.ExpiryInterval <= 0 {
		cfg.ExpiryInterval = time.Second
	}

	db := &DB{
		cfg:    cfg,
		shards: make([]*shard, cfg.ShardCount),
		expIdx: make(map[string]int),
		stop:   make(chan struct{}),
	}

	base := cfg.MaxMemory / uint64(cfg.ShardCount)
	rem := cfg.MaxMemory % uint64(cfg.ShardCount)
	for i := 0; i < cfg.ShardCount; i++ {
		b := base
		if uint64(i) < rem {
			b++
		}
		db.shards[i] = &shard{
			data:   make(map[string]*entry),
			budget: b,
		}
	}

	db.wg.Add(1)
	go db.expiryLoop()
	return db
}

// Close stops the expiry worker.
func (db *DB) Close() {
	select {
	case <-db.stop:
		return
	default:
		close(db.stop)
	}
	db.wg.Wait()
}

func (db *DB) shardIndex(key string) int {
	h := fnv.New32a()
	_, _ = h.Write([]byte(key))
	return int(h.Sum32() % uint32(len(db.shards)))
}

func memSize(key, value string) uint64 {
	return uint64(len(key)) + uint64(len(value)) + EntryOverhead
}

// Stats returns used memory, max memory, keys, hits, misses, evictions.
func (db *DB) Stats() (used, max uint64, keys, hits, misses, evictions uint64) {
	used = db.used.Load()
	max = db.cfg.MaxMemory
	for _, sh := range db.shards {
		sh.mu.Lock()
		keys += uint64(len(sh.data))
		sh.mu.Unlock()
	}
	return used, max, keys, db.hits.Load(), db.misses.Load(), db.evictions.Load()
}

// DBSize returns live key count (skips expired entries it observes).
func (db *DB) DBSize() int64 {
	var n int64
	now := time.Now()
	for _, sh := range db.shards {
		sh.mu.Lock()
		for _, e := range sh.data {
			if !e.expired(now) {
				n++
			}
		}
		sh.mu.Unlock()
	}
	return n
}

func (e *entry) expired(now time.Time) bool {
	return !e.expiresAt.IsZero() && !e.expiresAt.After(now)
}

func (db *DB) addUsed(delta uint64) {
	db.used.Add(delta)
}

func (db *DB) subUsed(delta uint64) {
	for {
		cur := db.used.Load()
		var next uint64
		if cur >= delta {
			next = cur - delta
		}
		if db.used.CompareAndSwap(cur, next) {
			return
		}
	}
}
