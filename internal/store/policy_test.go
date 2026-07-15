package store

import (
	"fmt"
	"testing"
	"time"
)

func newPolicyDB(pol EvictionPolicy, strat Strategy, max uint64, shards int) *DB {
	return New(Config{
		MaxMemory:  max,
		ShardCount: shards,
		Strategy:   strat,
		Policy:     pol,
	})
}

func TestParseEvictionPolicy(t *testing.T) {
	p, ok := ParseEvictionPolicy("allkeys-FIFO")
	if !ok || p != PolicyAllKeysFIFO {
		t.Fatalf("%v %v", p, ok)
	}
	if _, ok := ParseEvictionPolicy("nope"); ok {
		t.Fatal("expected false")
	}
}

// runAcrossStrategies runs fn for each sharding strategy.
func runAcrossStrategies(t *testing.T, name string, fn func(t *testing.T, strat Strategy)) {
	t.Helper()
	for _, strat := range []Strategy{StrategyGlobalTrack, StrategySteal, StrategyShardBudget} {
		strat := strat
		t.Run(fmt.Sprintf("%s/strategy%d", name, int(strat)), func(t *testing.T) {
			fn(t, strat)
		})
	}
}

func TestNoEvictionOOMs(t *testing.T) {
	runAcrossStrategies(t, "noeviction", func(t *testing.T, strat Strategy) {
		// 1 shard so per-shard budget == maxmemory (strategy 3).
		db := newPolicyDB(PolicyNoEviction, strat, 50, 1)
		defer db.Close()
		if _, err := db.Set("a", "x", SetOptions{}); err != nil {
			t.Fatal(err)
		}
		_, err := db.Set("b", "yyyyyyyyyyyyyyyyyyyy", SetOptions{})
		if err != ErrOOM {
			t.Fatalf("want OOM got %v", err)
		}
		if _, ok := db.Get("a"); !ok {
			t.Fatal("a should remain")
		}
	})
}

func TestAllKeysFIFOIgnoresGet(t *testing.T) {
	runAcrossStrategies(t, "fifo", func(t *testing.T, strat Strategy) {
		// 1 shard so insert order is deterministic for steal/budget too.
		db := newPolicyDB(PolicyAllKeysFIFO, strat, 80, 1)
		defer db.Close()
		_, _ = db.Set("first", "xxxxxxxxxx", SetOptions{})
		_, _ = db.Set("second", "xxxxxxxxxx", SetOptions{})
		_, _ = db.Get("first")
		if _, err := db.Set("third", "xxxxxxxxxx", SetOptions{}); err != nil {
			t.Fatal(err)
		}
		if _, ok := db.Get("first"); ok {
			t.Fatal("FIFO should evict oldest insert (first)")
		}
		if _, ok := db.Get("second"); !ok {
			t.Fatal("second should remain")
		}
	})
}

func TestAllKeysLFUEvictsLowFreq(t *testing.T) {
	runAcrossStrategies(t, "lfu", func(t *testing.T, strat Strategy) {
		db := newPolicyDB(PolicyAllKeysLFU, strat, 80, 1)
		defer db.Close()
		_, _ = db.Set("hot", "xxxxxxxxxx", SetOptions{})
		_, _ = db.Set("cold", "xxxxxxxxxx", SetOptions{})
		for i := 0; i < 20; i++ {
			_, _ = db.Get("hot")
		}
		if _, err := db.Set("other", "xxxxxxxxxx", SetOptions{}); err != nil {
			t.Fatal(err)
		}
		if _, ok := db.Get("hot"); !ok {
			t.Fatal("hot key should survive LFU")
		}
		_, _, _, _, _, ev := db.Stats()
		if ev == 0 {
			t.Fatal("expected eviction")
		}
	})
}

// Frequencies are unbounded: past the old uint8 cap, cold keys must still lose to hot keys.
func TestLFUUnboundedFreqStillCorrect(t *testing.T) {
	db := newPolicyDB(PolicyAllKeysLFU, StrategyGlobalTrack, 80, 1)
	defer db.Close()
	_, _ = db.Set("hot", "xxxxxxxxxx", SetOptions{})
	_, _ = db.Set("cold", "xxxxxxxxxx", SetOptions{})
	// 300 gets ⇒ freq well above 255 if counter were saturated/wrong.
	for i := 0; i < 300; i++ {
		_, _ = db.Get("hot")
	}
	// One more access on cold keeps it at low freq (initial + 1).
	_, _ = db.Get("cold")
	if _, err := db.Set("other", "xxxxxxxxxx", SetOptions{}); err != nil {
		t.Fatal(err)
	}
	if _, ok := db.Get("cold"); ok {
		t.Fatal("cold should be evicted under true LFU even after hot freq >> 255")
	}
	if _, ok := db.Get("hot"); !ok {
		t.Fatal("hot must survive")
	}
}

// Access frequency is tracked under any policy; switching to LFU uses true history.
func TestPolicySwitchLRUToLFUKeepsAccessFreq(t *testing.T) {
	db := newPolicyDB(PolicyAllKeysLRU, StrategyGlobalTrack, 80, 1)
	defer db.Close()

	_, _ = db.Set("hot", "xxxxxxxxxx", SetOptions{})
	_, _ = db.Set("cold", "xxxxxxxxxx", SetOptions{})
	for i := 0; i < 40; i++ {
		_, _ = db.Get("hot")
	}
	_, _ = db.Get("cold")

	db.SetEvictionPolicy(PolicyAllKeysLFU)

	if _, err := db.Set("other", "xxxxxxxxxx", SetOptions{}); err != nil {
		t.Fatal(err)
	}
	if _, ok := db.Get("cold"); ok {
		t.Fatal("after switch to LFU, cold should be evicted (fewer accesses while on LRU)")
	}
	if _, ok := db.Get("hot"); !ok {
		t.Fatal("hot should survive with higher access count accumulated under LRU")
	}
}

// Accesses while a key is non-volatile (not in the LFU index) must still count.
// After TTL is applied, it should re-join at its true frequency, not the pre-leave freq only.
func TestVolatileLFUTracksFreqWhileNotIndexed(t *testing.T) {
	db := newPolicyDB(PolicyVolatileLFU, StrategyGlobalTrack, 80, 1)
	defer db.Close()

	// hot: no TTL → not an eviction candidate, but GETs must bump e.freq.
	_, _ = db.Set("hot", "xxxxxxxxxx", SetOptions{})
	for i := 0; i < 50; i++ {
		_, _ = db.Get("hot")
	}
	// cold: has TTL, accessed once → low freq, is a candidate.
	_, _ = db.Set("cold", "xxxxxxxxxx", SetOptions{EX: 3600})
	_, _ = db.Get("cold")

	// Give hot a TTL → joins LFU at high true frequency (not stuck at 1).
	if db.Expire("hot", time.Hour) != 1 {
		t.Fatal("expire hot")
	}

	// Pressure: should evict cold (low freq), not hot (many prior accesses).
	if _, err := db.Set("other", "xxxxxxxxxx", SetOptions{EX: 3600}); err != nil {
		t.Fatal(err)
	}
	if _, ok := db.Get("cold"); ok {
		t.Fatal("cold should be evicted; hot had higher access freq while non-volatile")
	}
	if _, ok := db.Get("hot"); !ok {
		t.Fatal("hot should survive after re-joining at true frequency")
	}
}

func TestAllKeysRandomEvictsUnderPressure(t *testing.T) {
	runAcrossStrategies(t, "random", func(t *testing.T, strat Strategy) {
		// Entry ~34 bytes; keep shard budget >= entry (max/shards).
		db := newPolicyDB(PolicyAllKeysRandom, strat, 200, 2)
		defer db.Close()
		for i := 0; i < 40; i++ {
			_, _ = db.Set(fmt.Sprintf("k%d", i), "zzzzzzzz", SetOptions{})
		}
		used, max, _, _, _, ev := db.Stats()
		if used > max {
			t.Fatalf("used %d > max %d", used, max)
		}
		if ev == 0 {
			t.Fatal("expected random evictions")
		}
	})
}

func TestAllKeysLRUEvictsUnderPressure(t *testing.T) {
	runAcrossStrategies(t, "lru", func(t *testing.T, strat Strategy) {
		db := newPolicyDB(PolicyAllKeysLRU, strat, 200, 2)
		defer db.Close()
		for i := 0; i < 30; i++ {
			_, _ = db.Set(fmt.Sprintf("k%d", i), "xxxxxxxxxx", SetOptions{})
		}
		used, max, _, _, _, ev := db.Stats()
		if used > max {
			t.Fatalf("used %d > max %d", used, max)
		}
		if ev == 0 {
			t.Fatal("expected LRU evictions")
		}
	})
}

func TestVolatileLRUOnlyExpires(t *testing.T) {
	runAcrossStrategies(t, "volatile-lru", func(t *testing.T, strat Strategy) {
		db := newPolicyDB(PolicyVolatileLRU, strat, 80, 1)
		defer db.Close()
		_, _ = db.Set("perm", "xxxxxxxxxx", SetOptions{})
		_, _ = db.Set("tmp", "xxxxxxxxxx", SetOptions{PX: time.Hour})
		_, _ = db.Set("need", "xxxxxxxxxx", SetOptions{})
		if _, ok := db.Get("perm"); !ok {
			t.Fatal("permanent key must not be evicted by volatile-*")
		}
	})
}

func TestVolatileOnlyOOMsWhenNoVolatile(t *testing.T) {
	runAcrossStrategies(t, "volatile-oom", func(t *testing.T, strat Strategy) {
		db := newPolicyDB(PolicyVolatileRandom, strat, 50, 1)
		defer db.Close()
		_, err := db.Set("a", "xxxxxxxxxx", SetOptions{})
		if err != nil {
			t.Fatal(err)
		}
		_, err = db.Set("b", "yyyyyyyyyyyyyyyyyyyyyyyy", SetOptions{})
		if err != ErrOOM {
			_, err = db.Set("c", "zzzzzzzzzzzzzzzzzzzzzzzzzzzzzz", SetOptions{})
			if err != ErrOOM {
				t.Fatalf("want OOM when only non-volatile keys, got %v", err)
			}
		}
	})
}

func TestVolatileTTLPrefersSoonerExpiry(t *testing.T) {
	runAcrossStrategies(t, "volatile-ttl", func(t *testing.T, strat Strategy) {
		db := newPolicyDB(PolicyVolatileTTL, strat, 80, 1)
		defer db.Close()
		_, _ = db.Set("soon", "xxxxxxxxxx", SetOptions{PX: 50 * time.Millisecond})
		_, _ = db.Set("later", "xxxxxxxxxx", SetOptions{PX: time.Hour})
		if _, err := db.Set("x", "xxxxxxxxxx", SetOptions{}); err != nil {
			t.Fatal(err)
		}
		if _, ok := db.Get("soon"); ok {
			t.Fatal("expected soon-expiring key to be evicted first")
		}
	})
}

func TestFIFOVolatileExtension(t *testing.T) {
	runAcrossStrategies(t, "volatile-fifo", func(t *testing.T, strat Strategy) {
		db := newPolicyDB(PolicyVolatileFIFO, strat, 80, 1)
		defer db.Close()
		_, _ = db.Set("v1", "xxxxxxxxxx", SetOptions{EX: 60})
		_, _ = db.Set("v2", "xxxxxxxxxx", SetOptions{EX: 60})
		_, _ = db.Get("v1")
		if _, err := db.Set("v3", "xxxxxxxxxx", SetOptions{EX: 60}); err != nil {
			t.Fatal(err)
		}
		if _, ok := db.Get("v1"); ok {
			t.Fatal("volatile-fifo should evict oldest volatile insert (v1)")
		}
	})
}

func TestNeighborTenantsIndependent(t *testing.T) {
	hot := newPolicyDB(PolicyAllKeysLRU, StrategySteal, 60, 2)
	cold := newPolicyDB(PolicyAllKeysFIFO, StrategyShardBudget, 1<<20, 2)
	defer hot.Close()
	defer cold.Close()
	_, _ = cold.Set("keep", "v", SetOptions{})
	for i := 0; i < 50; i++ {
		_, _ = hot.Set(fmt.Sprintf("h%d", i), "xxxxxxxxxx", SetOptions{})
	}
	if _, ok := cold.Get("keep"); !ok {
		t.Fatal("cold tenant must be unaffected")
	}
	hu, hm, _, _, _, _ := hot.Stats()
	if hu > hm {
		t.Fatalf("hot used %d > max %d", hu, hm)
	}
}

func TestShardBudgetRespectsLocalCap(t *testing.T) {
	// Strategy 3: cannot steal; OOM if shard full under noeviction.
	// max=200, shards=2 => budget 100; entry ~34 fits, many SETs eventually OOM.
	db := newPolicyDB(PolicyNoEviction, StrategyShardBudget, 200, 2)
	defer db.Close()
	oom := 0
	for i := 0; i < 40; i++ {
		_, err := db.Set(fmt.Sprintf("b%d", i), "zzzzzzzz", SetOptions{})
		if err == ErrOOM {
			oom++
		}
	}
	if oom == 0 {
		t.Fatal("expected some OOM under shard budget + noeviction")
	}
	used, max, _, _, _, _ := db.Stats()
	if used > max {
		t.Fatalf("used %d > max %d", used, max)
	}
}

func TestStealEvictsAcrossShards(t *testing.T) {
	db := newPolicyDB(PolicyAllKeysFIFO, StrategySteal, 200, 4)
	defer db.Close()
	for i := 0; i < 50; i++ {
		_, _ = db.Set(fmt.Sprintf("s%d", i), "yyyyyyyy", SetOptions{})
	}
	used, max, _, _, _, ev := db.Stats()
	if used > max {
		t.Fatalf("used %d > max %d", used, max)
	}
	if ev == 0 {
		t.Fatal("expected steal-path evictions")
	}
}
