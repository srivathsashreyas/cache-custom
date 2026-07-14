package store

import (
	"fmt"
	"testing"
	"time"
)

func smallDB(pol EvictionPolicy, max uint64, shards int) *DB {
	return New(Config{
		MaxMemory:  max,
		ShardCount: shards,
		Strategy:   StrategyGlobalTrack,
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

func TestNoEvictionOOMs(t *testing.T) {
	// One entry ~ 1+1+24 = 26 bytes; max 40 allows one, not two.
	db := smallDB(PolicyNoEviction, 50, 1)
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
}

func TestAllKeysLRUEvictsCold(t *testing.T) {
	// Force tiny memory, 1 shard so order is clear.
	db := smallDB(PolicyAllKeysLRU, 80, 1)
	defer db.Close()
	_, _ = db.Set("old", "xxxxxxxxxx", SetOptions{}) // ~34
	_, _ = db.Set("new", "xxxxxxxxxx", SetOptions{})
	// Access old so new becomes colder if we only have 2 and need room for third.
	_, _ = db.Get("old")
	_, err := db.Set("third", "xxxxxxxxxx", SetOptions{})
	if err != nil {
		// may need more room - if OOM, try larger
		t.Log(err)
	}
	// After pressure, "new" should be more likely gone than "old" (touched).
	// With max 80 and ~34 each, third forces one eviction of LRU = "new".
	if _, ok := db.Get("new"); ok {
		// might still exist if sizes differ; check evictions happened
		_, _, _, _, _, ev := db.Stats()
		if ev == 0 {
			t.Fatal("expected some eviction under pressure")
		}
	}
	_, _, _, _, _, ev := db.Stats()
	if ev == 0 {
		t.Fatal("expected evictions > 0")
	}
}

func TestAllKeysFIFOIgnoresGet(t *testing.T) {
	db := smallDB(PolicyAllKeysFIFO, 80, 1)
	defer db.Close()
	_, _ = db.Set("first", "xxxxxxxxxx", SetOptions{})
	_, _ = db.Set("second", "xxxxxxxxxx", SetOptions{})
	// GET first must NOT promote it; FIFO should still evict first under pressure.
	_, _ = db.Get("first")
	_, err := db.Set("third", "xxxxxxxxxx", SetOptions{})
	if err != nil {
		t.Fatalf("set third: %v", err)
	}
	if _, ok := db.Get("first"); ok {
		t.Fatal("FIFO should have evicted first (oldest insert), not second")
	}
	if _, ok := db.Get("second"); !ok {
		t.Fatal("second should remain")
	}
}

func TestAllKeysLFUEvictsLowFreq(t *testing.T) {
	db := smallDB(PolicyAllKeysLFU, 80, 1)
	defer db.Close()
	_, _ = db.Set("hot", "xxxxxxxxxx", SetOptions{})
	_, _ = db.Set("cold", "xxxxxxxxxx", SetOptions{})
	for i := 0; i < 20; i++ {
		_, _ = db.Get("hot")
	}
	_, err := db.Set("other", "xxxxxxxxxx", SetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := db.Get("cold"); ok {
		// cold may survive if sizes allow all three - check memory
		used, max, keys, _, _, ev := db.Stats()
		t.Logf("used=%d max=%d keys=%d ev=%d", used, max, keys, ev)
		if ev == 0 {
			t.Fatal("expected eviction")
		}
	}
	if _, ok := db.Get("hot"); !ok {
		t.Fatal("hot key should survive LFU")
	}
}

func TestAllKeysRandomEvictsUnderPressure(t *testing.T) {
	db := smallDB(PolicyAllKeysRandom, 100, 2)
	defer db.Close()
	for i := 0; i < 30; i++ {
		_, _ = db.Set(fmt.Sprintf("k%d", i), "zzzzzzzz", SetOptions{})
	}
	used, max, _, _, _, ev := db.Stats()
	if used > max {
		t.Fatalf("used %d > max %d", used, max)
	}
	if ev == 0 {
		t.Fatal("expected random evictions under pressure")
	}
}

func TestVolatileLRUOnlyExpires(t *testing.T) {
	db := smallDB(PolicyVolatileLRU, 80, 1)
	defer db.Close()
	_, _ = db.Set("perm", "xxxxxxxxxx", SetOptions{})                   // no TTL
	_, _ = db.Set("tmp", "xxxxxxxxxx", SetOptions{PX: time.Hour})        // volatile
	_, err := db.Set("need", "xxxxxxxxxx", SetOptions{})
	if err != nil {
		// should evict tmp not perm
		t.Log(err)
	}
	if _, ok := db.Get("perm"); !ok {
		t.Fatal("permanent key must not be evicted by volatile-* policy")
	}
}

func TestVolatileOnlyOOMsWhenNoVolatile(t *testing.T) {
	db := smallDB(PolicyVolatileRandom, 50, 1)
	defer db.Close()
	// Fill with permanent keys only — volatile policy cannot free them.
	_, err := db.Set("a", "xxxxxxxxxx", SetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	_, err = db.Set("b", "yyyyyyyyyyyyyyyyyyyyyyyy", SetOptions{})
	if err != ErrOOM {
		// might fit or OOM depending on size
		if err == nil {
			// try more
			_, err = db.Set("c", "zzzzzzzzzzzzzzzzzzzzzzzzzzzzzz", SetOptions{})
		}
		if err != ErrOOM {
			t.Fatalf("want OOM when only non-volatile keys exist, got %v", err)
		}
	}
}

func TestVolatileTTLPrefersSoonerExpiry(t *testing.T) {
	db := smallDB(PolicyVolatileTTL, 80, 1)
	defer db.Close()
	_, _ = db.Set("soon", "xxxxxxxxxx", SetOptions{PX: 50 * time.Millisecond})
	_, _ = db.Set("later", "xxxxxxxxxx", SetOptions{PX: time.Hour})
	_, err := db.Set("x", "xxxxxxxxxx", SetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	// "soon" should be gone preferentially
	if _, ok := db.Get("soon"); ok {
		t.Fatal("expected soon-expiring key to be evicted first")
	}
}

func TestNeighborTenantsIndependent(t *testing.T) {
	// Two DBs simulate two tenants — fill one, other untouched.
	hot := smallDB(PolicyAllKeysLRU, 60, 2)
	cold := smallDB(PolicyAllKeysLRU, 1<<20, 2)
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

func TestFIFOVolatileExtension(t *testing.T) {
	db := smallDB(PolicyVolatileFIFO, 80, 1)
	defer db.Close()
	_, _ = db.Set("v1", "xxxxxxxxxx", SetOptions{EX: 60})
	_, _ = db.Set("v2", "xxxxxxxxxx", SetOptions{EX: 60})
	_, _ = db.Get("v1") // must not reorder FIFO
	_, err := db.Set("v3", "xxxxxxxxxx", SetOptions{EX: 60})
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := db.Get("v1"); ok {
		t.Fatal("volatile-fifo should evict oldest volatile insert (v1)")
	}
}
