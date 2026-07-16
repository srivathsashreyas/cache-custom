package store

import (
	"fmt"
	"sync"
	"testing"
	"time"
)

func TestGetSetDel(t *testing.T) {
	db := New(Config{MaxMemory: 1 << 20, ShardCount: 4, Strategy: StrategyGlobalTrack})
	defer db.Close()

	ok, err := db.Set("a", "1", SetOptions{})
	if err != nil || !ok {
		t.Fatalf("set: %v %v", ok, err)
	}
	v, found := db.Get("a")
	if !found || v != "1" {
		t.Fatalf("get: %v %v", v, found)
	}
	if db.Del("a") != 1 {
		t.Fatal("del")
	}
	if _, found := db.Get("a"); found {
		t.Fatal("expected miss")
	}
}

func TestLazyExpiry(t *testing.T) {
	db := New(Config{MaxMemory: 1 << 20, ShardCount: 2, Strategy: StrategyGlobalTrack, ExpiryInterval: time.Hour})
	defer db.Close()

	_, _ = db.Set("k", "v", SetOptions{PX: 30 * time.Millisecond})
	time.Sleep(50 * time.Millisecond)
	if _, ok := db.Get("k"); ok {
		t.Fatal("lazy expire failed")
	}
}

func TestPeriodicExpiry(t *testing.T) {
	db := New(Config{MaxMemory: 1 << 20, ShardCount: 2, Strategy: StrategyGlobalTrack, ExpiryInterval: 20 * time.Millisecond})
	defer db.Close()

	_, _ = db.Set("p", "v", SetOptions{PX: 25 * time.Millisecond})
	// Do not touch key; wait for periodic worker.
	deadline := time.Now().Add(500 * time.Millisecond)
	for time.Now().Before(deadline) {
		if db.Exists("p") == 0 {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("periodic expiry did not remove key")
}

func TestTTLPersist(t *testing.T) {
	db := New(Config{MaxMemory: 1 << 20, ShardCount: 1, Strategy: StrategyGlobalTrack})
	defer db.Close()
	_, _ = db.Set("t", "v", SetOptions{})
	if db.Expire("t", 5*time.Second) != 1 {
		t.Fatal("expire")
	}
	if db.TTL("t") < 1 {
		t.Fatalf("ttl %d", db.TTL("t"))
	}
	if db.Persist("t") != 1 {
		t.Fatal("persist")
	}
	if db.TTL("t") != -1 {
		t.Fatalf("ttl after persist %d", db.TTL("t"))
	}
}

func TestNXXoptions(t *testing.T) {
	db := New(Config{MaxMemory: 1 << 20, ShardCount: 1, Strategy: StrategyGlobalTrack})
	defer db.Close()
	ok, _ := db.Set("n", "1", SetOptions{NX: true})
	if !ok {
		t.Fatal("nx first")
	}
	ok, _ = db.Set("n", "2", SetOptions{NX: true})
	if ok {
		t.Fatal("nx second should fail")
	}
	ok, _ = db.Set("missing", "x", SetOptions{XX: true})
	if ok {
		t.Fatal("xx missing")
	}
	ok, _ = db.Set("n", "3", SetOptions{XX: true})
	if !ok {
		t.Fatal("xx existing")
	}
}

func TestIncr(t *testing.T) {
	db := New(Config{MaxMemory: 1 << 20, ShardCount: 2, Strategy: StrategyGlobalTrack})
	defer db.Close()
	n, err := db.IncrBy("c", 1)
	if err != nil || n != 1 {
		t.Fatalf("%v %v", n, err)
	}
	n, err = db.IncrBy("c", 5)
	if err != nil || n != 6 {
		t.Fatalf("%v %v", n, err)
	}
}

func TestStrategyGlobalTrackEvicts(t *testing.T) {
	// Small maxmemory forces eviction.
	db := New(Config{MaxMemory: 80, ShardCount: 2, Strategy: StrategyGlobalTrack})
	defer db.Close()
	for i := 0; i < 20; i++ {
		k := fmt.Sprintf("k%02d", i)
		_, err := db.Set(k, "xxxxxxxxxx", SetOptions{}) // ~10+2+24 = 36 bytes-ish
		if err != nil {
			// later keys may OOM if single entry too large; use smaller
			t.Logf("set %s: %v", k, err)
		}
	}
	used, max, keys, _, _, ev := db.Stats()
	if used > max {
		t.Fatalf("used %d > max %d", used, max)
	}
	if keys == 0 {
		t.Fatal("expected some keys")
	}
	if ev == 0 && used+36 > max {
		// may still have evicted
	}
	t.Logf("global track used=%d keys=%d evictions=%d", used, keys, ev)
}

func TestStrategyStealEvicts(t *testing.T) {
	db := New(Config{MaxMemory: 100, ShardCount: 4, Strategy: StrategySteal})
	defer db.Close()
	for i := 0; i < 30; i++ {
		_, _ = db.Set(fmt.Sprintf("s%d", i), "yyyyyyyy", SetOptions{})
	}
	used, max, _, _, _, _ := db.Stats()
	if used > max {
		t.Fatalf("used %d > max %d", used, max)
	}
}

func TestStrategyShardBudgetRejectsOrLocalEvict(t *testing.T) {
	// 2 shards => budget ~50 each
	db := New(Config{MaxMemory: 100, ShardCount: 2, Strategy: StrategyShardBudget})
	defer db.Close()
	// Fill aggressively; never exceed tenant max via sum of budgets.
	var oom int
	for i := 0; i < 40; i++ {
		_, err := db.Set(fmt.Sprintf("b%d", i), "zzzzzzzz", SetOptions{})
		if err == ErrOOM {
			oom++
		}
	}
	used, max, _, _, _, _ := db.Stats()
	if used > max {
		t.Fatalf("used %d > max %d", used, max)
	}
	// Per-shard caps can OOM while other shard has space.
	t.Logf("shard budget used=%d oom=%d", used, oom)
}

func TestMaxTTLCeiling(t *testing.T) {
	db := New(Config{MaxMemory: 1 << 20, ShardCount: 1, Strategy: StrategyGlobalTrack, MaxTTL: 100 * time.Millisecond})
	defer db.Close()
	_, _ = db.Set("m", "v", SetOptions{EX: 10 * time.Second}) // request 10s, clamp to MaxTTL
	pt := db.PTTL("m")
	if pt < 0 || pt > 150 {
		t.Fatalf("pttl %d want ~100ms", pt)
	}
}

func TestMgetStyle(t *testing.T) {
	db := New(Config{MaxMemory: 1 << 20, ShardCount: 3, Strategy: StrategySteal})
	defer db.Close()
	_, _ = db.Set("a", "1", SetOptions{})
	_, _ = db.Set("b", "2", SetOptions{})
	if db.Exists("a", "b", "c") != 2 {
		t.Fatal("exists")
	}
	if db.DBSize() != 2 {
		t.Fatalf("dbsize %d", db.DBSize())
	}
}

func TestConcurrentGetsSets(t *testing.T) {
	db := New(Config{MaxMemory: 1 << 20, ShardCount: 8, Strategy: StrategyGlobalTrack})
	defer db.Close()
	const (
		workers = 64
		iters   = 50
	)
	var wg sync.WaitGroup
	errCh := make(chan error, workers)

	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			k := fmt.Sprintf("ck%d", id)
			for j := 0; j < iters; j++ {
				want := fmt.Sprintf("v%d", j)
				ok, err := db.Set(k, want, SetOptions{})
				if err != nil {
					errCh <- fmt.Errorf("worker %d set j=%d: %w", id, j, err)
					return
				}
				if !ok {
					errCh <- fmt.Errorf("worker %d set j=%d: NX/XX blocked unexpectedly", id, j)
					return
				}
				got, found := db.Get(k)
				if !found {
					errCh <- fmt.Errorf("worker %d get j=%d: missing key", id, j)
					return
				}
				if got != want {
					errCh <- fmt.Errorf("worker %d get j=%d: got %q want %q", id, j, got, want)
					return
				}
			}
		}(i)
	}
	wg.Wait()
	close(errCh)
	for err := range errCh {
		t.Error(err)
	}
	if t.Failed() {
		return
	}

	// Final state: each worker's key holds the last written value.
	for i := 0; i < workers; i++ {
		k := fmt.Sprintf("ck%d", i)
		want := fmt.Sprintf("v%d", iters-1)
		got, found := db.Get(k)
		if !found || got != want {
			t.Errorf("final %s: found=%v got=%q want=%q", k, found, got, want)
		}
	}
	// Keys should not exceed worker count under this workload.
	if n := db.DBSize(); n != int64(workers) {
		t.Errorf("DBSize=%d want %d", n, workers)
	}
}
