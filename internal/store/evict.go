package store

import (
	"math/rand"
	"time"
)

// pickLocalVictim chooses an eviction candidate in sh (sh.mu held). skipKey is never chosen.
// Complexities: LRU/FIFO/LFU O(1); random O(1) expected; volatile-ttl O(log n) via min-heap.
func (db *DB) pickLocalVictim(sh *shard, skipKey string, now time.Time) *entry {
	pol := db.cfg.Policy
	if pol == PolicyNoEviction {
		return nil
	}
	vol := pol.volatileOnly()

	switch pol {
	case PolicyAllKeysLRU, PolicyAllKeysFIFO, PolicyVolatileLRU, PolicyVolatileFIFO:
		for e := sh.head; e != nil; e = e.lNext {
			if e.key == skipKey {
				continue
			}
			if pol.eligible(e) {
				return e
			}
		}
		return nil

	case PolicyAllKeysRandom, PolicyVolatileRandom:
		return sh.rndPick(skipKey, vol)

	case PolicyAllKeysLFU, PolicyVolatileLFU:
		return sh.lfuPick(skipKey, vol)

	case PolicyVolatileTTL:
		return sh.ttlPick(skipKey, now)

	default:
		for e := sh.head; e != nil; e = e.lNext {
			if e.key != skipKey && pol.eligible(e) {
				return e
			}
		}
		return nil
	}
}

// pickGlobalVictimKey returns a tenant-wide victim key (no shard locks held by caller).
func (db *DB) pickGlobalVictimKey(skipKey string, now time.Time) (key string, ok bool) {
	pol := db.cfg.Policy
	if pol == PolicyNoEviction {
		return "", false
	}

	switch pol {
	case PolicyAllKeysLRU, PolicyAllKeysFIFO, PolicyVolatileLRU, PolicyVolatileFIFO:
		db.gMu.Lock()
		for e := db.gHead; e != nil; e = e.gNext {
			if e.key == skipKey {
				continue
			}
			if pol.eligible(e) {
				key = e.key
				db.gMu.Unlock()
				return key, true
			}
		}
		db.gMu.Unlock()
		return "", false

	case PolicyAllKeysRandom, PolicyVolatileRandom:
		return db.pickGlobalRandom(skipKey)

	case PolicyAllKeysLFU, PolicyVolatileLFU:
		return db.pickGlobalLFU(skipKey)

	case PolicyVolatileTTL:
		return db.pickGlobalTTL(skipKey, now)

	default:
		return db.pickGlobalRandom(skipKey)
	}
}

// pickGlobalRandom: O(1) expected — random shard then O(1) local random.
func (db *DB) pickGlobalRandom(skipKey string) (string, bool) {
	vol := db.cfg.Policy.volatileOnly()
	n := len(db.shards)
	start := rand.Intn(n)
	for i := 0; i < n; i++ {
		si := (start + i) % n
		sh := db.shards[si]
		sh.mu.Lock()
		e := sh.rndPick(skipKey, vol)
		if e != nil {
			k := e.key
			sh.mu.Unlock()
			return k, true
		}
		sh.mu.Unlock()
	}
	return "", false
}

// pickGlobalLFU: O(numShards) with O(1) work per shard (min-freq bucket head).
func (db *DB) pickGlobalLFU(skipKey string) (string, bool) {
	vol := db.cfg.Policy.volatileOnly()
	var bestKey string
	var bestFreq uint8
	found := false
	for _, sh := range db.shards {
		sh.mu.Lock()
		e := sh.lfuPick(skipKey, vol)
		if e != nil {
			if !found || e.freq < bestFreq || (e.freq == bestFreq && e.key < bestKey) {
				bestKey, bestFreq, found = e.key, e.freq, true
			}
		}
		sh.mu.Unlock()
	}
	return bestKey, found
}

// pickGlobalTTL: O(log n) via tenant expiry min-heap (soonest deadline).
func (db *DB) pickGlobalTTL(skipKey string, now time.Time) (string, bool) {
	for {
		db.expMu.Lock()
		if len(db.expH) == 0 {
			db.expMu.Unlock()
			return "", false
		}
		item := db.expH[0]
		db.expMu.Unlock()

		if item.key == skipKey {
			// Cannot evict the key being written; drop heap entry only if it is still min and skip.
			// Leave heap intact and scan: temporarily not O(log n) worst-case if skip is always min.
			// Rare: peek next by removing skip from consideration via re-check under shard.
			si := db.shardIndex(item.key)
			sh := db.shards[si]
			sh.mu.Lock()
			// Find any other TTL key in same shard first (cheap), else try other shards' heaps.
			e := sh.ttlPick(skipKey, now)
			sh.mu.Unlock()
			if e != nil {
				return e.key, true
			}
			// Fall back: O(shards) local ttl picks.
			for _, osh := range db.shards {
				osh.mu.Lock()
				cand := osh.ttlPick(skipKey, now)
				if cand != nil {
					k := cand.key
					osh.mu.Unlock()
					return k, true
				}
				osh.mu.Unlock()
			}
			return "", false
		}

		// Validate key still exists with a TTL.
		si := db.shardIndex(item.key)
		sh := db.shards[si]
		sh.mu.Lock()
		e, ok := sh.data[item.key]
		if !ok || e.expiresAt.IsZero() {
			sh.mu.Unlock()
			db.clearExpiry(item.key)
			continue
		}
		// Stale heap time: still a valid volatile key; prefer true min under shard heap.
		if !e.expiresAt.Equal(item.at) {
			sh.mu.Unlock()
			db.noteExpiry(item.key, e.expiresAt)
			continue
		}
		k := e.key
		sh.mu.Unlock()
		return k, true
	}
}
