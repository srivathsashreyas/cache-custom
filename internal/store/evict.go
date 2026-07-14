package store

import (
	"math/rand"
	"time"
)

// pickLocalVictim chooses an eviction candidate in sh (sh.mu held). skipKey is never chosen.
func (db *DB) pickLocalVictim(sh *shard, skipKey string, now time.Time) *entry {
	pol := db.cfg.Policy
	if pol == PolicyNoEviction {
		return nil
	}

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
		return pickRandomFromMap(sh.data, skipKey, pol)

	case PolicyAllKeysLFU, PolicyVolatileLFU:
		return pickMinFreq(sh.data, skipKey, pol)

	case PolicyVolatileTTL:
		return pickMinTTL(sh.data, skipKey, now)

	default:
		// Fallback: list head
		for e := sh.head; e != nil; e = e.lNext {
			if e.key != skipKey && pol.eligible(e) {
				return e
			}
		}
		return nil
	}
}

// pickGlobalVictimKey returns a key to evict tenant-wide (no shard locks held).
// For list-based policies uses global list under gMu; for random/lfu/ttl samples shards.
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

	case PolicyAllKeysRandom, PolicyVolatileRandom,
		PolicyAllKeysLFU, PolicyVolatileLFU, PolicyVolatileTTL:
		return db.sampleGlobalVictim(skipKey, now)
	default:
		return db.sampleGlobalVictim(skipKey, now)
	}
}

func (db *DB) sampleGlobalVictim(skipKey string, now time.Time) (string, bool) {
	pol := db.cfg.Policy
	n := len(db.shards)
	start := rand.Intn(n)

	var bestKey string
	var haveBest bool
	var bestFreq uint8
	var bestRem time.Duration

	for i := 0; i < n; i++ {
		si := (start + i) % n
		sh := db.shards[si]
		sh.mu.Lock()
		var cand *entry
		switch pol {
		case PolicyAllKeysRandom, PolicyVolatileRandom:
			cand = pickRandomFromMap(sh.data, skipKey, pol)
		case PolicyAllKeysLFU, PolicyVolatileLFU:
			cand = pickMinFreq(sh.data, skipKey, pol)
		case PolicyVolatileTTL:
			cand = pickMinTTL(sh.data, skipKey, now)
		default:
			cand = pickMinFreq(sh.data, skipKey, pol)
		}
		if cand != nil {
			// Snapshot fields before unlock (do not retain *entry across shards).
			k, freq, rem := cand.key, cand.freq, remainingTTL(cand, now)
			sh.mu.Unlock()
			if pol == PolicyAllKeysRandom || pol == PolicyVolatileRandom {
				return k, true
			}
			if !haveBest {
				bestKey, bestFreq, bestRem, haveBest = k, freq, rem, true
			} else if pol == PolicyAllKeysLFU || pol == PolicyVolatileLFU {
				if freq < bestFreq || (freq == bestFreq && k < bestKey) {
					bestKey, bestFreq = k, freq
				}
			} else if pol == PolicyVolatileTTL {
				if rem < bestRem {
					bestKey, bestRem = k, rem
				}
			}
			continue
		}
		sh.mu.Unlock()
	}
	return bestKey, haveBest
}

func pickRandomFromMap(m map[string]*entry, skipKey string, pol EvictionPolicy) *entry {
	if len(m) == 0 {
		return nil
	}
	// Reservoir-style among eligible.
	var chosen *entry
	n := 0
	for _, e := range m {
		if e.key == skipKey || !pol.eligible(e) {
			continue
		}
		n++
		if rand.Intn(n) == 0 {
			chosen = e
		}
	}
	return chosen
}

func pickMinFreq(m map[string]*entry, skipKey string, pol EvictionPolicy) *entry {
	var best *entry
	for _, e := range m {
		if e.key == skipKey || !pol.eligible(e) {
			continue
		}
		if best == nil || e.freq < best.freq || (e.freq == best.freq && e.key < best.key) {
			best = e
		}
	}
	return best
}

func pickMinTTL(m map[string]*entry, skipKey string, now time.Time) *entry {
	var best *entry
	var bestRem time.Duration
	for _, e := range m {
		if e.key == skipKey || e.expiresAt.IsZero() {
			continue
		}
		rem := remainingTTL(e, now)
		if best == nil || rem < bestRem {
			best = e
			bestRem = rem
		}
	}
	return best
}

// bumpLFU increments a saturating approximate counter (simple LFU).
func bumpLFU(e *entry) {
	if e.freq < 255 {
		e.freq++
	}
}
