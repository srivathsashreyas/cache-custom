package store

import (
	"fmt"
	"strconv"
	"time"
)

// Get returns the value and true if present (after lazy expiry).
// Hot path locks only the key's shard; strategy 1 briefly takes gMu after releasing the shard.
func (db *DB) Get(key string) (string, bool) {
	si := db.shardIndex(key)
	sh := db.shards[si]
	now := time.Now()

	sh.mu.Lock()
	e, ok := sh.data[key]
	if !ok {
		sh.mu.Unlock()
		db.misses.Add(1)
		return "", false
	}
	if e.expired(now) {
		db.removeFromShard(sh, e, false)
		sh.mu.Unlock()
		db.clearExpiry(key)
		db.misses.Add(1)
		return "", false
	}
	db.onAccess(sh, e)
	val := e.value
	needGlobal := db.cfg.Strategy == StrategyGlobalTrack && db.cfg.Policy.touchesOnAccess()
	sh.mu.Unlock()

	if needGlobal {
		// Do not hold shard + gMu together (eviction order is gMu snapshot → shard → gMu).
		db.gMu.Lock()
		if db.stillInGlobal(e) {
			db.globalTouch(e)
		}
		db.gMu.Unlock()
	}

	db.hits.Add(1)
	return val, true
}

// onAccess updates recency/frequency under shard lock according to eviction policy.
func (db *DB) onAccess(sh *shard, e *entry) {
	if db.cfg.Policy.touchesOnAccess() {
		sh.lruTouch(e)
	}
	if db.cfg.Policy.bumpsLFUOnAccess() {
		bumpLFU(e)
	}
}

// stillInGlobal reports whether e is currently linked in the tenant-global LRU (gMu held).
func (db *DB) stillInGlobal(e *entry) bool {
	if db.gHead == e {
		return true
	}
	return e.gPrev != nil || e.gNext != nil
}

// SetOptions controls SET.
type SetOptions struct {
	EX, PX      time.Duration
	NX, XX      bool
	KeepTTL     bool
	ExpireAt    time.Time
	HasExpireAt bool
}

// Set stores a string. ok=false means NX/XX condition failed; err is ErrOOM on memory failure.
func (db *DB) Set(key, value string, opt SetOptions) (bool, error) {
	si := db.shardIndex(key)
	sh := db.shards[si]
	now := time.Now()
	newSize := memSize(key, value)

	var expiresAt time.Time
	if opt.HasExpireAt {
		expiresAt = opt.ExpireAt
	} else if opt.PX > 0 {
		expiresAt = now.Add(opt.PX)
	} else if opt.EX > 0 {
		expiresAt = now.Add(opt.EX)
	}
	if !expiresAt.IsZero() && db.cfg.MaxTTL > 0 {
		maxAt := now.Add(db.cfg.MaxTTL)
		if expiresAt.After(maxAt) {
			expiresAt = maxAt
		}
	}

	// Strategy 1: free tenant memory before taking the shard lock when possible,
	// so global eviction does not hold the target shard during multi-shard cleanup.
	if db.cfg.Strategy == StrategyGlobalTrack {
		if newSize > db.cfg.MaxMemory {
			return false, ErrOOM
		}
	}

	sh.mu.Lock()

	if e, ok := sh.data[key]; ok && e.expired(now) {
		db.removeFromShard(sh, e, false)
		sh.mu.Unlock()
		db.clearExpiry(key)
		sh.mu.Lock()
	}

	existing, exists := sh.data[key]
	if opt.NX && exists {
		sh.mu.Unlock()
		return false, nil
	}
	if opt.XX && !exists {
		sh.mu.Unlock()
		return false, nil
	}

	var oldSize uint64
	if exists {
		oldSize = existing.size
		if opt.KeepTTL && !opt.HasExpireAt && opt.EX == 0 && opt.PX == 0 {
			expiresAt = existing.expiresAt
		}
	}

	var need uint64
	if exists {
		if newSize > oldSize {
			need = newSize - oldSize
		}
	} else {
		need = newSize
	}

	// Make room. May unlock/relock the target shard (strategies 1–2).
	if err := db.ensureSpace(sh, si, key, need, newSize, exists); err != nil {
		sh.mu.Unlock()
		return false, err
	}

	// Re-fetch after possible unlock inside ensureSpace.
	existing, exists = sh.data[key]
	if exists {
		oldSize = existing.size
		if newSize > oldSize {
			need = newSize - oldSize
		} else {
			need = 0
		}
		// NX/XX could race if we unlocked — re-check.
		if opt.NX && exists {
			// already exists: NX fails
			sh.mu.Unlock()
			return false, nil
		}
	} else if opt.XX {
		sh.mu.Unlock()
		return false, nil
	}

	if exists {
		if newSize >= oldSize {
			db.addUsed(newSize - oldSize)
		} else {
			db.subUsed(oldSize - newSize)
		}
		sh.used = sh.used - oldSize + newSize
		existing.value = value
		existing.size = newSize
		if !(opt.KeepTTL && !opt.HasExpireAt && opt.EX == 0 && opt.PX == 0) {
			existing.expiresAt = expiresAt
		}
		finalExp := existing.expiresAt
		// Updates: LRU moves to MRU; FIFO keeps insertion position; LFU bumps freq.
		if db.cfg.Policy.touchesOnAccess() {
			sh.lruTouch(existing)
		}
		if db.cfg.Policy.bumpsLFUOnAccess() {
			bumpLFU(existing)
		}
		if db.cfg.Strategy == StrategyGlobalTrack && db.cfg.Policy.touchesOnAccess() {
			sh.mu.Unlock()
			db.gMu.Lock()
			if db.stillInGlobal(existing) {
				db.globalTouch(existing)
			}
			db.gMu.Unlock()
			db.noteExpiry(key, finalExp)
			return true, nil
		}
		sh.mu.Unlock()
		db.noteExpiry(key, finalExp)
		return true, nil
	}

	e := &entry{key: key, value: value, expiresAt: expiresAt, size: newSize, freq: 5}
	sh.data[key] = e
	sh.used += newSize
	db.addUsed(newSize)
	sh.lruPushTail(e)
	if db.cfg.Strategy == StrategyGlobalTrack {
		sh.mu.Unlock()
		db.gMu.Lock()
		db.globalPushTail(e)
		db.gMu.Unlock()
		db.noteExpiry(key, expiresAt)
		return true, nil
	}
	sh.mu.Unlock()
	db.noteExpiry(key, expiresAt)
	return true, nil
}

// ensureSpace requires sh.mu held on entry for target shard; may unlock/relock it.
func (db *DB) ensureSpace(sh *shard, si int, key string, need, newSize uint64, exists bool) error {
	// Hard ceiling: single entry larger than the limit cannot be stored.
	limit := db.cfg.MaxMemory
	if db.cfg.Strategy == StrategyShardBudget {
		if newSize > sh.budget {
			return ErrOOM
		}
	} else if newSize > limit {
		return ErrOOM
	}

	// noeviction: never free space; OOM when full.
	if db.cfg.Policy == PolicyNoEviction {
		switch db.cfg.Strategy {
		case StrategyShardBudget:
			if sh.used+need > sh.budget {
				return ErrOOM
			}
		default:
			if db.used.Load()+need > limit {
				return ErrOOM
			}
		}
		return nil
	}

	switch db.cfg.Strategy {
	case StrategyGlobalTrack:
		sh.mu.Unlock()
		for db.used.Load()+need > limit {
			if !db.evictOneGlobal(key) {
				sh.mu.Lock()
				return ErrOOM
			}
		}
		sh.mu.Lock()
		return nil

	case StrategySteal:
		for db.used.Load()+need > limit {
			if db.evictOneLocal(sh, key) {
				continue
			}
			// Steal: release target, take another shard, come back.
			sh.mu.Unlock()
			if !db.stealFromOtherShards(si, key) {
				sh.mu.Lock()
				return ErrOOM
			}
			sh.mu.Lock()
			_, exists = sh.data[key]
			_ = exists
		}
		return nil

	case StrategyShardBudget:
		for sh.used+need > sh.budget {
			if !db.evictOneLocal(sh, key) {
				return ErrOOM
			}
		}
		return nil
	}
	return nil
}

// evictOneGlobal removes one tenant-wide victim. No shard locks held by caller.
// skipKey is never evicted (key being written).
func (db *DB) evictOneGlobal(skipKey string) bool {
	now := time.Now()
	key, ok := db.pickGlobalVictimKey(skipKey, now)
	if !ok {
		return false
	}
	si := db.shardIndex(key)
	sh := db.shards[si]
	sh.mu.Lock()
	cur, ok := sh.data[key]
	if !ok {
		sh.mu.Unlock()
		// Stale global list node for list-based policies.
		db.gMu.Lock()
		// Best-effort: if head is stale and points at missing key, unlink if still linked.
		for e := db.gHead; e != nil; e = e.gNext {
			if e.key == key {
				db.globalRemove(e)
				break
			}
		}
		db.gMu.Unlock()
		return true
	}
	db.removeFromShard(sh, cur, false)
	sh.mu.Unlock()
	db.clearExpiry(key)
	db.evictions.Add(1)
	return true
}

// evictOneLocal picks and removes one victim in sh (sh.mu held).
func (db *DB) evictOneLocal(sh *shard, skipKey string) bool {
	v := db.pickLocalVictim(sh, skipKey, time.Now())
	if v == nil {
		return false
	}
	key := v.key
	db.removeFromShard(sh, v, false)
	db.evictions.Add(1)
	db.clearExpiry(key)
	return true
}

func (db *DB) stealFromOtherShards(holdSI int, skipKey string) bool {
	n := len(db.shards)
	for i := 0; i < n; i++ {
		si := (holdSI + 1 + i) % n
		if si == holdSI {
			continue
		}
		osh := db.shards[si]
		osh.mu.Lock()
		ok := db.evictOneLocal(osh, skipKey)
		osh.mu.Unlock()
		if ok {
			return true
		}
	}
	return false
}

// removeFromShard drops e from the shard map/local LRU and tenant used.
// sh.mu must be held. globalAlreadyUnlinked skips strategy-1 list unlink.
func (db *DB) removeFromShard(sh *shard, e *entry, globalAlreadyUnlinked bool) {
	delete(sh.data, e.key)
	sh.lruRemove(e)
	if sh.used >= e.size {
		sh.used -= e.size
	} else {
		sh.used = 0
	}
	db.subUsed(e.size)

	if db.cfg.Strategy == StrategyGlobalTrack && !globalAlreadyUnlinked {
		// Never hold gMu while calling out; we already hold shard — take gMu second.
		// Eviction path uses gMu then shard without holding both from the other direction
		// on a second shard. Here: shard held, then gMu — eviction holds gMu only briefly
		// without shard, then shard, then gMu again. Deadlock: this waits gMu while
		// evictOneGlobal holds gMu... it doesn't hold gMu across shard lock.
		// evict: gMu lock/unlock; shard lock; gMu lock. This: shard; gMu. OK.
		db.gMu.Lock()
		if db.stillInGlobal(e) {
			db.globalRemove(e)
		}
		db.gMu.Unlock()
	}
}

// Del removes keys; returns count removed (expired keys count as not found).
func (db *DB) Del(keys ...string) int64 {
	var n int64
	now := time.Now()
	for _, key := range keys {
		si := db.shardIndex(key)
		sh := db.shards[si]
		sh.mu.Lock()
		e, ok := sh.data[key]
		if !ok {
			sh.mu.Unlock()
			continue
		}
		expired := e.expired(now)
		db.removeFromShard(sh, e, false)
		sh.mu.Unlock()
		db.clearExpiry(key)
		if !expired {
			n++
		}
	}
	return n
}

// Exists returns how many of the keys exist.
func (db *DB) Exists(keys ...string) int64 {
	var n int64
	now := time.Now()
	for _, key := range keys {
		si := db.shardIndex(key)
		sh := db.shards[si]
		sh.mu.Lock()
		e, ok := sh.data[key]
		if !ok {
			sh.mu.Unlock()
			continue
		}
		if e.expired(now) {
			db.removeFromShard(sh, e, false)
			sh.mu.Unlock()
			db.clearExpiry(key)
			continue
		}
		sh.mu.Unlock()
		n++
	}
	return n
}

// Expire sets a relative TTL. d<=0 deletes the key (Redis-like).
func (db *DB) Expire(key string, d time.Duration) int64 {
	if d <= 0 {
		if db.Del(key) > 0 {
			return 1
		}
		return 0
	}
	if db.cfg.MaxTTL > 0 && d > db.cfg.MaxTTL {
		d = db.cfg.MaxTTL
	}
	return db.setExpireAt(key, time.Now().Add(d))
}

func (db *DB) setExpireAt(key string, at time.Time) int64 {
	si := db.shardIndex(key)
	sh := db.shards[si]
	now := time.Now()
	sh.mu.Lock()
	e, ok := sh.data[key]
	if !ok || e.expired(now) {
		if ok {
			db.removeFromShard(sh, e, false)
			sh.mu.Unlock()
			db.clearExpiry(key)
			return 0
		}
		sh.mu.Unlock()
		return 0
	}
	e.expiresAt = at
	sh.mu.Unlock()
	db.noteExpiry(key, at)
	return 1
}

// Persist removes any TTL.
func (db *DB) Persist(key string) int64 {
	si := db.shardIndex(key)
	sh := db.shards[si]
	now := time.Now()
	sh.mu.Lock()
	e, ok := sh.data[key]
	if !ok || e.expired(now) {
		if ok {
			db.removeFromShard(sh, e, false)
			sh.mu.Unlock()
			db.clearExpiry(key)
			return 0
		}
		sh.mu.Unlock()
		return 0
	}
	if e.expiresAt.IsZero() {
		sh.mu.Unlock()
		return 0
	}
	e.expiresAt = time.Time{}
	sh.mu.Unlock()
	db.clearExpiry(key)
	return 1
}

// TTL returns seconds remaining; -2 missing, -1 no expiry.
func (db *DB) TTL(key string) int64 {
	ms := db.PTTL(key)
	if ms < 0 {
		return ms
	}
	return (ms + 999) / 1000
}

// PTTL returns milliseconds remaining; -2 missing, -1 no expiry.
func (db *DB) PTTL(key string) int64 {
	si := db.shardIndex(key)
	sh := db.shards[si]
	now := time.Now()
	sh.mu.Lock()
	e, ok := sh.data[key]
	if !ok {
		sh.mu.Unlock()
		return -2
	}
	if e.expired(now) {
		db.removeFromShard(sh, e, false)
		sh.mu.Unlock()
		db.clearExpiry(key)
		return -2
	}
	if e.expiresAt.IsZero() {
		sh.mu.Unlock()
		return -1
	}
	ms := e.expiresAt.Sub(now).Milliseconds()
	sh.mu.Unlock()
	if ms < 0 {
		return -2
	}
	return ms
}

// Type returns "string" or "none".
func (db *DB) Type(key string) string {
	if db.Exists(key) == 0 {
		return "none"
	}
	return "string"
}

// IncrBy applies delta to the integer-at-key (creates 0 base if missing). Preserves TTL.
func (db *DB) IncrBy(key string, delta int64) (int64, error) {
	si := db.shardIndex(key)
	sh := db.shards[si]
	now := time.Now()

	sh.mu.Lock()
	e, ok := sh.data[key]
	if ok && e.expired(now) {
		db.removeFromShard(sh, e, false)
		ok = false
		e = nil
	}

	var cur int64
	var exp time.Time
	if ok {
		var err error
		cur, err = strconv.ParseInt(e.value, 10, 64)
		if err != nil {
			sh.mu.Unlock()
			return 0, fmt.Errorf("value is not an integer or out of range")
		}
		exp = e.expiresAt
	}
	sh.mu.Unlock()
	if !ok {
		// fall through with cur=0
	} else {
		// clearExpiry if we expired above
	}

	n := cur + delta
	opt := SetOptions{}
	if ok && !exp.IsZero() {
		opt.HasExpireAt = true
		opt.ExpireAt = exp
	}
	if _, err := db.Set(key, strconv.FormatInt(n, 10), opt); err != nil {
		return 0, err
	}
	return n, nil
}
