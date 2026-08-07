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
	if e.valueType() != TypeString {
		sh.mu.Unlock()
		// Wrong type is not a miss; callers that need WRONGTYPE use GetString.
		return "", false
	}
	db.touch(sh, e)
	val := e.value
	// Always-on global LRU recency (strategy 1); FIFO global list is insert-only.
	needGlobal := db.cfg.Strategy == StrategyGlobalTrack
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

// GetString is like Get but returns ErrWrongType when the key holds a non-string.
func (db *DB) GetString(key string) (string, bool, error) {
	si := db.shardIndex(key)
	sh := db.shards[si]
	now := time.Now()

	sh.mu.Lock()
	e, ok := sh.data[key]
	if !ok {
		sh.mu.Unlock()
		db.misses.Add(1)
		return "", false, nil
	}
	if e.expired(now) {
		db.removeFromShard(sh, e, false)
		sh.mu.Unlock()
		db.clearExpiry(key)
		db.misses.Add(1)
		return "", false, nil
	}
	if e.valueType() != TypeString {
		sh.mu.Unlock()
		return "", false, ErrWrongType
	}
	db.touch(sh, e)
	val := e.value
	needGlobal := db.cfg.Strategy == StrategyGlobalTrack
	sh.mu.Unlock()
	if needGlobal {
		db.gMu.Lock()
		if db.stillInGlobal(e) {
			db.globalTouch(e)
		}
		db.gMu.Unlock()
	}
	db.hits.Add(1)
	return val, true, nil
}

// touch updates all always-on access indexes (shard mu held).
// Policy only selects the victim rule at eviction time; structures stay warm for seamless switch.
func (db *DB) touch(sh *shard, e *entry) {
	// Frequency (LFU history).
	if e.freq <= 0 {
		e.freq = 1
	}
	e.freq++
	// Recency (LRU); FIFO insertion list is not moved here.
	sh.lruTouch(e)
	// LFU bucket position for current frequency.
	if e.inLFU && e.bucket != nil {
		sh.lfuRelocate(e)
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

	// Redis SET replaces any prior type with a string.
	if exists && existing.valueType() != TypeString {
		oldSize := existing.size
		db.removeFromShard(sh, existing, false)
		// removeFromShard already adjusted used; clear expiry index for key.
		sh.mu.Unlock()
		db.clearExpiry(key)
		sh.mu.Lock()
		existing, exists = sh.data[key]
		_ = oldSize
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
		existing.typ = TypeString
		existing.value = value
		existing.hash = nil
		existing.list = nil
		existing.set = nil
		existing.zset = nil
		existing.size = newSize
		if !(opt.KeepTTL && !opt.HasExpireAt && opt.EX == 0 && opt.PX == 0) {
			existing.expiresAt = expiresAt
		}
		finalExp := existing.expiresAt
		sh.ttlUpdate(key, finalExp)
		db.touch(sh, existing)
		if db.cfg.Strategy == StrategyGlobalTrack {
			sh.mu.Unlock()
			db.gMu.Lock()
			if db.stillInGlobal(existing) {
				db.globalTouch(existing)
			}
			db.gMu.Unlock()
			db.noteExpiry(key, finalExp)
			db.emit(Mutation{Op: "SET", Key: key, Value: value, ExpiresAt: finalExp})
			return true, nil
		}
		sh.mu.Unlock()
		db.noteExpiry(key, finalExp)
		db.emit(Mutation{Op: "SET", Key: key, Value: value, ExpiresAt: finalExp})
		return true, nil
	}

	e := &entry{key: key, typ: TypeString, value: value, expiresAt: expiresAt, size: newSize, freq: 1, rndIdx: -1}
	sh.data[key] = e
	sh.used += newSize
	db.addUsed(newSize)
	// Always-on indexes: recency, insertion order, random, LFU.
	sh.lruPushTail(e)
	sh.fifoPushTail(e)
	sh.rndAdd(e)
	sh.lfuAdd(e)
	sh.ttlUpdate(key, expiresAt)
	if db.cfg.Strategy == StrategyGlobalTrack {
		sh.mu.Unlock()
		db.gMu.Lock()
		db.globalPushTail(e)
		db.globalFifoPushTail(e)
		db.gMu.Unlock()
		db.noteExpiry(key, expiresAt)
		db.emit(Mutation{Op: "SET", Key: key, Value: value, ExpiresAt: expiresAt})
		return true, nil
	}
	sh.mu.Unlock()
	db.noteExpiry(key, expiresAt)
	db.emit(Mutation{Op: "SET", Key: key, Value: value, ExpiresAt: expiresAt})
	return true, nil
}

// SetEvictionPolicy changes the maxmemory victim policy. Indexes are always-on, so this is O(1).
func (db *DB) SetEvictionPolicy(p EvictionPolicy) {
	if _, ok := ParseEvictionPolicy(string(p)); !ok || p == "" {
		p = PolicyAllKeysLRU
	}
	db.cfg.Policy = p
}

// SetShardingStrategy changes how memory pressure is applied across shards.
// Local indexes stay correct always; global LRU/FIFO lists are only maintained while
// strategy is StrategyGlobalTrack. Switching into global rebuilds those lists from
// per-shard state; switching out clears them.
func (db *DB) SetShardingStrategy(s Strategy) {
	if s < StrategyGlobalTrack || s > StrategyShardBudget {
		s = StrategyGlobalTrack
	}
	if db.cfg.Strategy == s {
		return
	}

	// Freeze all shards then gMu so rebuild cannot race with insert/evict
	// (those use shard→gMu or shard-only lock orders).
	for _, sh := range db.shards {
		sh.mu.Lock()
	}
	db.gMu.Lock()

	db.clearGlobalOrderListsLocked()
	db.cfg.Strategy = s
	if s == StrategyGlobalTrack {
		db.rebuildGlobalOrderListsLocked()
	}

	db.gMu.Unlock()
	for i := len(db.shards) - 1; i >= 0; i-- {
		db.shards[i].mu.Unlock()
	}
}

// clearGlobalOrderListsLocked clears global LRU/FIFO under gMu + all shard locks.
func (db *DB) clearGlobalOrderListsLocked() {
	for _, sh := range db.shards {
		for e := sh.head; e != nil; e = e.lNext {
			e.gPrev, e.gNext = nil, nil
			e.giPrev, e.giNext = nil, nil
		}
	}
	db.gHead, db.gTail = nil, nil
	db.gFifoHead, db.gFifoTail = nil, nil
}

// rebuildGlobalOrderListsLocked repopulates global LRU/FIFO from per-shard lists.
// Call only with every shard.mu and gMu held, after clearGlobalOrderListsLocked.
func (db *DB) rebuildGlobalOrderListsLocked() {
	for _, sh := range db.shards {
		// Local LRU head→tail is LRU→MRU; pushTail preserves that relative order.
		for e := sh.head; e != nil; e = e.lNext {
			db.globalPushTail(e)
		}
		// Local FIFO head→tail is oldest→newest insertion.
		for e := sh.fifoHead; e != nil; e = e.iNext {
			db.globalFifoPushTail(e)
		}
	}
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

// removeFromShard drops e from the shard map and all always-on indexes.
// sh.mu must be held. globalAlreadyUnlinked skips strategy-1 list unlinks.
func (db *DB) removeFromShard(sh *shard, e *entry, globalAlreadyUnlinked bool) {
	delete(sh.data, e.key)
	sh.lruRemove(e)
	sh.fifoRemove(e)
	sh.rndRemove(e)
	if e.inLFU {
		sh.lfuRemove(e)
	}
	sh.ttlRemove(e.key)
	if sh.used >= e.size {
		sh.used -= e.size
	} else {
		sh.used = 0
	}
	db.subUsed(e.size)

	if db.cfg.Strategy == StrategyGlobalTrack && !globalAlreadyUnlinked {
		db.gMu.Lock()
		if db.stillInGlobal(e) {
			db.globalRemove(e)
		}
		// FIFO global: unlink if still linked (head or has neighbors).
		if db.gFifoHead == e || e.giPrev != nil || e.giNext != nil {
			db.globalFifoRemove(e)
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
			db.emit(Mutation{Op: "DEL", Keys: []string{key}})
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
	sh.ttlUpdate(key, at)
	sh.mu.Unlock()
	db.noteExpiry(key, at)
	db.emit(Mutation{Op: "EXPIRE", Key: key, ExpiresAt: at})
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
	sh.ttlUpdate(key, time.Time{})
	sh.mu.Unlock()
	db.clearExpiry(key)
	db.emit(Mutation{Op: "PERSIST", Key: key})
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

// Type returns Redis TYPE name: none|string|hash|list|set|zset.
func (db *DB) Type(key string) string {
	si := db.shardIndex(key)
	sh := db.shards[si]
	now := time.Now()
	sh.mu.Lock()
	e, ok := sh.data[key]
	if !ok {
		sh.mu.Unlock()
		return "none"
	}
	if e.expired(now) {
		db.removeFromShard(sh, e, false)
		sh.mu.Unlock()
		db.clearExpiry(key)
		return "none"
	}
	t := e.valueType().String()
	sh.mu.Unlock()
	return t
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
		if e.valueType() != TypeString {
			sh.mu.Unlock()
			return 0, ErrWrongType
		}
		var err error
		cur, err = strconv.ParseInt(e.value, 10, 64)
		if err != nil {
			sh.mu.Unlock()
			return 0, fmt.Errorf("value is not an integer or out of range")
		}
		exp = e.expiresAt
	}
	sh.mu.Unlock()

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

// adjustEntrySize updates memory accounting after in-place structure mutation (sh.mu held).
func (db *DB) adjustEntrySize(sh *shard, e *entry, newSize uint64) {
	old := e.size
	if newSize >= old {
		db.addUsed(newSize - old)
	} else {
		db.subUsed(old - newSize)
	}
	if sh.used >= old {
		sh.used = sh.used - old + newSize
	} else {
		sh.used = newSize
	}
	e.size = newSize
}

// insertNewEntry places a brand-new entry into the shard (sh.mu held). Caller must ensureSpace first.
func (db *DB) insertNewEntry(sh *shard, e *entry) {
	sh.data[e.key] = e
	sh.used += e.size
	db.addUsed(e.size)
	sh.lruPushTail(e)
	sh.fifoPushTail(e)
	sh.rndAdd(e)
	sh.lfuAdd(e)
	sh.ttlUpdate(e.key, e.expiresAt)
}

// afterInsertGlobal links strategy-1 global lists (call after releasing sh.mu if needed).
func (db *DB) afterInsertGlobal(e *entry) {
	if db.cfg.Strategy != StrategyGlobalTrack {
		return
	}
	db.gMu.Lock()
	db.globalPushTail(e)
	db.globalFifoPushTail(e)
	db.gMu.Unlock()
}
