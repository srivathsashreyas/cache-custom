package store

import "time"

// HSet sets field/value pairs. Returns number of new fields added.
func (db *DB) HSet(key string, fields [][2]string) (int64, error) {
	if len(fields) == 0 {
		return 0, nil
	}
	si := db.shardIndex(key)
	sh := db.shards[si]
	now := time.Now()

	// Estimate need: worst case all new fields.
	estExtra := uint64(0)
	for _, fv := range fields {
		estExtra += uint64(len(fv[0]) + len(fv[1]) + 16)
	}

	sh.mu.Lock()
	if e, ok := sh.data[key]; ok && e.expired(now) {
		db.removeFromShard(sh, e, false)
		sh.mu.Unlock()
		db.clearExpiry(key)
		sh.mu.Lock()
	}

	e, exists := sh.data[key]
	if exists && e.valueType() != TypeHash {
		sh.mu.Unlock()
		return 0, ErrWrongType
	}

	var need uint64
	if !exists {
		need = memSizeHash(key, nil) + estExtra
	} else {
		need = estExtra // upper bound
	}
	if err := db.ensureSpace(sh, si, key, need, need, exists); err != nil {
		sh.mu.Unlock()
		return 0, err
	}
	e, exists = sh.data[key]
	if exists && e.valueType() != TypeHash {
		sh.mu.Unlock()
		return 0, ErrWrongType
	}

	var added int64
	if !exists {
		h := make(map[string]string, len(fields))
		for _, fv := range fields {
			if _, ok := h[fv[0]]; !ok {
				added++
			}
			h[fv[0]] = fv[1]
		}
		ne := &entry{
			key: key, typ: TypeHash, hash: h, size: memSizeHash(key, h),
			freq: 1, rndIdx: -1,
		}
		if err := db.ensureSpace(sh, si, key, ne.size, ne.size, false); err != nil {
			sh.mu.Unlock()
			return 0, err
		}
		db.insertNewEntry(sh, ne)
		sh.mu.Unlock()
		db.afterInsertGlobal(ne)
		for _, fv := range fields {
			db.emit(Mutation{Op: "HSET", Key: key, Field: fv[0], Value: fv[1]})
		}
		return added, nil
	}

	oldSize := e.size
	for _, fv := range fields {
		if _, ok := e.hash[fv[0]]; !ok {
			added++
		}
		e.hash[fv[0]] = fv[1]
	}
	newSize := memSizeHash(key, e.hash)
	var delta uint64
	if newSize > oldSize {
		delta = newSize - oldSize
	}
	if delta > 0 {
		if err := db.ensureSpace(sh, si, key, delta, newSize, true); err != nil {
			// rollback not attempted; best-effort
			sh.mu.Unlock()
			return 0, err
		}
		e, exists = sh.data[key]
		if !exists || e.valueType() != TypeHash {
			sh.mu.Unlock()
			return 0, ErrWrongType
		}
		// re-apply in case of race (fields already applied if no unlock path... ensureSpace may unlock)
		// After ensureSpace unlock, re-set fields.
		for _, fv := range fields {
			if e.hash == nil {
				e.hash = make(map[string]string)
			}
			e.hash[fv[0]] = fv[1]
		}
		newSize = memSizeHash(key, e.hash)
	}
	db.adjustEntrySize(sh, e, newSize)
	db.touch(sh, e)
	needGlobal := db.cfg.Strategy == StrategyGlobalTrack
	sh.mu.Unlock()
	if needGlobal {
		db.gMu.Lock()
		if db.stillInGlobal(e) {
			db.globalTouch(e)
		}
		db.gMu.Unlock()
	}
	for _, fv := range fields {
		db.emit(Mutation{Op: "HSET", Key: key, Field: fv[0], Value: fv[1]})
	}
	return added, nil
}

// HGet returns field value.
func (db *DB) HGet(key, field string) (string, bool, error) {
	si := db.shardIndex(key)
	sh := db.shards[si]
	now := time.Now()
	sh.mu.Lock()
	e, ok := sh.data[key]
	if !ok {
		sh.mu.Unlock()
		return "", false, nil
	}
	if e.expired(now) {
		db.removeFromShard(sh, e, false)
		sh.mu.Unlock()
		db.clearExpiry(key)
		return "", false, nil
	}
	if e.valueType() != TypeHash {
		sh.mu.Unlock()
		return "", false, ErrWrongType
	}
	v, ok := e.hash[field]
	db.touch(sh, e)
	needGlobal := db.cfg.Strategy == StrategyGlobalTrack
	sh.mu.Unlock()
	if needGlobal {
		db.gMu.Lock()
		if db.stillInGlobal(e) {
			db.globalTouch(e)
		}
		db.gMu.Unlock()
	}
	return v, ok, nil
}

// HMGet returns values for fields (null/missing as ok=false).
func (db *DB) HMGet(key string, fields []string) ([]string, []bool, error) {
	vals := make([]string, len(fields))
	oks := make([]bool, len(fields))
	si := db.shardIndex(key)
	sh := db.shards[si]
	now := time.Now()
	sh.mu.Lock()
	e, ok := sh.data[key]
	if !ok {
		sh.mu.Unlock()
		return vals, oks, nil
	}
	if e.expired(now) {
		db.removeFromShard(sh, e, false)
		sh.mu.Unlock()
		db.clearExpiry(key)
		return vals, oks, nil
	}
	if e.valueType() != TypeHash {
		sh.mu.Unlock()
		return nil, nil, ErrWrongType
	}
	for i, f := range fields {
		if v, found := e.hash[f]; found {
			vals[i] = v
			oks[i] = true
		}
	}
	db.touch(sh, e)
	needGlobal := db.cfg.Strategy == StrategyGlobalTrack
	sh.mu.Unlock()
	if needGlobal {
		db.gMu.Lock()
		if db.stillInGlobal(e) {
			db.globalTouch(e)
		}
		db.gMu.Unlock()
	}
	return vals, oks, nil
}

// HDel removes fields; returns count removed.
func (db *DB) HDel(key string, fields []string) (int64, error) {
	si := db.shardIndex(key)
	sh := db.shards[si]
	now := time.Now()
	sh.mu.Lock()
	e, ok := sh.data[key]
	if !ok {
		sh.mu.Unlock()
		return 0, nil
	}
	if e.expired(now) {
		db.removeFromShard(sh, e, false)
		sh.mu.Unlock()
		db.clearExpiry(key)
		return 0, nil
	}
	if e.valueType() != TypeHash {
		sh.mu.Unlock()
		return 0, ErrWrongType
	}
	var n int64
	for _, f := range fields {
		if _, found := e.hash[f]; found {
			delete(e.hash, f)
			n++
		}
	}
	if n == 0 {
		sh.mu.Unlock()
		return 0, nil
	}
	if len(e.hash) == 0 {
		db.removeFromShard(sh, e, false)
		sh.mu.Unlock()
		db.clearExpiry(key)
		db.emit(Mutation{Op: "HDEL", Key: key, Args: fields})
		return n, nil
	}
	db.adjustEntrySize(sh, e, memSizeHash(key, e.hash))
	db.touch(sh, e)
	sh.mu.Unlock()
	db.emit(Mutation{Op: "HDEL", Key: key, Args: fields})
	return n, nil
}

// HGetAll returns flat field/value pairs.
func (db *DB) HGetAll(key string) ([]string, error) {
	si := db.shardIndex(key)
	sh := db.shards[si]
	now := time.Now()
	sh.mu.Lock()
	e, ok := sh.data[key]
	if !ok {
		sh.mu.Unlock()
		return nil, nil
	}
	if e.expired(now) {
		db.removeFromShard(sh, e, false)
		sh.mu.Unlock()
		db.clearExpiry(key)
		return nil, nil
	}
	if e.valueType() != TypeHash {
		sh.mu.Unlock()
		return nil, ErrWrongType
	}
	out := make([]string, 0, len(e.hash)*2)
	for f, v := range e.hash {
		out = append(out, f, v)
	}
	db.touch(sh, e)
	sh.mu.Unlock()
	return out, nil
}

// HExists returns 1 if field exists.
func (db *DB) HExists(key, field string) (int64, error) {
	si := db.shardIndex(key)
	sh := db.shards[si]
	now := time.Now()
	sh.mu.Lock()
	e, ok := sh.data[key]
	if !ok {
		sh.mu.Unlock()
		return 0, nil
	}
	if e.expired(now) {
		db.removeFromShard(sh, e, false)
		sh.mu.Unlock()
		db.clearExpiry(key)
		return 0, nil
	}
	if e.valueType() != TypeHash {
		sh.mu.Unlock()
		return 0, ErrWrongType
	}
	_, found := e.hash[field]
	db.touch(sh, e)
	sh.mu.Unlock()
	if found {
		return 1, nil
	}
	return 0, nil
}

// HIncrBy increments field by delta (int64); creates 0 base.
func (db *DB) HIncrBy(key, field string, delta int64) (int64, error) {
	si := db.shardIndex(key)
	sh := db.shards[si]
	now := time.Now()
	sh.mu.Lock()
	if e, ok := sh.data[key]; ok && e.expired(now) {
		db.removeFromShard(sh, e, false)
		sh.mu.Unlock()
		db.clearExpiry(key)
		sh.mu.Lock()
	}
	e, exists := sh.data[key]
	if exists && e.valueType() != TypeHash {
		sh.mu.Unlock()
		return 0, ErrWrongType
	}
	var cur int64
	if exists {
		if s, ok := e.hash[field]; ok {
			// parse
			var err error
			cur, err = parseInt64(s)
			if err != nil {
				sh.mu.Unlock()
				return 0, err
			}
		}
	}
	// overflow check
	n, ok := addInt64(cur, delta)
	if !ok {
		sh.mu.Unlock()
		return 0, errIntOverflow
	}
	val := formatInt64(n)
	if !exists {
		h := map[string]string{field: val}
		ne := &entry{key: key, typ: TypeHash, hash: h, size: memSizeHash(key, h), freq: 1, rndIdx: -1}
		if err := db.ensureSpace(sh, si, key, ne.size, ne.size, false); err != nil {
			sh.mu.Unlock()
			return 0, err
		}
		db.insertNewEntry(sh, ne)
		sh.mu.Unlock()
		db.afterInsertGlobal(ne)
		db.emit(Mutation{Op: "HINCRBY", Key: key, Field: field, Value: val})
		return n, nil
	}
	if e.hash == nil {
		e.hash = make(map[string]string)
	}
	oldSize := e.size
	e.hash[field] = val
	newSize := memSizeHash(key, e.hash)
	if newSize > oldSize {
		if err := db.ensureSpace(sh, si, key, newSize-oldSize, newSize, true); err != nil {
			sh.mu.Unlock()
			return 0, err
		}
		e = sh.data[key]
		if e == nil || e.valueType() != TypeHash {
			sh.mu.Unlock()
			return 0, ErrWrongType
		}
		e.hash[field] = val
		newSize = memSizeHash(key, e.hash)
	}
	db.adjustEntrySize(sh, e, newSize)
	db.touch(sh, e)
	sh.mu.Unlock()
	db.emit(Mutation{Op: "HINCRBY", Key: key, Field: field, Value: val})
	return n, nil
}
