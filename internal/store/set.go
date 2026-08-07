package store

import "time"

// SAdd adds members; returns count newly added.
func (db *DB) SAdd(key string, members []string) (int64, error) {
	if len(members) == 0 {
		return 0, nil
	}
	si := db.shardIndex(key)
	sh := db.shards[si]
	now := time.Now()
	extra := uint64(0)
	for _, m := range members {
		extra += uint64(len(m) + 8)
	}

	sh.mu.Lock()
	if e, ok := sh.data[key]; ok && e.expired(now) {
		db.removeFromShard(sh, e, false)
		sh.mu.Unlock()
		db.clearExpiry(key)
		sh.mu.Lock()
	}
	e, exists := sh.data[key]
	if exists && e.valueType() != TypeSet {
		sh.mu.Unlock()
		return 0, ErrWrongType
	}

	var added int64
	if !exists {
		s := make(map[string]struct{}, len(members))
		for _, m := range members {
			if _, ok := s[m]; !ok {
				s[m] = struct{}{}
				added++
			}
		}
		ne := &entry{key: key, typ: TypeSet, set: s, size: memSizeSet(key, s), freq: 1, rndIdx: -1}
		if err := db.ensureSpace(sh, si, key, ne.size, ne.size, false); err != nil {
			sh.mu.Unlock()
			return 0, err
		}
		db.insertNewEntry(sh, ne)
		sh.mu.Unlock()
		db.afterInsertGlobal(ne)
		db.emit(Mutation{Op: "SADD", Key: key, Args: members})
		return added, nil
	}

	oldSize := e.size
	for _, m := range members {
		if _, ok := e.set[m]; !ok {
			e.set[m] = struct{}{}
			added++
		}
	}
	if added == 0 {
		sh.mu.Unlock()
		return 0, nil
	}
	newSize := memSizeSet(key, e.set)
	if newSize > oldSize {
		if err := db.ensureSpace(sh, si, key, newSize-oldSize, newSize, true); err != nil {
			sh.mu.Unlock()
			return 0, err
		}
		e = sh.data[key]
		if e == nil || e.valueType() != TypeSet {
			sh.mu.Unlock()
			return 0, ErrWrongType
		}
		newSize = memSizeSet(key, e.set)
	}
	db.adjustEntrySize(sh, e, newSize)
	db.touch(sh, e)
	sh.mu.Unlock()
	db.emit(Mutation{Op: "SADD", Key: key, Args: members})
	return added, nil
}

// SRem removes members.
func (db *DB) SRem(key string, members []string) (int64, error) {
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
	if e.valueType() != TypeSet {
		sh.mu.Unlock()
		return 0, ErrWrongType
	}
	var n int64
	for _, m := range members {
		if _, found := e.set[m]; found {
			delete(e.set, m)
			n++
		}
	}
	if n == 0 {
		sh.mu.Unlock()
		return 0, nil
	}
	if len(e.set) == 0 {
		db.removeFromShard(sh, e, false)
		sh.mu.Unlock()
		db.clearExpiry(key)
		db.emit(Mutation{Op: "SREM", Key: key, Args: members})
		return n, nil
	}
	db.adjustEntrySize(sh, e, memSizeSet(key, e.set))
	db.touch(sh, e)
	sh.mu.Unlock()
	db.emit(Mutation{Op: "SREM", Key: key, Args: members})
	return n, nil
}

// SIsMember returns 1 if member present.
func (db *DB) SIsMember(key, member string) (int64, error) {
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
	if e.valueType() != TypeSet {
		sh.mu.Unlock()
		return 0, ErrWrongType
	}
	_, found := e.set[member]
	db.touch(sh, e)
	sh.mu.Unlock()
	if found {
		return 1, nil
	}
	return 0, nil
}

// SMembers returns all members.
func (db *DB) SMembers(key string) ([]string, error) {
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
	if e.valueType() != TypeSet {
		sh.mu.Unlock()
		return nil, ErrWrongType
	}
	out := make([]string, 0, len(e.set))
	for m := range e.set {
		out = append(out, m)
	}
	db.touch(sh, e)
	sh.mu.Unlock()
	return out, nil
}

// SCard returns cardinality.
func (db *DB) SCard(key string) (int64, error) {
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
	if e.valueType() != TypeSet {
		sh.mu.Unlock()
		return 0, ErrWrongType
	}
	n := int64(len(e.set))
	sh.mu.Unlock()
	return n, nil
}

// loadSetMap returns a copy of set members for key (empty if missing). Wrong type → error.
func (db *DB) loadSetMap(key string) (map[string]struct{}, error) {
	si := db.shardIndex(key)
	sh := db.shards[si]
	now := time.Now()
	sh.mu.Lock()
	e, ok := sh.data[key]
	if !ok {
		sh.mu.Unlock()
		return map[string]struct{}{}, nil
	}
	if e.expired(now) {
		db.removeFromShard(sh, e, false)
		sh.mu.Unlock()
		db.clearExpiry(key)
		return map[string]struct{}{}, nil
	}
	if e.valueType() != TypeSet {
		sh.mu.Unlock()
		return nil, ErrWrongType
	}
	out := make(map[string]struct{}, len(e.set))
	for m := range e.set {
		out[m] = struct{}{}
	}
	sh.mu.Unlock()
	return out, nil
}

// SInter intersection of keys.
// All keys are type-checked first so a WRONGTYPE key later in the arg list
// is still reported even if the running intersection is already empty.
func (db *DB) SInter(keys []string) ([]string, error) {
	if len(keys) == 0 {
		return nil, nil
	}
	maps := make([]map[string]struct{}, len(keys))
	for i, k := range keys {
		m, err := db.loadSetMap(k)
		if err != nil {
			return nil, err
		}
		maps[i] = m
	}
	// Copy first set so we can shrink it.
	base := make(map[string]struct{}, len(maps[0]))
	for m := range maps[0] {
		base[m] = struct{}{}
	}
	for i := 1; i < len(maps); i++ {
		if len(base) == 0 {
			break
		}
		other := maps[i]
		for m := range base {
			if _, ok := other[m]; !ok {
				delete(base, m)
			}
		}
	}
	out := make([]string, 0, len(base))
	for m := range base {
		out = append(out, m)
	}
	return out, nil
}

// SUnion union of keys.
func (db *DB) SUnion(keys []string) ([]string, error) {
	out := make(map[string]struct{})
	for _, k := range keys {
		s, err := db.loadSetMap(k)
		if err != nil {
			return nil, err
		}
		for m := range s {
			out[m] = struct{}{}
		}
	}
	res := make([]string, 0, len(out))
	for m := range out {
		res = append(res, m)
	}
	return res, nil
}

// SDiff first set minus the rest.
func (db *DB) SDiff(keys []string) ([]string, error) {
	if len(keys) == 0 {
		return nil, nil
	}
	base, err := db.loadSetMap(keys[0])
	if err != nil {
		return nil, err
	}
	for _, k := range keys[1:] {
		other, err := db.loadSetMap(k)
		if err != nil {
			return nil, err
		}
		for m := range other {
			delete(base, m)
		}
	}
	out := make([]string, 0, len(base))
	for m := range base {
		out = append(out, m)
	}
	return out, nil
}
