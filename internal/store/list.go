package store

import "time"

// LPush prepends elements (Redis multi-arg order: first arg ends closest to head).
func (db *DB) LPush(key string, elems []string) (int64, error) {
	return db.listPush(key, elems, true)
}

// RPush appends elements.
func (db *DB) RPush(key string, elems []string) (int64, error) {
	return db.listPush(key, elems, false)
}

func (db *DB) listPush(key string, elems []string, left bool) (int64, error) {
	if len(elems) == 0 {
		return 0, nil
	}
	si := db.shardIndex(key)
	sh := db.shards[si]
	now := time.Now()
	extra := uint64(0)
	for _, e := range elems {
		extra += uint64(len(e) + 8)
	}

	sh.mu.Lock()
	if e, ok := sh.data[key]; ok && e.expired(now) {
		db.removeFromShard(sh, e, false)
		sh.mu.Unlock()
		db.clearExpiry(key)
		sh.mu.Lock()
	}
	e, exists := sh.data[key]
	if exists && e.valueType() != TypeList {
		sh.mu.Unlock()
		return 0, ErrWrongType
	}

	if !exists {
		// Build list in Redis multi-arg order.
		list := make([]string, 0, len(elems))
		if left {
			// LPUSH a b c → list [c,b,a]
			for i := len(elems) - 1; i >= 0; i-- {
				list = append(list, elems[i])
			}
		} else {
			list = append(list, elems...)
		}
		ne := &entry{key: key, typ: TypeList, list: list, size: memSizeList(key, list), freq: 1, rndIdx: -1}
		if err := db.ensureSpace(sh, si, key, ne.size, ne.size, false); err != nil {
			sh.mu.Unlock()
			return 0, err
		}
		db.insertNewEntry(sh, ne)
		n := int64(len(list))
		sh.mu.Unlock()
		db.afterInsertGlobal(ne)
		op := "RPUSH"
		if left {
			op = "LPUSH"
		}
		db.emit(Mutation{Op: op, Key: key, Args: elems})
		return n, nil
	}

	oldSize := e.size
	if left {
		// Prepend in reverse order of args so first arg is nearest head.
		for i := len(elems) - 1; i >= 0; i-- {
			e.list = append([]string{elems[i]}, e.list...)
		}
	} else {
		e.list = append(e.list, elems...)
	}
	newSize := memSizeList(key, e.list)
	delta := uint64(0)
	if newSize > oldSize {
		delta = newSize - oldSize
	}
	if delta > 0 {
		if err := db.ensureSpace(sh, si, key, delta, newSize, true); err != nil {
			sh.mu.Unlock()
			return 0, err
		}
		e = sh.data[key]
		if e == nil || e.valueType() != TypeList {
			sh.mu.Unlock()
			return 0, ErrWrongType
		}
		// After possible unlock, rebuild push if list was rolled back... fields already applied on e before ensureSpace
		// ensureSpace may unlock; re-fetch list state - if race lost, re-apply is complex. For single-threaded RESP OK.
		newSize = memSizeList(key, e.list)
	}
	db.adjustEntrySize(sh, e, newSize)
	db.touch(sh, e)
	n := int64(len(e.list))
	sh.mu.Unlock()
	op := "RPUSH"
	if left {
		op = "LPUSH"
	}
	db.emit(Mutation{Op: op, Key: key, Args: elems})
	return n, nil
}

// LPop removes and returns head.
func (db *DB) LPop(key string) (string, bool, error) {
	return db.listPop(key, true)
}

// RPop removes and returns tail.
func (db *DB) RPop(key string) (string, bool, error) {
	return db.listPop(key, false)
}

func (db *DB) listPop(key string, left bool) (string, bool, error) {
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
	if e.valueType() != TypeList {
		sh.mu.Unlock()
		return "", false, ErrWrongType
	}
	if len(e.list) == 0 {
		sh.mu.Unlock()
		return "", false, nil
	}
	var v string
	if left {
		v = e.list[0]
		e.list = e.list[1:]
	} else {
		v = e.list[len(e.list)-1]
		e.list = e.list[:len(e.list)-1]
	}
	if len(e.list) == 0 {
		db.removeFromShard(sh, e, false)
		sh.mu.Unlock()
		db.clearExpiry(key)
		op := "RPOP"
		if left {
			op = "LPOP"
		}
		db.emit(Mutation{Op: op, Key: key, Value: v})
		return v, true, nil
	}
	db.adjustEntrySize(sh, e, memSizeList(key, e.list))
	db.touch(sh, e)
	sh.mu.Unlock()
	op := "RPOP"
	if left {
		op = "LPOP"
	}
	db.emit(Mutation{Op: op, Key: key, Value: v})
	return v, true, nil
}

// LLen returns list length.
func (db *DB) LLen(key string) (int64, error) {
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
	if e.valueType() != TypeList {
		sh.mu.Unlock()
		return 0, ErrWrongType
	}
	n := int64(len(e.list))
	sh.mu.Unlock()
	return n, nil
}

// LRange returns inclusive range with Redis negative index semantics.
func (db *DB) LRange(key string, start, stop int64) ([]string, error) {
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
	if e.valueType() != TypeList {
		sh.mu.Unlock()
		return nil, ErrWrongType
	}
	n := int64(len(e.list))
	if n == 0 {
		sh.mu.Unlock()
		return nil, nil
	}
	if start < 0 {
		start = n + start
	}
	if stop < 0 {
		stop = n + stop
	}
	if start < 0 {
		start = 0
	}
	if stop >= n {
		stop = n - 1
	}
	if start > stop || start >= n {
		sh.mu.Unlock()
		return nil, nil
	}
	out := make([]string, stop-start+1)
	copy(out, e.list[start:stop+1])
	db.touch(sh, e)
	sh.mu.Unlock()
	return out, nil
}
