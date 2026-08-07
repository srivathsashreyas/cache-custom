package store

import (
	"sort"
	"strconv"
	"strings"
	"time"
)

// zsetData holds member scores for a sorted set.
type zsetData struct {
	scores map[string]float64
}

// ZMember is a scored member for export/range.
type ZMember struct {
	Member string
	Score  float64
}

// ZAdd adds score/member pairs; returns new members count (updates don't count).
func (db *DB) ZAdd(key string, pairs []ZMember) (int64, error) {
	if len(pairs) == 0 {
		return 0, nil
	}
	si := db.shardIndex(key)
	sh := db.shards[si]
	now := time.Now()
	extra := uint64(0)
	for _, p := range pairs {
		extra += uint64(len(p.Member) + 16)
	}

	sh.mu.Lock()
	if e, ok := sh.data[key]; ok && e.expired(now) {
		db.removeFromShard(sh, e, false)
		sh.mu.Unlock()
		db.clearExpiry(key)
		sh.mu.Lock()
	}
	e, exists := sh.data[key]
	if exists && e.valueType() != TypeZSet {
		sh.mu.Unlock()
		return 0, ErrWrongType
	}

	var added int64
	if !exists {
		zs := &zsetData{scores: make(map[string]float64, len(pairs))}
		for _, p := range pairs {
			if _, ok := zs.scores[p.Member]; !ok {
				added++
			}
			zs.scores[p.Member] = p.Score
		}
		ne := &entry{key: key, typ: TypeZSet, zset: zs, size: memSizeZSet(key, zs), freq: 1, rndIdx: -1}
		if err := db.ensureSpace(sh, si, key, ne.size, ne.size, false); err != nil {
			sh.mu.Unlock()
			return 0, err
		}
		db.insertNewEntry(sh, ne)
		sh.mu.Unlock()
		db.afterInsertGlobal(ne)
		for _, p := range pairs {
			db.emit(Mutation{Op: "ZADD", Key: key, Member: p.Member, Value: formatFloat(p.Score)})
		}
		return added, nil
	}

	oldSize := e.size
	if e.zset == nil {
		e.zset = &zsetData{scores: make(map[string]float64)}
	}
	for _, p := range pairs {
		if _, ok := e.zset.scores[p.Member]; !ok {
			added++
		}
		e.zset.scores[p.Member] = p.Score
	}
	newSize := memSizeZSet(key, e.zset)
	if newSize > oldSize {
		if err := db.ensureSpace(sh, si, key, newSize-oldSize, newSize, true); err != nil {
			sh.mu.Unlock()
			return 0, err
		}
		e = sh.data[key]
		if e == nil || e.valueType() != TypeZSet {
			sh.mu.Unlock()
			return 0, ErrWrongType
		}
		newSize = memSizeZSet(key, e.zset)
	}
	db.adjustEntrySize(sh, e, newSize)
	db.touch(sh, e)
	sh.mu.Unlock()
	for _, p := range pairs {
		db.emit(Mutation{Op: "ZADD", Key: key, Member: p.Member, Value: formatFloat(p.Score)})
	}
	return added, nil
}

// ZRem removes members.
func (db *DB) ZRem(key string, members []string) (int64, error) {
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
	if e.valueType() != TypeZSet {
		sh.mu.Unlock()
		return 0, ErrWrongType
	}
	var n int64
	for _, m := range members {
		if _, found := e.zset.scores[m]; found {
			delete(e.zset.scores, m)
			n++
		}
	}
	if n == 0 {
		sh.mu.Unlock()
		return 0, nil
	}
	if len(e.zset.scores) == 0 {
		db.removeFromShard(sh, e, false)
		sh.mu.Unlock()
		db.clearExpiry(key)
		db.emit(Mutation{Op: "ZREM", Key: key, Args: members})
		return n, nil
	}
	db.adjustEntrySize(sh, e, memSizeZSet(key, e.zset))
	db.touch(sh, e)
	sh.mu.Unlock()
	db.emit(Mutation{Op: "ZREM", Key: key, Args: members})
	return n, nil
}

// ZScore returns score.
func (db *DB) ZScore(key, member string) (float64, bool, error) {
	si := db.shardIndex(key)
	sh := db.shards[si]
	now := time.Now()
	sh.mu.Lock()
	e, ok := sh.data[key]
	if !ok {
		sh.mu.Unlock()
		return 0, false, nil
	}
	if e.expired(now) {
		db.removeFromShard(sh, e, false)
		sh.mu.Unlock()
		db.clearExpiry(key)
		return 0, false, nil
	}
	if e.valueType() != TypeZSet {
		sh.mu.Unlock()
		return 0, false, ErrWrongType
	}
	sc, found := e.zset.scores[member]
	db.touch(sh, e)
	sh.mu.Unlock()
	return sc, found, nil
}

// ZRank returns 0-based rank by ascending score (then member).
func (db *DB) ZRank(key, member string) (int64, bool, error) {
	si := db.shardIndex(key)
	sh := db.shards[si]
	now := time.Now()
	sh.mu.Lock()
	e, ok := sh.data[key]
	if !ok {
		sh.mu.Unlock()
		return 0, false, nil
	}
	if e.expired(now) {
		db.removeFromShard(sh, e, false)
		sh.mu.Unlock()
		db.clearExpiry(key)
		return 0, false, nil
	}
	if e.valueType() != TypeZSet {
		sh.mu.Unlock()
		return 0, false, ErrWrongType
	}
	if _, found := e.zset.scores[member]; !found {
		sh.mu.Unlock()
		return 0, false, nil
	}
	sorted := zsetSorted(e.zset)
	var rank int64
	for i, zm := range sorted {
		if zm.Member == member {
			rank = int64(i)
			break
		}
	}
	db.touch(sh, e)
	sh.mu.Unlock()
	return rank, true, nil
}

// ZIncrBy increments member score.
func (db *DB) ZIncrBy(key string, incr float64, member string) (float64, error) {
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
	if exists && e.valueType() != TypeZSet {
		sh.mu.Unlock()
		return 0, ErrWrongType
	}
	var cur float64
	if exists {
		cur = e.zset.scores[member]
	}
	n := cur + incr
	if !exists {
		zs := &zsetData{scores: map[string]float64{member: n}}
		ne := &entry{key: key, typ: TypeZSet, zset: zs, size: memSizeZSet(key, zs), freq: 1, rndIdx: -1}
		if err := db.ensureSpace(sh, si, key, ne.size, ne.size, false); err != nil {
			sh.mu.Unlock()
			return 0, err
		}
		db.insertNewEntry(sh, ne)
		sh.mu.Unlock()
		db.afterInsertGlobal(ne)
		db.emit(Mutation{Op: "ZINCRBY", Key: key, Member: member, Value: formatFloat(n)})
		return n, nil
	}
	oldSize := e.size
	e.zset.scores[member] = n
	newSize := memSizeZSet(key, e.zset)
	if newSize > oldSize {
		if err := db.ensureSpace(sh, si, key, newSize-oldSize, newSize, true); err != nil {
			sh.mu.Unlock()
			return 0, err
		}
		e = sh.data[key]
		if e == nil || e.valueType() != TypeZSet {
			sh.mu.Unlock()
			return 0, ErrWrongType
		}
		e.zset.scores[member] = n
		newSize = memSizeZSet(key, e.zset)
	}
	db.adjustEntrySize(sh, e, newSize)
	db.touch(sh, e)
	sh.mu.Unlock()
	db.emit(Mutation{Op: "ZINCRBY", Key: key, Member: member, Value: formatFloat(n)})
	return n, nil
}

// ZRange by rank with optional scores.
func (db *DB) ZRange(key string, start, stop int64, withScores bool) ([]ZMember, error) {
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
	if e.valueType() != TypeZSet {
		sh.mu.Unlock()
		return nil, ErrWrongType
	}
	sorted := zsetSorted(e.zset)
	n := int64(len(sorted))
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
	out := append([]ZMember(nil), sorted[start:stop+1]...)
	db.touch(sh, e)
	sh.mu.Unlock()
	_ = withScores // caller formats
	return out, nil
}

// ZRangeByScore returns members with min<=score<=max (exclusive bounds with '(').
func (db *DB) ZRangeByScore(key, minS, maxS string, withScores bool, limitOffset, limitCount int64) ([]ZMember, error) {
	min, minEx, err := parseScoreBound(minS, true)
	if err != nil {
		return nil, err
	}
	max, maxEx, err := parseScoreBound(maxS, false)
	if err != nil {
		return nil, err
	}
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
	if e.valueType() != TypeZSet {
		sh.mu.Unlock()
		return nil, ErrWrongType
	}
	sorted := zsetSorted(e.zset)
	var out []ZMember
	for _, zm := range sorted {
		if minEx {
			if !(zm.Score > min) {
				continue
			}
		} else if zm.Score < min {
			continue
		}
		if maxEx {
			if !(zm.Score < max) {
				continue
			}
		} else if zm.Score > max {
			continue
		}
		out = append(out, zm)
	}
	if limitCount >= 0 {
		if limitOffset < 0 {
			limitOffset = 0
		}
		if int64(len(out)) > limitOffset {
			out = out[limitOffset:]
			if limitCount > 0 && int64(len(out)) > limitCount {
				out = out[:limitCount]
			}
		} else {
			out = nil
		}
	}
	db.touch(sh, e)
	sh.mu.Unlock()
	_ = withScores
	return out, nil
}

func zsetSorted(zs *zsetData) []ZMember {
	if zs == nil || len(zs.scores) == 0 {
		return nil
	}
	out := make([]ZMember, 0, len(zs.scores))
	for m, sc := range zs.scores {
		out = append(out, ZMember{Member: m, Score: sc})
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Score != out[j].Score {
			return out[i].Score < out[j].Score
		}
		return out[i].Member < out[j].Member
	})
	return out
}

func formatFloat(f float64) string {
	return strconv.FormatFloat(f, 'f', -1, 64)
}

// parseScoreBound parses Redis score range tokens like 1, (1, -inf, +inf.
func parseScoreBound(s string, isMin bool) (float64, bool, error) {
	_ = isMin
	exclusive := false
	if strings.HasPrefix(s, "(") {
		exclusive = true
		s = s[1:]
	}
	f, err := strconv.ParseFloat(s, 64)
	if err != nil {
		return 0, false, err
	}
	return f, exclusive, nil
}
