package store

import "time"

// Record is a durable key for snapshot/AOF restore (all value types).
type Record struct {
	Key       string
	Type      string // string|hash|list|set|zset (empty = string)
	Value     string
	Hash      map[string]string
	List      []string
	Set       []string
	ZSet      []ZMember
	ExpiresAt time.Time // zero = no TTL
	Freq      int
}

// ExportAll returns a point-in-time copy of all non-expired keys.
func (db *DB) ExportAll() []Record {
	now := time.Now()
	var out []Record
	for _, sh := range db.shards {
		sh.mu.Lock()
		for _, e := range sh.data {
			if e.expired(now) {
				continue
			}
			rec := Record{
				Key:       e.key,
				Type:      e.valueType().String(),
				ExpiresAt: e.expiresAt,
				Freq:      e.freq,
			}
			switch e.valueType() {
			case TypeHash:
				rec.Hash = make(map[string]string, len(e.hash))
				for f, v := range e.hash {
					rec.Hash[f] = v
				}
			case TypeList:
				rec.List = append([]string(nil), e.list...)
			case TypeSet:
				rec.Set = make([]string, 0, len(e.set))
				for m := range e.set {
					rec.Set = append(rec.Set, m)
				}
			case TypeZSet:
				rec.ZSet = zsetSorted(e.zset)
			default:
				rec.Value = e.value
			}
			out = append(out, rec)
		}
		sh.mu.Unlock()
	}
	return out
}

// FlushDB removes all keys from this DB.
func (db *DB) FlushDB() int {
	var keys []string
	for _, sh := range db.shards {
		sh.mu.Lock()
		for k := range sh.data {
			keys = append(keys, k)
		}
		sh.mu.Unlock()
	}
	var n int
	for _, k := range keys {
		si := db.shardIndex(k)
		sh := db.shards[si]
		sh.mu.Lock()
		e, ok := sh.data[k]
		if !ok {
			sh.mu.Unlock()
			continue
		}
		db.removeFromShard(sh, e, false)
		sh.mu.Unlock()
		db.clearExpiry(k)
		n++
	}
	if n > 0 {
		db.emit(Mutation{Op: "FLUSHDB"})
	}
	return n
}

// LoadRecords replaces the dataset with records (used on startup restore).
func (db *DB) LoadRecords(recs []Record) error {
	sink := db.sink
	db.sink = nil
	defer func() { db.sink = sink }()

	db.FlushDB()
	now := time.Now()
	for _, r := range recs {
		if !r.ExpiresAt.IsZero() && !r.ExpiresAt.After(now) {
			continue
		}
		typ := r.Type
		if typ == "" {
			typ = "string"
		}
		var err error
		switch typ {
		case "string":
			opt := SetOptions{}
			if !r.ExpiresAt.IsZero() {
				opt.HasExpireAt = true
				opt.ExpireAt = r.ExpiresAt
			}
			_, err = db.Set(r.Key, r.Value, opt)
		case "hash":
			pairs := make([][2]string, 0, len(r.Hash))
			for f, v := range r.Hash {
				pairs = append(pairs, [2]string{f, v})
			}
			_, err = db.HSet(r.Key, pairs)
		case "list":
			if len(r.List) > 0 {
				_, err = db.RPush(r.Key, r.List)
			}
		case "set":
			if len(r.Set) > 0 {
				_, err = db.SAdd(r.Key, r.Set)
			}
		case "zset":
			if len(r.ZSet) > 0 {
				_, err = db.ZAdd(r.Key, r.ZSet)
			}
		default:
			opt := SetOptions{}
			if !r.ExpiresAt.IsZero() {
				opt.HasExpireAt = true
				opt.ExpireAt = r.ExpiresAt
			}
			_, err = db.Set(r.Key, r.Value, opt)
		}
		if err != nil {
			return err
		}
		if !r.ExpiresAt.IsZero() {
			db.setExpireAt(r.Key, r.ExpiresAt)
		}
		db.setFreq(r.Key, r.Freq)
	}
	return nil
}

func (db *DB) setFreq(key string, freq int) {
	if freq <= 0 {
		return
	}
	si := db.shardIndex(key)
	sh := db.shards[si]
	sh.mu.Lock()
	defer sh.mu.Unlock()
	e, ok := sh.data[key]
	if !ok {
		return
	}
	if e.inLFU {
		sh.lfuRemove(e)
		e.freq = freq
		sh.lfuAdd(e)
	} else {
		e.freq = freq
	}
}

// Mutation describes a write for AOF logging.
type Mutation struct {
	Op        string // SET, DEL, HSET, LPUSH, SADD, ZADD, ...
	Key       string
	Value     string
	Field     string
	Member    string
	Keys      []string
	Args      []string
	ExpiresAt time.Time
}

// MutationSink receives successful mutations (optional AOF hook).
type MutationSink interface {
	OnMutation(m Mutation)
}

// SetMutationSink attaches an optional sink (e.g. AOF). nil disables.
func (db *DB) SetMutationSink(s MutationSink) {
	db.sink = s
}

func (db *DB) emit(m Mutation) {
	if db.sink != nil {
		db.sink.OnMutation(m)
	}
}
