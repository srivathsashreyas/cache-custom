package store

import "time"

// Record is a durable string key for snapshot/AOF restore.
type Record struct {
	Key       string
	Value     string
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
			out = append(out, Record{
				Key:       e.key,
				Value:     e.value,
				ExpiresAt: e.expiresAt,
				Freq:      e.freq,
			})
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
		// Use internal delete path that still emits DEL — for FlushDB we want one op.
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
	// Suppress AOF while loading.
	sink := db.sink
	db.sink = nil
	defer func() { db.sink = sink }()

	db.FlushDB()
	now := time.Now()
	for _, r := range recs {
		if !r.ExpiresAt.IsZero() && !r.ExpiresAt.After(now) {
			continue
		}
		opt := SetOptions{}
		if !r.ExpiresAt.IsZero() {
			opt.HasExpireAt = true
			opt.ExpireAt = r.ExpiresAt
		}
		if _, err := db.Set(r.Key, r.Value, opt); err != nil {
			return err
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
	Op        string // SET, DEL, EXPIRE, PERSIST, FLUSHDB
	Key       string
	Value     string
	Keys      []string
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
