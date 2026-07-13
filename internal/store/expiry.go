package store

import "time"

type expItem struct {
	key string
	at  time.Time
}

func (db *DB) expiryLoop() {
	defer db.wg.Done()
	t := time.NewTicker(db.cfg.ExpiryInterval)
	defer t.Stop()
	for {
		select {
		case <-db.stop:
			return
		case <-t.C:
			db.expireDue(time.Now(), 256)
		}
	}
}

// expireDue removes up to limit keys whose TTL has passed (periodic active expiry).
func (db *DB) expireDue(now time.Time, limit int) int {
	removed := 0
	for removed < limit {
		db.expMu.Lock()
		if len(db.expH) == 0 || db.expH[0].at.After(now) {
			db.expMu.Unlock()
			break
		}
		item := db.expH[0]
		db.expMu.Unlock()

		// DeleteKey re-checks expiry under shard lock (avoids racing a TTL update).
		if db.deleteIfExpired(item.key, now) {
			removed++
			continue
		}
		// Stale heap entry (TTL changed or key already gone).
		db.expMu.Lock()
		if idx, ok := db.expIdx[item.key]; ok {
			if idx < len(db.expH) && db.expH[idx].key == item.key && db.expH[idx].at.Equal(item.at) {
				db.heapRemoveAt(idx)
			}
		}
		db.expMu.Unlock()
	}
	return removed
}

func (db *DB) deleteIfExpired(key string, now time.Time) bool {
	si := db.shardIndex(key)
	sh := db.shards[si]

	sh.mu.Lock()
	e, ok := sh.data[key]
	if !ok || !e.expired(now) {
		sh.mu.Unlock()
		return false
	}
	db.removeFromShard(sh, e, false)
	sh.mu.Unlock()
	db.clearExpiry(key)
	return true
}

// noteExpiry updates the expiry heap; call without expMu held.
func (db *DB) noteExpiry(key string, at time.Time) {
	db.expMu.Lock()
	defer db.expMu.Unlock()
	if idx, ok := db.expIdx[key]; ok {
		db.heapRemoveAt(idx)
	}
	if at.IsZero() {
		return
	}
	db.heapPush(expItem{key: key, at: at})
}

func (db *DB) clearExpiry(key string) {
	db.expMu.Lock()
	defer db.expMu.Unlock()
	if idx, ok := db.expIdx[key]; ok {
		db.heapRemoveAt(idx)
	}
}

func (db *DB) heapPush(it expItem) {
	db.expH = append(db.expH, it)
	db.expIdx[it.key] = len(db.expH) - 1
	db.siftUp(len(db.expH) - 1)
}

func (db *DB) heapRemoveAt(i int) {
	n := len(db.expH) - 1
	key := db.expH[i].key
	db.swapExp(i, n)
	db.expH = db.expH[:n]
	delete(db.expIdx, key)
	if i < n {
		db.siftDown(i)
		db.siftUp(i)
	}
}

func (db *DB) siftUp(i int) {
	for i > 0 {
		p := (i - 1) / 2
		if !db.expH[i].at.Before(db.expH[p].at) {
			break
		}
		db.swapExp(i, p)
		i = p
	}
}

func (db *DB) siftDown(i int) {
	n := len(db.expH)
	for {
		l, r, smallest := 2*i+1, 2*i+2, i
		if l < n && db.expH[l].at.Before(db.expH[smallest].at) {
			smallest = l
		}
		if r < n && db.expH[r].at.Before(db.expH[smallest].at) {
			smallest = r
		}
		if smallest == i {
			return
		}
		db.swapExp(i, smallest)
		i = smallest
	}
}

func (db *DB) swapExp(i, j int) {
	db.expH[i], db.expH[j] = db.expH[j], db.expH[i]
	db.expIdx[db.expH[i].key] = i
	db.expIdx[db.expH[j].key] = j
}
