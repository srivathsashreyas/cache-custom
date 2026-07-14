package store

// Per-shard LFU in true O(1) (amortized) with unbounded frequencies:
//
//	freqMap[freq] -> freqBucket, and buckets form a DLL ordered by increasing freq.
//	Each bucket holds a DLL of entries at that frequency (head = victim / LRU-within-freq).
//
// min frequency is always lfuMin (head of the bucket DLL). No artificial freq cap.

// freqBucket groups all entries that share the same access frequency.
type freqBucket struct {
	freq       int
	head, tail *entry // same-frequency entry list
	prev, next *freqBucket
}

func (sh *shard) lfuAdd(e *entry) {
	if e.freq <= 0 {
		e.freq = 1
	}
	b := sh.lfuGetOrCreateBucket(e.freq, nil)
	b.pushTail(e)
	e.inLFU = true
	sh.lfuSize++
}

func (sh *shard) lfuRemove(e *entry) {
	if !e.inLFU || e.bucket == nil {
		e.inLFU = false
		return
	}
	b := e.bucket
	b.detach(e)
	sh.lfuSize--
	e.inLFU = false
	if b.empty() {
		sh.lfuUnlinkBucket(b)
	}
}

// lfuOnAccess records one access against e.
// Frequency always increases (access count), whether or not e is currently an
// eviction candidate in the LFU structure (e.g. no TTL under volatile-lfu).
// Bucket moves happen only while e.inLFU.
func (sh *shard) lfuOnAccess(e *entry) {
	if e.inLFU && e.bucket != nil {
		oldB := e.bucket
		oldB.detach(e)
		e.freq++
		// New bucket for freq+1 sits immediately after oldB when created (O(1) splice).
		newB := sh.lfuGetOrCreateBucket(e.freq, oldB)
		newB.pushTail(e)
		if oldB.empty() {
			sh.lfuUnlinkBucket(oldB)
		}
		return
	}
	// Not in the structure: still track true access frequency for a later re-join.
	if e.freq <= 0 {
		e.freq = 1
	}
	e.freq++
}

// lfuGetOrCreateBucket returns the bucket for freq.
// after is the previous frequency bucket when inserting freq == after.freq+1 (bump path).
func (sh *shard) lfuGetOrCreateBucket(freq int, after *freqBucket) *freqBucket {
	if b, ok := sh.freqMap[freq]; ok {
		return b
	}
	b := &freqBucket{freq: freq}
	sh.freqMap[freq] = b

	if after != nil && after.freq+1 == freq {
		// O(1): splice directly after the old frequency bucket.
		b.prev = after
		b.next = after.next
		if after.next != nil {
			after.next.prev = b
		}
		after.next = b
		return b
	}

	// General insert in increasing-freq order (used for first add at freq 1).
	if sh.lfuMin == nil {
		sh.lfuMin = b
		return b
	}
	if freq < sh.lfuMin.freq {
		b.next = sh.lfuMin
		sh.lfuMin.prev = b
		sh.lfuMin = b
		return b
	}
	// Walk forward — only when not using the after-hint; rare for initial placement.
	cur := sh.lfuMin
	for cur.next != nil && cur.next.freq < freq {
		cur = cur.next
	}
	b.prev = cur
	b.next = cur.next
	if cur.next != nil {
		cur.next.prev = b
	}
	cur.next = b
	return b
}

func (sh *shard) lfuUnlinkBucket(b *freqBucket) {
	delete(sh.freqMap, b.freq)
	if b.prev != nil {
		b.prev.next = b.next
	} else {
		sh.lfuMin = b.next
	}
	if b.next != nil {
		b.next.prev = b.prev
	}
	b.prev, b.next, b.head, b.tail = nil, nil, nil, nil
}

func (b *freqBucket) empty() bool {
	return b.head == nil
}

func (b *freqBucket) pushTail(e *entry) {
	e.bucket = b
	e.fPrev = b.tail
	e.fNext = nil
	if b.tail != nil {
		b.tail.fNext = e
	} else {
		b.head = e
	}
	b.tail = e
}

func (b *freqBucket) detach(e *entry) {
	if e.fPrev != nil {
		e.fPrev.fNext = e.fNext
	} else if b.head == e {
		b.head = e.fNext
	}
	if e.fNext != nil {
		e.fNext.fPrev = e.fPrev
	} else if b.tail == e {
		b.tail = e.fPrev
	}
	e.fPrev, e.fNext, e.bucket = nil, nil, nil
}

// lfuPick returns a victim from the lowest frequency bucket (O(1) typical).
// Within a frequency, head is least-recently touched among equals.
func (sh *shard) lfuPick(skipKey string, volatileOnly bool) *entry {
	if sh.lfuSize == 0 {
		return nil
	}
	for b := sh.lfuMin; b != nil; b = b.next {
		for e := b.head; e != nil; e = e.fNext {
			if e.key == skipKey {
				continue
			}
			if volatileOnly && e.expiresAt.IsZero() {
				continue
			}
			return e
		}
	}
	return nil
}
