package store

// Per-shard LFU in O(1) amortized: frequency buckets are doubly-linked lists.
// Evict = head of the minimum non-empty frequency list (skip ineligible keys).

func (sh *shard) lfuAdd(e *entry) {
	if e.freq == 0 {
		e.freq = 5
	}
	sh.lfuAttach(e)
	if sh.lfuSize == 0 || e.freq < sh.lfuMin {
		sh.lfuMin = e.freq
	}
	sh.lfuSize++
}

func (sh *shard) lfuRemove(e *entry) {
	if !e.inLFU {
		return
	}
	sh.lfuDetach(e)
	sh.lfuSize--
	e.inLFU = false
	if sh.lfuSize == 0 {
		sh.lfuMin = 0
		return
	}
	// Advance min if this frequency bucket emptied (at most 256 steps → O(1)).
	for sh.lfuMin < 255 && sh.lfu[sh.lfuMin] == nil {
		sh.lfuMin++
	}
}

func (sh *shard) lfuBump(e *entry) {
	if !e.inLFU {
		return
	}
	old := e.freq
	sh.lfuDetach(e)
	if e.freq < 255 {
		e.freq++
	}
	sh.lfuAttach(e)
	if old == sh.lfuMin && sh.lfu[old] == nil {
		for sh.lfuMin < 255 && sh.lfu[sh.lfuMin] == nil {
			sh.lfuMin++
		}
	}
}

func (sh *shard) lfuAttach(e *entry) {
	head := sh.lfu[e.freq]
	e.fPrev, e.fNext = nil, head
	if head != nil {
		head.fPrev = e
	}
	sh.lfu[e.freq] = e
	e.inLFU = true
}

func (sh *shard) lfuDetach(e *entry) {
	if e.fPrev != nil {
		e.fPrev.fNext = e.fNext
	} else if sh.lfu[e.freq] == e {
		sh.lfu[e.freq] = e.fNext
	}
	if e.fNext != nil {
		e.fNext.fPrev = e.fPrev
	}
	if sh.lfu[e.freq] == nil {
		delete(sh.lfu, e.freq)
	}
	e.fPrev, e.fNext = nil, nil
}

// lfuPick returns an O(1)-expected victim from frequency lists (sh.mu held).
func (sh *shard) lfuPick(skipKey string, volatileOnly bool) *entry {
	if sh.lfuSize == 0 {
		return nil
	}
	for f := int(sh.lfuMin); f <= 255; f++ {
		for e := sh.lfu[uint8(f)]; e != nil; e = e.fNext {
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
