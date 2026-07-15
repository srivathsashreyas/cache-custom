package store

// listKind selects which doubly-linked list an entry participates in.
type listKind int

const (
	listLocalLRU listKind = iota // recency (always updated on access)
	listLocalFIFO                // insertion order (never reordered on access)
	listGlobalLRU
	listGlobalFIFO
)

// dllRemove unlinks e from the list rooted at head/tail using the link pair selected by kind.
func dllRemove(e *entry, kind listKind, head, tail **entry) {
	p, n := e.getLinks(kind)
	prev, next := *p, *n
	if prev != nil {
		_, prevNext := prev.getLinks(kind)
		*prevNext = next
	} else {
		*head = next
	}
	if next != nil {
		nextPrev, _ := next.getLinks(kind)
		*nextPrev = prev
	} else {
		*tail = prev
	}
	*p, *n = nil, nil
}

func dllPushTail(e *entry, kind listKind, head, tail **entry) {
	p, n := e.getLinks(kind)
	*p = *tail
	*n = nil
	if *tail != nil {
		_, tailNext := (*tail).getLinks(kind)
		*tailNext = e
	} else {
		*head = e
	}
	*tail = e
}

func dllTouch(e *entry, kind listKind, head, tail **entry) {
	dllRemove(e, kind, head, tail)
	dllPushTail(e, kind, head, tail)
}

func (e *entry) getLinks(kind listKind) (prev, next **entry) {
	switch kind {
	case listLocalLRU:
		return &e.lPrev, &e.lNext
	case listLocalFIFO:
		return &e.iPrev, &e.iNext
	case listGlobalLRU:
		return &e.gPrev, &e.gNext
	default: // listGlobalFIFO
		return &e.giPrev, &e.giNext
	}
}

func (sh *shard) lruRemove(e *entry)  { dllRemove(e, listLocalLRU, &sh.head, &sh.tail) }
func (sh *shard) lruPushTail(e *entry) { dllPushTail(e, listLocalLRU, &sh.head, &sh.tail) }
func (sh *shard) lruTouch(e *entry)    { dllTouch(e, listLocalLRU, &sh.head, &sh.tail) }

func (sh *shard) fifoRemove(e *entry)  { dllRemove(e, listLocalFIFO, &sh.fifoHead, &sh.fifoTail) }
func (sh *shard) fifoPushTail(e *entry) { dllPushTail(e, listLocalFIFO, &sh.fifoHead, &sh.fifoTail) }

// global* require db.gMu held.
func (db *DB) globalRemove(e *entry) { dllRemove(e, listGlobalLRU, &db.gHead, &db.gTail) }
func (db *DB) globalPushTail(e *entry) {
	dllPushTail(e, listGlobalLRU, &db.gHead, &db.gTail)
}
func (db *DB) globalTouch(e *entry) { dllTouch(e, listGlobalLRU, &db.gHead, &db.gTail) }

func (db *DB) globalFifoRemove(e *entry) {
	dllRemove(e, listGlobalFIFO, &db.gFifoHead, &db.gFifoTail)
}
func (db *DB) globalFifoPushTail(e *entry) {
	dllPushTail(e, listGlobalFIFO, &db.gFifoHead, &db.gFifoTail)
}
