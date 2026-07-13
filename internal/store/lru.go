package store

// listKind selects which doubly-linked list an entry participates in.
type listKind int

const (
	listLocal  listKind = iota // per-shard LRU
	listGlobal                 // tenant-wide LRU (strategy 1)
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

// dllPushTail inserts e as the most-recently-used node.
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
	if kind == listLocal {
		return &e.lPrev, &e.lNext
	}
	return &e.gPrev, &e.gNext
}

func (sh *shard) lruRemove(e *entry) { dllRemove(e, listLocal, &sh.head, &sh.tail) }
func (sh *shard) lruPushTail(e *entry) { dllPushTail(e, listLocal, &sh.head, &sh.tail) }
func (sh *shard) lruTouch(e *entry)    { dllTouch(e, listLocal, &sh.head, &sh.tail) }

// global* require db.gMu held.
func (db *DB) globalRemove(e *entry) { dllRemove(e, listGlobal, &db.gHead, &db.gTail) }
func (db *DB) globalPushTail(e *entry) {
	dllPushTail(e, listGlobal, &db.gHead, &db.gTail)
}
func (db *DB) globalTouch(e *entry) { dllTouch(e, listGlobal, &db.gHead, &db.gTail) }
