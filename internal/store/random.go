package store

import "math/rand"

// Dense key array for O(1) random victim selection (swap-remove on delete).

func (sh *shard) rndAdd(e *entry) {
	e.rndIdx = len(sh.rnd)
	sh.rnd = append(sh.rnd, e)
}

func (sh *shard) rndRemove(e *entry) {
	i := e.rndIdx
	if i < 0 || i >= len(sh.rnd) || sh.rnd[i] != e {
		return
	}
	last := sh.rnd[len(sh.rnd)-1]
	sh.rnd[i] = last
	last.rndIdx = i
	sh.rnd = sh.rnd[:len(sh.rnd)-1]
	e.rndIdx = -1
}

// rndPick is O(1) expected for allkeys-random; O(1) expected for volatile if many TTLs.
func (sh *shard) rndPick(skipKey string, volatileOnly bool) *entry {
	n := len(sh.rnd)
	if n == 0 {
		return nil
	}
	// Bounded attempts then linear fallback among a small window.
	for try := 0; try < 8; try++ {
		e := sh.rnd[rand.Intn(n)]
		if e.key == skipKey {
			continue
		}
		if volatileOnly && e.expiresAt.IsZero() {
			continue
		}
		return e
	}
	start := rand.Intn(n)
	for i := 0; i < n; i++ {
		e := sh.rnd[(start+i)%n]
		if e.key == skipKey {
			continue
		}
		if volatileOnly && e.expiresAt.IsZero() {
			continue
		}
		return e
	}
	return nil
}
