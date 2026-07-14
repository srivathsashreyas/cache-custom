package store

import (
	"strings"
	"time"
)

// EvictionPolicy selects how victims are chosen when maxmemory is exceeded.
// Redis-standard names plus allkeys-fifo / volatile-fifo (not in stock Redis).
type EvictionPolicy string

const (
	PolicyNoEviction     EvictionPolicy = "noeviction"
	PolicyAllKeysLRU     EvictionPolicy = "allkeys-lru"
	PolicyAllKeysLFU     EvictionPolicy = "allkeys-lfu"
	PolicyAllKeysRandom  EvictionPolicy = "allkeys-random"
	PolicyAllKeysFIFO    EvictionPolicy = "allkeys-fifo" // extension (not Redis)
	PolicyVolatileLRU    EvictionPolicy = "volatile-lru"
	PolicyVolatileLFU    EvictionPolicy = "volatile-lfu"
	PolicyVolatileRandom EvictionPolicy = "volatile-random"
	PolicyVolatileTTL    EvictionPolicy = "volatile-ttl"
	PolicyVolatileFIFO   EvictionPolicy = "volatile-fifo" // extension (not Redis)
)

// ParseEvictionPolicy maps a config string to a policy (case-insensitive).
// Empty string defaults to allkeys-lru.
func ParseEvictionPolicy(s string) (EvictionPolicy, bool) {
	p := EvictionPolicy(strings.ToLower(strings.TrimSpace(s)))
	switch p {
	case PolicyNoEviction, PolicyAllKeysLRU, PolicyAllKeysLFU, PolicyAllKeysRandom, PolicyAllKeysFIFO,
		PolicyVolatileLRU, PolicyVolatileLFU, PolicyVolatileRandom, PolicyVolatileTTL, PolicyVolatileFIFO:
		return p, true
	case "":
		return PolicyAllKeysLRU, true
	default:
		return "", false
	}
}

func (p EvictionPolicy) volatileOnly() bool {
	switch p {
	case PolicyVolatileLRU, PolicyVolatileLFU, PolicyVolatileRandom, PolicyVolatileTTL, PolicyVolatileFIFO:
		return true
	default:
		return false
	}
}

func (p EvictionPolicy) usesLFU() bool {
	return p == PolicyAllKeysLFU || p == PolicyVolatileLFU
}

func (p EvictionPolicy) eligible(e *entry) bool {
	if !p.volatileOnly() {
		return true
	}
	return !e.expiresAt.IsZero()
}

func remainingTTL(e *entry, now time.Time) time.Duration {
	if e.expiresAt.IsZero() {
		return time.Duration(1<<63 - 1)
	}
	d := e.expiresAt.Sub(now)
	if d < 0 {
		return 0
	}
	return d
}
