package store

import "errors"

// ValueType is the Redis-like type tag for a key.
type ValueType byte

const (
	// TypeString is the default (including legacy entries with typ==0).
	TypeString ValueType = iota
	TypeHash
	TypeList
	TypeSet
	TypeZSet
)

// ErrWrongType matches Redis WRONGTYPE error text (without leading '-').
var ErrWrongType = errors.New("WRONGTYPE Operation against a key holding the wrong kind of value")

func (t ValueType) String() string {
	switch t {
	case TypeString:
		return "string"
	case TypeHash:
		return "hash"
	case TypeList:
		return "list"
	case TypeSet:
		return "set"
	case TypeZSet:
		return "zset"
	default:
		return "string"
	}
}

func (e *entry) valueType() ValueType {
	if e == nil {
		return TypeString
	}
	return e.typ
}

// memSizeString charges key+value+overhead (string type).
func memSizeString(key, value string) uint64 {
	return uint64(len(key)) + uint64(len(value)) + EntryOverhead
}

func memSizeHash(key string, h map[string]string) uint64 {
	s := uint64(len(key)) + EntryOverhead
	for f, v := range h {
		s += uint64(len(f)) + uint64(len(v)) + 16
	}
	return s
}

func memSizeList(key string, list []string) uint64 {
	s := uint64(len(key)) + EntryOverhead
	for _, v := range list {
		s += uint64(len(v)) + 8
	}
	return s
}

func memSizeSet(key string, set map[string]struct{}) uint64 {
	s := uint64(len(key)) + EntryOverhead
	for m := range set {
		s += uint64(len(m)) + 8
	}
	return s
}

func memSizeZSet(key string, zs *zsetData) uint64 {
	s := uint64(len(key)) + EntryOverhead
	if zs == nil {
		return s
	}
	for m := range zs.scores {
		s += uint64(len(m)) + 16
	}
	return s
}

func (e *entry) computeSize() uint64 {
	switch e.valueType() {
	case TypeHash:
		return memSizeHash(e.key, e.hash)
	case TypeList:
		return memSizeList(e.key, e.list)
	case TypeSet:
		return memSizeSet(e.key, e.set)
	case TypeZSet:
		return memSizeZSet(e.key, e.zset)
	default:
		return memSizeString(e.key, e.value)
	}
}
