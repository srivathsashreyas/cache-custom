//go:build legacy

package main

import "time"

// keep track of the LRU values corresponding to the given key
type LRUKV struct {
	ClientKey string
	Key       uint64
	Value     string
	Next      *LRUKV
	Prev      *LRUKV
	Timestamp time.Time //insert timestamp - used to implement optional ttl logic
}

type LRUCache struct {
	//capacity of the cache (bytes)
	MaxMemory uint64
	//allows O(1) lookup of the LRUKV node when given the key
	Store map[uint64]*LRUKV
	//Head of the LRUKV list
	KeyHead *LRUKV
	//Tail of the LRUKV list
	KeyTail *LRUKV
	//Keeps track of how much cache memory (bytes) has already been used
	UsedMemory uint64
	//track how many keys were evicted from the cache
	Evictions uint64
}

func ConstructorLRU(capacity uint64) *LRUCache {
	//change maxmemory to a configurable value
	cache := &LRUCache{MaxMemory: capacity}
	cache.Store = make(map[uint64]*LRUKV)
	return cache
}

func (lruCache *LRUCache) removeNode(node *LRUKV) {

	//if there is a prior node, attach it to the subsequent node
	//if there is no prior node, then the key we are removing must be the head node
	//set the head to the subsequent node in lruCache case
	if node.Prev != nil {
		node.Prev.Next = node.Next
	} else {
		lruCache.KeyHead = node.Next
	}

	//if there is a next node, attach it to the prior node
	//if there is no next node, then the key we are removing must be the tail node
	//set the tail to the prior node in lruCache case
	if node.Next != nil {
		node.Next.Prev = node.Prev
	} else {
		lruCache.KeyTail = node.Prev
	}
}

func (lruCache *LRUCache) addToTail(node *LRUKV) {
	//the node must point to the original tail
	node.Prev = lruCache.KeyTail
	//since lruCache will become the new tail node, it must not have any subsequent node
	node.Next = nil

	//make the original tail node point to the current node
	//if the original tail doesn't exist, then the list doesn't exist
	//set the head to the current node
	if lruCache.KeyTail != nil {
		lruCache.KeyTail.Next = node
	} else {
		lruCache.KeyHead = node
	}

	//set the new tail to the current node
	lruCache.KeyTail = node
}

// when a key is retrieved or set (get/put)
// that key must be shifted to the end of the LRUKV list
// the last element in lruCache list is the most recently used key
// the first key in lruCache list is the least recently used key
func (lruCache *LRUCache) shiftKV(mru *LRUKV) {
	//shift the most recently used key to the end
	//by first removing the key and then adding it to the tail
	lruCache.removeNode(mru)
	lruCache.addToTail(mru)

}

// retrieve a value from the LRU cache
func (lruCache *LRUCache) Get(key uint64) string {
	//if the value exists shift the key to the end of the LRUKV list
	//and return the value
	//else return -1 if the key does not exist
	if mru, ok := lruCache.Store[key]; ok {
		lruCache.shiftKV(mru)
		return mru.Value
	}
	return "key not found"
}

// insert a value in the LRU cache
func (lruCache *LRUCache) Put(key uint64, value string, clientKey string, ttlHeap *TTLHeap, keyMap map[string]uint64) string {

	//this check ensures that it's not possible to exceed max memory limit
	//if the length of client key and/or value is greater than the max memory limit
	if entrySize(clientKey, value) > lruCache.MaxMemory {
		return "Not enough memory to insert key-value pair."
	}

	//if the key exists in the store, update it's value provided
	//the (used memory - size of old value + size of the new value) is less than or equal to the MaxMemory (note that if the value is not updated, the key does not get shifted to the end of the lru key list)
	//and shift it to the end of the lru key list
	if mru, ok := lruCache.Store[key]; ok {
		if (lruCache.UsedMemory-entrySize("", mru.Value))+entrySize("", value) > lruCache.MaxMemory {
			return "Not enough memory to update value. Consider deleting the key and reinserting."
		}

		//update the used memory by subtracting the size of the old value and adding the size of the new value
		lruCache.UsedMemory -= entrySize("", mru.Value)
		lruCache.UsedMemory += entrySize("", value)

		lruCache.Store[key].Value = value

		//update the timestamp to the current time
		lruCache.Store[key].Timestamp = time.Now()
		//update the ttlHeap with the new timestamp
		increaseKeyHeap(ttlHeap, TTL{Key: key, Timestamp: lruCache.Store[key].Timestamp})

		lruCache.shiftKV(mru)
		return "OK"
	}

	//if the key doesn't exist, and the capacity has already been reached
	//i)  delete the lru clientKey and (cache) Key from keyMap and lruCache store
	//ii) append the lru key to the freedKeys list
	//iii) reduce the used memory by the size of the evicted key
	//iv) remove the key from the ttlHeap
	//v) evict the lru key
	//continue this process until the usedMemory + size of the new entry is less than or equal to the MaxMemory

	//if the key doesn't exist, and the capacity has not been reached
	//i)  add the key to the lruCache store
	//ii) update the used memory by the size of the entry
	for lruCache.UsedMemory+entrySize(clientKey, value) > lruCache.MaxMemory {
		lru := lruCache.KeyHead

		//if there is no lru key, break the loop
		if lru == nil {
			break
		}

		delete(keyMap, lru.ClientKey)
		delete(lruCache.Store, lru.Key)

		gmu.Lock()
		freedKeys = append(freedKeys, lru.Key)
		gmu.Unlock()

		lruCache.UsedMemory -= entrySize(lru.ClientKey, lru.Value)

		deleteKeyHeap(ttlHeap, TTL{Key: lru.Key})

		lruCache.removeNode(lru)
		//track how many keys were evicted from the cache
		lruCache.Evictions++
	}

	lruCache.UsedMemory += entrySize(clientKey, value)

	//add the key to the lru cache
	node := &LRUKV{Key: key}

	node.Timestamp = time.Now()
	//add the client key, cache key and the associated ttl to the heap
	insertHeap(ttlHeap, TTL{ClientKey: clientKey, Key: key, Timestamp: node.Timestamp})

	node.Value = value
	node.ClientKey = clientKey
	lruCache.Store[key] = node
	lruCache.addToTail(node)
	return "OK"
}

//delete the key from the LRU cache
func (lruCache *LRUCache) Delete(key uint64, ttlHeap *TTLHeap) {

	if node, ok := lruCache.Store[key]; ok {
		lruCache.UsedMemory -= entrySize(node.ClientKey, node.Value)
		lruCache.removeNode(node)
		//remove the key from the ttlHeap
		deleteKeyHeap(ttlHeap, TTL{Key: key})
	}
	delete(lruCache.Store, key)
}

//retrieve stats for the LRU cache
func (lruCache *LRUCache) Stats() (uint64, uint64, uint64) {
	//return the used memory, max memory and number of evictions
	return lruCache.UsedMemory, lruCache.MaxMemory, lruCache.Evictions
}
