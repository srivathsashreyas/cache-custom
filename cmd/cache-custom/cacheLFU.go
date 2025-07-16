package main

import "time"

// keep track of the LFU values corresponding to the given key
type LFUKV struct {
	ClientKey string
	Key       uint64
	//keeps track of how often the key is accessed (get/put)
	Frequency int
	Value     string
	Next      *LFUKV
	Prev      *LFUKV
	Timestamp time.Time //insert timestamp - used to implement optional ttl logic
}

//track the head and tail of the list for each frequency
type LFUKVList struct {
	Head *LFUKV
	Tail *LFUKV
}

type LFUCache struct {
	//capacity of the cache (bytes)
	MaxMemory uint64
	//allows O(1) lookup of the LFUKV node when given the key
	Store map[uint64]*LFUKV
	//frequency store - maps frequency to a (linked) list of LFUKV nodes with that frequency
	FrequencyStore map[int]*LFUKVList
	//Keeps track of how much cache memory (bytes) has already been used
	UsedMemory uint64
	//keep track of the min freq bucket
	MinFreq int
	//keep track of the max freq bucket
	MaxFreq int
	//track how many keys were evicted from the cache
	Evictions uint64
}

func ConstructorLFU(capacity uint64) *LFUCache {
	//change maxmemory to a configurable value
	cache := &LFUCache{MaxMemory: capacity}
	cache.Store = make(map[uint64]*LFUKV)
	cache.FrequencyStore = make(map[int]*LFUKVList)
	//initialize max frequency to 1 (min frequency will be set to 1 on the first put operation)
	cache.MaxFreq = 1
	return cache
}

//set minFreq to the next non empty bucket if the current bucket is empty
func (lfuCache *LFUCache) findNextMinFreq(currMinFreq int, maxFreq int) int {
	newMinFreq := 1

	//check for the next non empty bucket after currMinFreq
	for i := currMinFreq; i <= maxFreq; i++ {
		if v, ok := lfuCache.FrequencyStore[i]; ok && v.Head != nil {
			newMinFreq = i
			break
		}
	}
	return newMinFreq
}

//remove from the node from a given frequency bucket
func (lfuCache *LFUCache) removeNode(freq int, node *LFUKV) {

	//if the frequency bucket does not exist, return
	if _, ok := lfuCache.FrequencyStore[freq]; !ok {
		return
	}

	//if there is a prior node in the frequency bucket, attach it to the subsequent node
	//if there is no prior node in the frequency bucket, then the key we are removing must be the head node of that frequency bucket
	//set the head to the subsequent node in this frequency bucket
	if node.Prev != nil {
		node.Prev.Next = node.Next
	} else {
		lfuCache.FrequencyStore[freq].Head = node.Next
	}

	//if there is a next node in the frequency bucket, attach it to the prior node
	//if there is no next node in the frequency bucket, then the key we are removing must be the tail node
	//set the tail of this bucket to the prior node in the list
	if node.Next != nil {
		node.Next.Prev = node.Prev
	} else {
		lfuCache.FrequencyStore[freq].Tail = node.Prev
	}

	//if the bucket becomes empty, delete the frequency from the frequency store and find the next min frequency
	if lfuCache.FrequencyStore[freq].Head == nil && lfuCache.FrequencyStore[freq].Tail == nil {
		//if the frequency bucket is empty, delete it from the frequency store
		delete(lfuCache.FrequencyStore, freq)
		//if the minFreq is equal to the freq, update the minFreq
		if lfuCache.MinFreq == freq {
			lfuCache.MinFreq = lfuCache.findNextMinFreq(freq, lfuCache.MaxFreq)
		}
	}
}

//add to the tail of the appropriate frequency bucket
func (lfuCache *LFUCache) addToTail(freq int, node *LFUKV) {

	//if the frequency bucket does not exist, create it
	if _, ok := lfuCache.FrequencyStore[freq]; !ok {
		lfuCache.FrequencyStore[freq] = &LFUKVList{}

		/** extremely important **/
		//if a node is being shifted to a new frequency bucket
		//and that bucket does not already exist
		//the references of the node to the previous bucket (if present) must be cleared
		//to avoid pointing to 'incorrect' data in the previous bucket
		//note that the removeNode function does not clear the next and prev references of the node itself
		//rather it simply makes the node inaccessible in that list by manipulating the pointers of the adjacent (next and prev) nodes
		if node.Next != nil {
			node.Next = nil
		}
		if node.Prev != nil {
			node.Prev = nil
		}
		/* ***  */

		lfuCache.FrequencyStore[freq].Head = node
		lfuCache.FrequencyStore[freq].Tail = node
		return
	}

	//if the frequency bucket exists, add the node to the tail of the list

	//the node must point to the original tail
	node.Prev = lfuCache.FrequencyStore[freq].Tail
	//since the current node will become the new tail node, it must not have any subsequent node
	node.Next = nil

	//make the original tail node point to the current node if it's not nil
	//if it is nil then the list for that frequency is empty
	//set the head of the list for that frequency bucket to the current node
	if lfuCache.FrequencyStore[freq].Tail != nil {
		lfuCache.FrequencyStore[freq].Tail.Next = node
	} else {
		lfuCache.FrequencyStore[freq].Head = node
	}

	//set the new tail of the bucket to the current node
	lfuCache.FrequencyStore[freq].Tail = node
}

// when a key is retrieved or set (get/put)
// that key must be removed from the frequency bucket with key 'freq'
// and added to the end/tail of the frequency bucket with key 'freq + 1'
// the first key in each frequency bucket is the least frequently used key in that bucket
func (lfuCache *LFUCache) shiftKV(freq int, node *LFUKV) {
	//shift the most recently used key to the end
	//by first removing the key and then adding it to the tail
	lfuCache.removeNode(freq, node)
	lfuCache.addToTail(freq+1, node)

}

// retrieve a value from the LFU cache
func (lfuCache *LFUCache) Get(key uint64) string {

	//if the value exists
	//shift the node to the appropriate frequency bucket
	//increment it's frequency
	//and return the value
	//else return -1 if the key does not exist
	if mru, ok := lfuCache.Store[key]; ok {
		lfuCache.shiftKV(mru.Frequency, mru)
		mru.Frequency++

		//update min/max frequencies
		if mru.Frequency > lfuCache.MaxFreq {
			lfuCache.MaxFreq = mru.Frequency
		}
		if mru.Frequency < lfuCache.MinFreq {
			lfuCache.MinFreq = mru.Frequency
		}
		return mru.Value
	}
	return "key not found"
}

// insert a value in the LFU cache
func (lfuCache *LFUCache) Put(key uint64, value string, clientKey string, ttlHeap *TTLHeap, keyMap map[string]uint64) string {

	//this check ensures that it's not possible to exceed max memory limit
	//if the length of client key and/or value is greater than the max memory limit
	if entrySize(clientKey, value) > lfuCache.MaxMemory {
		return "Not enough memory to insert key-value pair."
	}

	//if the key exists in the store, update it's value provided
	//the (used memory - size of old value + size of the new value) is less than or equal to the MaxMemory (note that if the value is not updated, the key does not get shifted to the end of the lfu key list)
	//and shift it to the end of the lfu key list
	if mru, ok := lfuCache.Store[key]; ok {
		if (lfuCache.UsedMemory-entrySize("", mru.Value))+entrySize("", value) > lfuCache.MaxMemory {
			return "Not enough memory to update value. Consider deleting the key and reinserting."
		}

		//update the used memory by subtracting the size of the old value and adding the size of the new value
		lfuCache.UsedMemory -= entrySize("", mru.Value)
		lfuCache.UsedMemory += entrySize("", value)

		lfuCache.Store[key].Value = value

		//move the key value pair to the next frequency bucket
		lfuCache.shiftKV(mru.Frequency, mru)

		//update the frequency of the key and min/max frequencies
		lfuCache.Store[key].Frequency++
		if mru.Frequency > lfuCache.MaxFreq {
			lfuCache.MaxFreq = mru.Frequency
		}
		if mru.Frequency < lfuCache.MinFreq {
			lfuCache.MinFreq = mru.Frequency
		}

		//update the timestamp to the current time
		lfuCache.Store[key].Timestamp = time.Now()
		//update the ttlHeap with the new timestamp
		increaseKeyHeap(ttlHeap, TTL{Key: key, Timestamp: lfuCache.Store[key].Timestamp})

		return "OK"
	}

	//if the key doesn't exist, and the capacity has already been reached
	//i)  find the minimum frequency bucket
	//ii) if the minimum frequency bucket is empty, find the bucket with lowest frequency that is non empty
	//iii) delete the client and cache key corresponding to the head of this bucket from the keyMap and lfuCache store
	//iv) append the lfu key to the freedKeys list
	//iv) reduce the used memory by the size of the evicted key
	//v) remove the key from the ttlHeap
	//vi) evict the lfu key (the head of the minFreq bucket)
	//continue this process until the usedMemory + size of the new entry is less than or equal to the MaxMemory

	//if the key doesn't exist, and the capacity has not been reached
	//i)  add the key to the lfuCache store
	//ii) update the used memory by the size of the entry
	for lfuCache.UsedMemory+entrySize(clientKey, value) > lfuCache.MaxMemory {
		lfu, ok := lfuCache.FrequencyStore[lfuCache.MinFreq]
		//if the min frequency bucket exists and is empty, find the next non empty bucket
		if ok && lfu.Head == nil {
			lfuCache.MinFreq = lfuCache.findNextMinFreq(lfuCache.MinFreq, lfuCache.MaxFreq)
		}

		//retrieve the list with the correct (potentially 'new') minfreq
		lfu = lfuCache.FrequencyStore[lfuCache.MinFreq]

		//remove the client key and cache key of the lru key in the minFreq bucket
		delete(keyMap, lfu.Head.ClientKey)
		delete(lfuCache.Store, lfu.Head.Key)

		gmu.Lock()
		freedKeys = append(freedKeys, lfu.Head.Key)
		gmu.Unlock()

		lfuCache.UsedMemory -= entrySize(lfu.Head.ClientKey, lfu.Head.Value)

		deleteKeyHeap(ttlHeap, TTL{Key: lfu.Head.Key})

		lfuCache.removeNode(lfu.Head.Frequency, lfu.Head)

		//track how many keys were evicted from the cache
		lfuCache.Evictions++
	}

	lfuCache.UsedMemory += entrySize(clientKey, value)

	//add the key to the lfu cache
	node := &LFUKV{Key: key}

	//update the frequency of the node
	node.Frequency++
	//reset MinFreq (since the first put operation for a new key value pair will always set the MinFreq to 1)
	lfuCache.MinFreq = 1

	node.Timestamp = time.Now()
	//add the client key, cache key and the associated ttl to the heap
	insertHeap(ttlHeap, TTL{ClientKey: clientKey, Key: key, Timestamp: node.Timestamp})

	node.Value = value
	node.ClientKey = clientKey
	lfuCache.Store[key] = node
	lfuCache.addToTail(node.Frequency, node)
	return "OK"
}

//delete the key from the LFU cache
func (lfuCache *LFUCache) Delete(key uint64, ttlHeap *TTLHeap) {

	if node, ok := lfuCache.Store[key]; ok {
		lfuCache.UsedMemory -= entrySize(node.ClientKey, node.Value)
		lfuCache.removeNode(node.Frequency, node)
		//remove the key from the ttlHeap
		deleteKeyHeap(ttlHeap, TTL{Key: key})
	}
	delete(lfuCache.Store, key)
}

//retrieve stats for the LRU cache
func (lfuCache *LFUCache) Stats() (uint64, uint64, uint64) {
	//return the used memory, max memory and number of evictions
	return lfuCache.UsedMemory, lfuCache.MaxMemory, lfuCache.Evictions
}
