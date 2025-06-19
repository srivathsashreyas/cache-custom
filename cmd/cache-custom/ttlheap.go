package main

import (
	"sync"
	"time"
)

type TTL struct {
	ClientKey string
	Key       uint64
	Timestamp time.Time
}

type TTLHeap struct {
	TTLs []TTL
	Mu   sync.Mutex
	// map key to index in the heap for quick access
	KeyIdx map[uint64]int
}

func parent(i int) int {
	return (i - 1) / 2
}

func leftChild(i int) int {
	return 2*i + 1
}

func rightChild(i int) int {
	return 2*i + 2
}

func swap(ttlHeap *TTLHeap, i int, j int) {
	//swap the elements
	ttlHeap.TTLs[i], ttlHeap.TTLs[j] = ttlHeap.TTLs[j], ttlHeap.TTLs[i]

	//update the key index map to reflect the new indices for the keys
	ttlHeap.KeyIdx[ttlHeap.TTLs[i].Key] = i
	ttlHeap.KeyIdx[ttlHeap.TTLs[j].Key] = j
}

func heapify(ttlHeap *TTLHeap, i int) {
	n := len(ttlHeap.TTLs)

	//if there is only one element, then it is already a heap
	if n == 1 {
		return
	}

	//initially set the smallest node to the current node
	//and calculate the left and right children indices
	smallest := i
	left := leftChild(i)
	right := rightChild(i)

	//check if either the left or right child is smaller than the current node
	if left < n && ttlHeap.TTLs[left].Timestamp.Before(ttlHeap.TTLs[smallest].Timestamp) {
		smallest = left
	}
	if right < n && ttlHeap.TTLs[right].Timestamp.Before(ttlHeap.TTLs[smallest].Timestamp) {
		smallest = right
	}

	//if the smallest is not the current node, then we need to swap
	if smallest != i {
		//swap the current node with the smallest child
		swap(ttlHeap, i, smallest)
		//recursively heapify the affected subtree
		heapify(ttlHeap, smallest)
	}
}

func insertHeap(ttlHeap *TTLHeap, k TTL) {
	//acquire lock to ensure that other concurrent operations on the cache that involve the heap do not interfere
	ttlHeap.Mu.Lock()
	defer ttlHeap.Mu.Unlock()

	//add the key to the ttlHeap
	ttlHeap.TTLs = append(ttlHeap.TTLs, k)
	//add an entry in the key index map for the ttl
	ttlHeap.KeyIdx[k.Key] = len(ttlHeap.TTLs) - 1

	i := len(ttlHeap.TTLs) - 1
	//repeat the process of swapping the current node with it's parent if the heap condition is violated
	//until the heap condition is satisfied
	for i > 0 && ttlHeap.TTLs[parent(i)].Timestamp.After(ttlHeap.TTLs[i].Timestamp) {
		swap(ttlHeap, i, parent(i))
		//set the current node to it's parent and repeat the process
		i = parent(i)
	}
}

func increaseKeyHeap(ttlHeap *TTLHeap, k TTL) {
	//acquire lock to ensure that other concurrent operations on the cache that involve the heap do not interfere
	ttlHeap.Mu.Lock()
	defer ttlHeap.Mu.Unlock()

	//find the position of the element(key) to be updated in the heap using the key index map
	updatePos := ttlHeap.KeyIdx[k.Key]

	//update the timestamp of the element in the heap
	ttlHeap.TTLs[updatePos].Timestamp = k.Timestamp
	//restore the heap structure from updatePos
	heapify(ttlHeap, updatePos)
}

// delete a given element from the heap
func deleteKeyHeap(ttlHeap *TTLHeap, k TTL) {
	//acquire lock to ensure that other concurrent operations on the cache that involve the heap do not interfere
	ttlHeap.Mu.Lock()
	defer ttlHeap.Mu.Unlock()

	//find the position of the element to be deleted from the heap using the key index map
	deletePos := ttlHeap.KeyIdx[k.Key]

	//set the element to the first element in the heap
	//so that it is ultimately moved to the top of the heap
	ttlHeap.TTLs[deletePos] = ttlHeap.TTLs[0]

	//restore the heap structure from deletePos
	i := deletePos
	for i > 0 && ttlHeap.TTLs[parent(i)].Timestamp.After(ttlHeap.TTLs[i].Timestamp) {
		ttlHeap.TTLs[i], ttlHeap.TTLs[parent(i)] = ttlHeap.TTLs[parent(i)], ttlHeap.TTLs[i]
		i = parent(i)
	}

	/* popHeap -> pop the root element from the heap (which is now the element to be deleted) */
	//if the heap is empty return an empty TTL reference
	if len(ttlHeap.TTLs) == 0 {
		return
	}

	//replace the root (minimum TTL) with the last element in the heap (equivalent to popping the root element)
	ttlHeap.TTLs[0] = ttlHeap.TTLs[len(ttlHeap.TTLs)-1]
	//update the key index map to reflect the position(0) of the 'new'(temporary) root element
	ttlHeap.KeyIdx[ttlHeap.TTLs[0].Key] = 0

	//reduce the heap size by one (remove the last element)
	ttlHeap.TTLs = ttlHeap.TTLs[0 : len(ttlHeap.TTLs)-1]

	//call heapify on the root (which now contains the last element) to restore the heap structure
	heapify(ttlHeap, 0)
	/* end popHeap logic*/
}

// retrieve the minimum TTL from the heap
func getMinTTL(ttlHeap *TTLHeap) (TTL, int) {
	//acquire lock to ensure that other concurrent operations on the cache that involve the heap do not interfere
	ttlHeap.Mu.Lock()
	defer ttlHeap.Mu.Unlock()

	if len(ttlHeap.TTLs) == 0 {
		return TTL{}, 0
	}
	//return the minimum TTL (the root of the heap)
	return ttlHeap.TTLs[0], len(ttlHeap.TTLs)
}
