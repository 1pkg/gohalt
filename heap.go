package gohalt

import "sync"

type blatheap struct {
	buffer []uint64
	mutex  sync.Mutex
}

// Len is the number of elements in the collection.
func (lh *blatheap) Len() int {
	lh.mutex.Lock()
	defer lh.mutex.Unlock()
	return len(lh.buffer)
}

// Less reports whether the element with
// index i should sort before the element with index j.
func (lh *blatheap) Less(i int, j int) bool {
	lh.mutex.Lock()
	defer lh.mutex.Unlock()
	return lh.buffer[i] < lh.buffer[j]
}

// Swap swaps the elements with indexes i and j.
func (lh *blatheap) Swap(i int, j int) {
	lh.mutex.Lock()
	defer lh.mutex.Unlock()
	lh.buffer[i], lh.buffer[j] = lh.buffer[j], lh.buffer[i]
}

// Push add x as element Len().
func (lh *blatheap) Push(x interface{}) {
	lh.mutex.Lock()
	defer lh.mutex.Unlock()
	if lat, ok := x.(uint64); ok {
		lh.buffer = append(lh.buffer, lat)
	}
}

// Pop remove and return element Len() - 1.
func (lh *blatheap) Pop() interface{} {
	lh.mutex.Lock()
	defer lh.mutex.Unlock()
	blen := len(lh.buffer)
	val := lh.buffer[blen-1]
	lh.buffer = lh.buffer[:blen-1]
	return val
}

// At returns element at position.
func (lh *blatheap) At(pos int) uint64 {
	lh.mutex.Lock()
	defer lh.mutex.Unlock()
	return lh.buffer[pos]
}

// Prune cleans heap buffer.
func (lh *blatheap) Prune() {
	lh.mutex.Lock()
	defer lh.mutex.Unlock()
	lh.buffer = nil
}
