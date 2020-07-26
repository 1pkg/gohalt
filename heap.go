package gohalt

import "sync"

type blatheap struct {
	buffer []uint64
	lock   sync.Mutex
}

func (lh *blatheap) Len() int {
	lh.lock.Lock()
	defer lh.lock.Unlock()
	return len(lh.buffer)
}

func (lh *blatheap) Less(i int, j int) bool {
	lh.lock.Lock()
	defer lh.lock.Unlock()
	return lh.buffer[i] < lh.buffer[j]
}

func (lh *blatheap) Swap(i int, j int) {
	lh.lock.Lock()
	defer lh.lock.Unlock()
	lh.buffer[i], lh.buffer[j] = lh.buffer[j], lh.buffer[i]
}

func (lh *blatheap) Push(x interface{}) {
	lh.lock.Lock()
	defer lh.lock.Unlock()
	if lat, ok := x.(uint64); ok {
		lh.buffer = append(lh.buffer, lat)
	}
}

func (lh *blatheap) Pop() interface{} {
	lh.lock.Lock()
	defer lh.lock.Unlock()
	blen := len(lh.buffer)
	val := lh.buffer[blen-1]
	lh.buffer = lh.buffer[:blen-1]
	return val
}

func (lh *blatheap) At(pos int) uint64 {
	lh.lock.Lock()
	defer lh.lock.Unlock()
	return lh.buffer[pos]
}

func (lh *blatheap) Prune() {
	lh.lock.Lock()
	defer lh.lock.Unlock()
	lh.buffer = nil
}
