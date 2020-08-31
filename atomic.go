package gohalt

import (
	"math"
	"sync/atomic"
)

func atomicBAdd(number *uint64, delta uint64) (res uint64) {
	prev := atomic.LoadUint64(number)
	if res = atomic.AddUint64(number, delta); res < prev {
		res = math.MaxUint64
		atomic.StoreUint64(number, res)
	}
	return
}

func atomicBSub(number *uint64, delta uint64) (res uint64) {
	prev := atomic.LoadUint64(number)
	if res = atomic.AddUint64(number, ^uint64(delta-1)); res > prev {
		res = 0
		atomic.StoreUint64(number, res)
	}
	return
}

func atomicBIncr(number *uint64) (res uint64) {
	if res = atomic.AddUint64(number, 1); res == 0 {
		res = atomic.AddUint64(number, ^uint64(0))
	}
	return
}

func atomicBDecr(number *uint64) (res uint64) {
	if res = atomic.AddUint64(number, ^uint64(0)); res == math.MaxUint64 {
		res = atomic.AddUint64(number, 1)
	}
	return
}

func atomicIncr(number *uint64) uint64 {
	return atomic.AddUint64(number, 1)
}

func atomicSet(number *uint64, value uint64) uint64 {
	atomic.StoreUint64(number, value)
	return value
}

func atomicGet(number *uint64) uint64 {
	return atomic.LoadUint64(number)
}
