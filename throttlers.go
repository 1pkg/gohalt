package gohalt

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
)

// Throttler defines main interfaces
// for all derived throttlers, and
// defines main throttling lock/unlock flows.
type Throttler interface {
	Acquire(context.Context) error
	Release(context.Context) error
}

type maxRunningAtomic struct {
	max, running uint64
}

func NewMaxRunningAtomic(max uint64) *maxRunningAtomic {
	return &maxRunningAtomic{
		max:     max,
		running: 0,
	}
}

func (t *maxRunningAtomic) Acquire(context.Context) error {
	if atomic.CompareAndSwapUint64(&t.running, t.max, t.max) {
		return fmt.Errorf("throttler max running limit has been exceed %d", t.max)
	}
	atomic.AddUint64(&t.running, 1)
	return nil
}

func (t *maxRunningAtomic) Release(context.Context) error {
	if atomic.CompareAndSwapUint64(&t.running, 0, 0) {
		return errors.New("throttler has nothing to release")
	}
	atomic.AddUint64(&t.running, ^uint64(0))
	return nil
}

type maxRunningBlocking struct {
	ch chan struct{}
}

func NewMaxRunningBlocking(max uint64) *maxRunningBlocking {
	return &maxRunningBlocking{
		ch: make(chan struct{}, max),
	}
}

func (t *maxRunningBlocking) Acquire(context.Context) error {
	t.ch <- struct{}{}
	return nil
}

func (t *maxRunningBlocking) Release(context.Context) error {
	select {
	case <-t.ch:
	default:
	}
	return nil
}
