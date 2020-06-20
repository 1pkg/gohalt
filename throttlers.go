package gohalt

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"
)

// Throttler defines main interfaces
// for all derived throttlers, and
// defines main throttling lock/unlock flows.
type Throttler interface {
	Acquire(context.Context) error
	Release(context.Context) error
}

type NewThrottler func() Throttler

type tfixed struct {
	c uint64
	m uint64
}

func NewThrottlerFixed(max uint64) *tfixed {
	return &tfixed{m: max}
}

func (t *tfixed) Acquire(context.Context) error {
	if atomic.CompareAndSwapUint64(&t.c, t.m, t.m) {
		return fmt.Errorf("throttler max running limit has been exceed %d", t.m)
	}
	atomic.AddUint64(&t.c, 1)
	return nil
}

func (t *tfixed) Release(context.Context) error {
	return nil
}

type tatomic struct {
	r uint64
	m uint64
}

func NewThrottlerAtomic(max uint64) *tatomic {
	return &tatomic{m: max}
}

func (t *tatomic) Acquire(context.Context) error {
	if atomic.CompareAndSwapUint64(&t.r, t.m, t.m) {
		return fmt.Errorf("throttler max running limit has been exceed %d", t.m)
	}
	atomic.AddUint64(&t.r, 1)
	return nil
}

func (t *tatomic) Release(context.Context) error {
	if atomic.CompareAndSwapUint64(&t.r, 0, 0) {
		return errors.New("throttler has nothing to release")
	}
	atomic.AddUint64(&t.r, ^uint64(0))
	return nil
}

type tblocking struct {
	r chan struct{}
}

func NewThrottlerBlocking(max uint64) *tblocking {
	return &tblocking{r: make(chan struct{}, max)}
}

func (t *tblocking) Acquire(context.Context) error {
	t.r <- struct{}{}
	return nil
}

func (t *tblocking) Release(context.Context) error {
	select {
	case <-t.r:
	default:
		return errors.New("throttler has nothing to release")
	}
	return nil
}

type ttimed struct {
	*tfixed
}

func NewThrottlerTimed(max uint64, duration time.Duration) ttimed {
	t := NewThrottlerFixed(max)
	go func() {
		tick := time.NewTicker(duration)
		defer tick.Stop()
		for {
			<-tick.C
			atomic.StoreUint64(&t.c, 0)
		}
	}()
	return ttimed{t}
}

func (t ttimed) Acquire(ctx context.Context) error {
	return t.tfixed.Acquire(ctx)
}

func (t ttimed) Release(ctx context.Context) error {
	return t.tfixed.Release(ctx)
}

type tquartered struct {
	*tfixed
}

func NewThrottlerQuartered(max uint64, duration time.Duration, quarter time.Duration) tquartered {
	t := NewThrottlerFixed(max)
	go func() {
		interval := duration
		delta := max
		if quarter < duration {
			koef := uint64(interval / quarter)
			interval = quarter
			delta /= koef
		}
		tick := time.NewTicker(interval)
		defer tick.Stop()
		for {
			<-tick.C
			atomic.AddUint64(&t.c, ^uint64(delta-1))
		}
	}()
	return tquartered{t}
}

func (t *tquartered) Acquire(ctx context.Context) error {
	return t.tfixed.Acquire(ctx)
}

func (t *tquartered) Release(ctx context.Context) error {
	return t.tfixed.Release(ctx)
}

func KeyedContext(ctx context.Context, key interface{}) context.Context {
	return context.WithValue(ctx, gohaltctxkey, key)
}

const gohaltctxkey = "gohalt_context_key"

type tkeyed struct {
	t  sync.Map
	nt NewThrottler
}

func NewThrottlerKeyed(newt NewThrottler) *tkeyed {
	return &tkeyed{nt: newt}
}

func (t *tkeyed) Acquire(ctx context.Context) error {
	if key := ctx.Value(gohaltctxkey); key != nil {
		r, _ := t.t.LoadOrStore(key, t.nt())
		return r.(Throttler).Acquire(ctx)
	}
	return errors.New("throttler can't find any key")
}

func (t *tkeyed) Release(ctx context.Context) error {
	if key := ctx.Value(gohaltctxkey); key != nil {
		if r, ok := t.t.Load(key); ok {
			return r.(Throttler).Release(ctx)
		}
		return errors.New("throttler has nothing to release")
	}
	return errors.New("throttler can't find any key")
}
