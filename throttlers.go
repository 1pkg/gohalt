package gohalt

import (
	"context"
	"errors"
	"fmt"
	"runtime"
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

type each struct {
	cur uint64
	num uint64
}

func (thr *each) Acquire(context.Context) error {
	atomic.AddUint64(&thr.cur, 1)
	if thr.cur%thr.num == 0 {
		return fmt.Errorf("throttler skip has been reached %d", thr.cur)
	}
	return nil
}

func (thr *each) Release(context.Context) error {
	return nil
}

type tfixed struct {
	cur uint64
	max uint64
}

func NewThrottlerFixed(max uint64) *tfixed {
	return &tfixed{max: max}
}

func (thr *tfixed) Acquire(context.Context) error {
	if atomic.CompareAndSwapUint64(&thr.cur, thr.max, thr.max) {
		return fmt.Errorf("throttler max running limit has been exceed %d", thr.cur)
	}
	atomic.AddUint64(&thr.cur, 1)
	return nil
}

func (thr *tfixed) Release(context.Context) error {
	return nil
}

type tatomic struct {
	run uint64
	max uint64
}

func NewThrottlerAtomic(max uint64) *tatomic {
	return &tatomic{max: max}
}

func (thr *tatomic) Acquire(context.Context) error {
	if atomic.CompareAndSwapUint64(&thr.run, thr.max, thr.max) {
		return fmt.Errorf("throttler max running limit has been exceed %d", thr.run)
	}
	atomic.AddUint64(&thr.run, 1)
	return nil
}

func (thr *tatomic) Release(context.Context) error {
	if atomic.CompareAndSwapUint64(&thr.run, 0, 0) {
		return errors.New("throttler has nothing to release")
	}
	atomic.AddUint64(&thr.run, ^uint64(0))
	return nil
}

type tblocking struct {
	run chan struct{}
}

func NewThrottlerBlocking(max uint64) *tblocking {
	return &tblocking{run: make(chan struct{}, max)}
}

func (thr *tblocking) Acquire(context.Context) error {
	thr.run <- struct{}{}
	return nil
}

func (thr *tblocking) Release(context.Context) error {
	select {
	case <-thr.run:
	default:
		return errors.New("throttler has nothing to release")
	}
	return nil
}

type ttimed struct {
	*tfixed
}

func NewThrottlerTimed(max uint64, duration time.Duration) ttimed {
	thr := NewThrottlerFixed(max)
	go func() {
		tick := time.NewTicker(duration)
		defer tick.Stop()
		for {
			<-tick.C
			atomic.StoreUint64(&thr.cur, 0)
		}
	}()
	return ttimed{thr}
}

func (thr ttimed) Acquire(ctx context.Context) error {
	return thr.tfixed.Acquire(ctx)
}

func (thr ttimed) Release(ctx context.Context) error {
	return thr.tfixed.Release(ctx)
}

type tquartered struct {
	*tfixed
}

func NewThrottlerQuartered(max uint64, duration time.Duration, quarter time.Duration) tquartered {
	thr := NewThrottlerFixed(max)
	go func() {
		delta, interval := max, duration
		if quarter < duration {
			koef := uint64(interval / quarter)
			delta, interval = delta/koef, quarter
		}
		delta = ^uint64(delta - 1)
		tick := time.NewTicker(interval)
		defer tick.Stop()
		for {
			<-tick.C
			atomic.AddUint64(&thr.cur, delta)
		}
	}()
	return tquartered{thr}
}

func (thr *tquartered) Acquire(ctx context.Context) error {
	return thr.tfixed.Acquire(ctx)
}

func (thr *tquartered) Release(ctx context.Context) error {
	return thr.tfixed.Release(ctx)
}

func KeyedContext(ctx context.Context, key interface{}) context.Context {
	return context.WithValue(ctx, gohaltctxkey, key)
}

const gohaltctxkey = "gohalt_context_key"

type tkeyed struct {
	store  sync.Map
	newthr NewThrottler
}

func NewThrottlerKeyed(newthr NewThrottler) *tkeyed {
	return &tkeyed{newthr: newthr}
}

func (thr *tkeyed) Acquire(ctx context.Context) error {
	if key := ctx.Value(gohaltctxkey); key != nil {
		r, _ := thr.store.LoadOrStore(key, thr.newthr())
		return r.(Throttler).Acquire(ctx)
	}
	return errors.New("keyed throttler can't find any key")
}

func (thr *tkeyed) Release(ctx context.Context) error {
	if key := ctx.Value(gohaltctxkey); key != nil {
		if r, ok := thr.store.Load(key); ok {
			return r.(Throttler).Release(ctx)
		}
		return errors.New("throttler has nothing to release")
	}
	return errors.New("keyed throttler can't find any key")
}

type tmemmark struct {
	alloc    uint64
	system   uint64
	avgpause uint64
}

func NewThrottlerMemMark(alloc uint64, system uint64, avgpause uint64) tmemmark {
	return tmemmark{alloc: alloc, system: system, avgpause: avgpause}
}

func (thr tmemmark) Acquire(context.Context) error {
	alloc, system, avgpause := thr.stats()
	if thr.alloc >= alloc ||
		thr.system >= system ||
		thr.avgpause >= avgpause {
		return fmt.Errorf(
			`throttler memory watermark limit has been exceed
alloc %d mb, system %d mb, average collector pause %s`,
			alloc/1024,
			system/1024,
			time.Duration(avgpause),
		)
	}
	return nil
}

func (thr tmemmark) Release(context.Context) error {
	return nil
}

func (thr tmemmark) stats() (alloc uint64, system uint64, avgpause uint64) {
	var stats runtime.MemStats
	runtime.ReadMemStats(&stats)
	for _, p := range stats.PauseNs {
		avgpause += p
	}
	avgpause /= 256
	alloc = stats.Alloc
	system = stats.Sys
	return
}
