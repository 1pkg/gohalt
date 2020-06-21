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
	Acquire(context.Context) (context.Context, error)
	Release(context.Context) (context.Context, error)
}

type NewThrottler func() Throttler

type each struct {
	cur uint64
	num uint64
}

func NewThrottlerEach(num uint64) *each {
	return &each{num: num}
}

func (thr *each) Acquire(ctx context.Context) (context.Context, error) {
	atomic.AddUint64(&thr.cur, 1)
	if cur := atomic.LoadUint64(&thr.cur); cur%thr.num == 0 {
		return ctx, fmt.Errorf("throttler skip has been reached %d", cur)
	}
	return ctx, nil
}

func (thr *each) Release(ctx context.Context) (context.Context, error) {
	return ctx, nil
}

type tfixed struct {
	cur uint64
	max uint64
}

func NewThrottlerFixed(max uint64) *tfixed {
	return &tfixed{max: max}
}

func (thr *tfixed) Acquire(ctx context.Context) (context.Context, error) {
	if cur := atomic.LoadUint64(&thr.cur); cur > thr.max {
		return ctx, fmt.Errorf("throttler max fixed limit has been exceed %d", cur)
	}
	atomic.AddUint64(&thr.cur, 1)
	return ctx, nil
}

func (thr *tfixed) Release(ctx context.Context) (context.Context, error) {
	return ctx, nil
}

type tatomic struct {
	run uint64
	max uint64
}

func NewThrottlerAtomic(max uint64) *tatomic {
	return &tatomic{max: max}
}

func (thr *tatomic) Acquire(ctx context.Context) (context.Context, error) {
	if run := atomic.LoadUint64(&thr.run); run > thr.max {
		return ctx, fmt.Errorf("throttler max running limit has been exceed %d", run)
	}
	atomic.AddUint64(&thr.run, 1)
	return ctx, nil
}

func (thr *tatomic) Release(ctx context.Context) (context.Context, error) {
	if run := atomic.LoadUint64(&thr.run); run <= 0 {
		return ctx, errors.New("throttler has nothing to release")
	}
	atomic.AddUint64(&thr.run, ^uint64(0))
	return ctx, nil
}

type tblocking struct {
	run chan struct{}
}

func NewThrottlerBlocking(max uint64) *tblocking {
	return &tblocking{run: make(chan struct{}, max)}
}

func (thr *tblocking) Acquire(ctx context.Context) (context.Context, error) {
	thr.run <- struct{}{}
	return ctx, nil
}

func (thr *tblocking) Release(ctx context.Context) (context.Context, error) {
	select {
	case <-thr.run:
	default:
		return ctx, errors.New("throttler has nothing to release")
	}
	return ctx, nil
}

type ttimed struct {
	*tfixed
}

func NewThrottlerTimed(max uint64, duration time.Duration, quarter uint64) ttimed {
	thr := NewThrottlerFixed(max)
	if duration > 0 {
		go func() {
			delta, interval := max, duration
			if quarter > 0 && quarter < uint64(duration) {
				delta, interval = delta/quarter, interval/time.Duration(quarter)
			}
			delta = ^uint64(delta - 1)
			tick := time.NewTicker(interval)
			defer tick.Stop()
			for {
				<-tick.C
				atomic.AddUint64(&thr.cur, delta)
			}
		}()
	}
	return ttimed{thr}
}

func (thr ttimed) Acquire(ctx context.Context) (context.Context, error) {
	return thr.tfixed.Acquire(ctx)
}

func (thr ttimed) Release(ctx context.Context) (context.Context, error) {
	return thr.tfixed.Release(ctx)
}

type tstats struct {
	stats    Stats
	alloc    uint64
	system   uint64
	avgpause uint64
	usage    float64
}

func NewThrottlerStats(stats Stats, alloc uint64, system uint64, avgpause uint64, usage float64) tstats {
	return tstats{
		stats:    stats,
		alloc:    alloc,
		system:   system,
		avgpause: avgpause,
		usage:    usage,
	}
}

func (thr tstats) Acquire(ctx context.Context) (context.Context, error) {
	alloc, system := thr.stats.MEM()
	avgpause, usage := thr.stats.CPU()
	if alloc >= thr.alloc ||
		system >= thr.system ||
		avgpause >= thr.avgpause ||
		usage >= thr.usage {
		return ctx, fmt.Errorf(
			`throttler memory watermark limit has been exceed
alloc %d mb, system %d mb, average collector pause %s`,
			alloc/1024,
			system/1024,
			time.Duration(avgpause),
		)
	}
	return ctx, nil
}

func (thr tstats) Release(ctx context.Context) (context.Context, error) {
	return ctx, nil
}

const gohaltctxlatency = "gohalt_context_latency"

type tlatency struct {
	lat uint64
	max uint64
}

func NewThrottlerLatency(max time.Duration) *tlatency {
	return &tlatency{max: uint64(max)}
}

func (thr tlatency) Acquire(ctx context.Context) (context.Context, error) {
	if lat := atomic.LoadUint64(&thr.lat); lat > thr.max {
		return ctx, fmt.Errorf("throttler max latency limit has been exceed %d", lat)
	}
	ctx = context.WithValue(ctx, gohaltctxlatency, time.Now().UTC().UnixNano())
	return ctx, nil
}

func (thr *tlatency) Release(ctx context.Context) (context.Context, error) {
	if lat := atomic.LoadUint64(&thr.lat); lat < thr.max {
		if lat := ctx.Value(gohaltctxlatency); lat != nil {
			if ts, ok := lat.(int64); ok {
				lat := uint64(ts - time.Now().UTC().UnixNano())
				if lat > thr.lat {
					atomic.StoreUint64(&thr.lat, lat)
				}
			}
		}
	}
	return ctx, nil
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

func (thr *tkeyed) Acquire(ctx context.Context) (context.Context, error) {
	if key := ctx.Value(gohaltctxkey); key != nil {
		r, _ := thr.store.LoadOrStore(key, thr.newthr())
		return r.(Throttler).Acquire(ctx)
	}
	return ctx, errors.New("keyed throttler can't find any key")
}

func (thr *tkeyed) Release(ctx context.Context) (context.Context, error) {
	if key := ctx.Value(gohaltctxkey); key != nil {
		if r, ok := thr.store.Load(key); ok {
			return r.(Throttler).Release(ctx)
		}
		return ctx, errors.New("throttler has nothing to release")
	}
	return ctx, errors.New("keyed throttler can't find any key")
}
