package gohalt

import (
	"context"
	"errors"
	"fmt"
	"math"
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
		return ctx, fmt.Errorf("throttler periodic skip has been reached %d", cur)
	}
	return ctx, nil
}

func (thr *each) Release(ctx context.Context) (context.Context, error) {
	return ctx, nil
}

type after struct {
	cur uint64
	num uint64
}

func NewThrottlerAfter(num uint64) *after {
	return &after{num: num}
}

func (thr *after) Acquire(ctx context.Context) (context.Context, error) {
	atomic.AddUint64(&thr.cur, 1)
	if cur := atomic.LoadUint64(&thr.cur); cur < thr.num {
		return ctx, fmt.Errorf("throttler boundary has not been reached yet %d", cur)
	}
	return ctx, nil
}

func (thr *after) Release(ctx context.Context) (context.Context, error) {
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

type tblockpriority struct {
	run map[uint8]chan struct{}
	max uint8
	mut sync.Mutex
}

func NewThrottlerBlockingPriority(max uint64, priority uint8) *tblockpriority {
	if priority == 0 {
		priority = 1
	}
	thr := tblockpriority{max: priority}
	sum := float64(priority) / 2 * float64((2 + (priority - 1)))
	koef := uint64(math.Ceil(float64(max) / sum))
	for i := uint8(1); i <= priority; i++ {
		thr.run[i] = make(chan struct{}, uint64(i)*koef)
	}
	return &thr
}

func (thr *tblockpriority) Acquire(ctx context.Context) (context.Context, error) {
	thr.mut.Lock()
	run := thr.run[ctxPriority(ctx, thr.max)]
	thr.mut.Unlock()
	run <- struct{}{}
	return ctx, nil
}

func (thr *tblockpriority) Release(ctx context.Context) (context.Context, error) {
	thr.mut.Lock()
	run := thr.run[ctxPriority(ctx, thr.max)]
	thr.mut.Unlock()
	select {
	case <-run:
	default:
		return ctx, errors.New("throttler has nothing to release")
	}
	return ctx, nil
}

type ttimed struct {
	*tfixed
}

func NewThrottlerTimed(ctx context.Context, max uint64, duration time.Duration, quarter uint64) ttimed {
	thr := NewThrottlerFixed(max)
	delta, interval := max, duration
	if quarter > 0 && duration > time.Duration(quarter) {
		delta = uint64(math.Ceil(float64(delta) / float64(quarter)))
		interval /= time.Duration(quarter)
	}
	loop(ctx, interval, func(ctx context.Context) error {
		atomic.AddUint64(&thr.cur, ^uint64(delta-1))
		return ctx.Err()
	})
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
alloc %d mb, system %d mb, average collector pause %s, average cpu usafe %.2f%%`,
			alloc/1024,
			system/1024,
			time.Duration(avgpause),
			usage,
		)
	}
	return ctx, nil
}

func (thr tstats) Release(ctx context.Context) (context.Context, error) {
	return ctx, nil
}

type tlatency struct {
	lat uint64
	max uint64
	ret time.Duration
}

func NewThrottlerLatency(max time.Duration, retention time.Duration) *tlatency {
	return &tlatency{max: uint64(max), ret: retention}
}

func (thr tlatency) Acquire(ctx context.Context) (context.Context, error) {
	if lat := atomic.LoadUint64(&thr.lat); lat > thr.max {
		return ctx, fmt.Errorf("throttler max latency limit has been exceed %d", lat)
	}
	return withTimestamp(ctx), nil
}

func (thr *tlatency) Release(ctx context.Context) (context.Context, error) {
	if lat := atomic.LoadUint64(&thr.lat); lat < thr.max {
		ts := ctxTimestamp(ctx)
		lat := uint64(ts - time.Now().UTC().UnixNano())
		if lat > thr.lat {
			atomic.StoreUint64(&thr.lat, lat)
		}
		once(ctx, thr.ret, func(context.Context) error {
			atomic.StoreUint64(&thr.lat, 0)
			return nil
		})
	}
	return ctx, nil
}

type tcontext struct{}

func NewThrottlerContext() tcontext {
	return tcontext{}
}

func (thr tcontext) Acquire(ctx context.Context) (context.Context, error) {
	select {
	case <-ctx.Done():
		return ctx, fmt.Errorf("throttler context error occured %w", ctx.Err())
	default:
		return ctx, nil
	}
}

func (thr tcontext) Release(ctx context.Context) (context.Context, error) {
	select {
	case <-ctx.Done():
		return ctx, fmt.Errorf("throttler context error occured %w", ctx.Err())
	default:
		return ctx, nil
	}
}

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
