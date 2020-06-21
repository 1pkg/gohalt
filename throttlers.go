package gohalt

import (
	"container/heap"
	"context"
	"errors"
	"fmt"
	"math"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"
)

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
		return ctx, fmt.Errorf("throttler has reached periodic skip %d", cur)
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
		return ctx, fmt.Errorf("throttler has not reached pass yet %d", cur)
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
		return ctx, fmt.Errorf("throttler has exceed fixed limit %d", cur)
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
		return ctx, fmt.Errorf("throttler has exceed running limit %d", run)
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
			`throttler has exceed stats limits
alloc %d mb, system %d mb, avg gc cpu pause %s, avg cpu usage %.2f%%`,
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

type tchance struct {
	pos float64
}

func NewThrottlerChance(possibillity float64) tchance {
	possibillity = math.Abs(possibillity)
	if possibillity > 1.0 {
		possibillity = 1.0
	}
	return tchance{pos: possibillity}
}

func (thr tchance) Acquire(ctx context.Context) (context.Context, error) {
	if thr.pos > 1.0-rand.Float64() {
		return ctx, errors.New("throttler has missed a chance")
	}
	return ctx, nil
}

func (thr tchance) Release(ctx context.Context) (context.Context, error) {
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
		return ctx, fmt.Errorf("throttler has exceed latency limit %s", time.Duration(lat))
	}
	return withTimestamp(ctx), nil
}

func (thr *tlatency) Release(ctx context.Context) (context.Context, error) {
	if lat := atomic.LoadUint64(&thr.lat); lat < thr.max {
		lat := uint64(ctxTimestamp(ctx) - time.Now().UTC().UnixNano())
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

type tquantile struct {
	lat *latheap
	mut sync.Mutex
	qnt float64
	max uint64
	ret time.Duration
}

func NewThrottlerQuantile(max time.Duration, quantile float64, retention time.Duration) *tquantile {
	quantile = math.Abs(quantile)
	if quantile > 1.0 {
		quantile = 1.0
	}
	return &tquantile{
		lat: &latheap{},
		max: uint64(max),
		qnt: quantile,
		ret: retention,
	}
}

func (thr *tquantile) Acquire(ctx context.Context) (context.Context, error) {
	thr.mut.Lock()
	size := float64(thr.lat.Len())
	pos := int(math.Round(size * thr.qnt))
	lat := thr.lat.At(pos)
	thr.mut.Unlock()
	if lat > thr.max {
		once(ctx, thr.ret, func(context.Context) error {
			thr.mut.Lock()
			thr.lat = &latheap{}
			thr.mut.Unlock()
			return nil
		})
		return ctx, fmt.Errorf("throttler has exceed latency limit %s", time.Duration(lat))
	}
	return withTimestamp(ctx), nil
}

func (thr *tquantile) Release(ctx context.Context) (context.Context, error) {
	lat := uint64(ctxTimestamp(ctx) - time.Now().UTC().UnixNano())
	thr.mut.Lock()
	heap.Push(thr.lat, lat)
	thr.mut.Unlock()
	return ctx, nil
}

type tcontext struct{}

func NewThrottlerContext() tcontext {
	return tcontext{}
}

func (thr tcontext) Acquire(ctx context.Context) (context.Context, error) {
	select {
	case <-ctx.Done():
		return ctx, fmt.Errorf("throttler context error has occured %w", ctx.Err())
	default:
		return ctx, nil
	}
}

func (thr tcontext) Release(ctx context.Context) (context.Context, error) {
	select {
	case <-ctx.Done():
		return ctx, fmt.Errorf("throttler context error has occured %w", ctx.Err())
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
	if key := ctxKey(ctx); key != nil {
		r, _ := thr.store.LoadOrStore(key, thr.newthr())
		return r.(Throttler).Acquire(ctx)
	}
	return ctx, errors.New("keyed throttler can't the key")
}

func (thr *tkeyed) Release(ctx context.Context) (context.Context, error) {
	if key := ctxKey(ctx); key != nil {
		if r, ok := thr.store.Load(key); ok {
			return r.(Throttler).Release(ctx)
		}
		return ctx, errors.New("throttler has nothing to release")
	}
	return ctx, errors.New("keyed throttler can't find any key")
}

type tall struct {
	thrs []Throttler
}

func NewThrottlerAll(thrs []Throttler) tall {
	return tall{thrs: thrs}
}

func (thr tall) Acquire(ctx context.Context) (context.Context, error) {
	var err error
	for _, thr := range thr.thrs {
		thrctx, threrr := thr.Acquire(ctx)
		if threrr == nil {
			return ctx, nil
		}
		err = fmt.Errorf("%w %w", err, threrr)
		ctx = thrctx
	}
	return ctx, err
}

func (thr tall) Release(ctx context.Context) (context.Context, error) {
	err := errors.New("throttler error happened")
	for _, thr := range thr.thrs {
		thrctx, threrr := thr.Release(ctx)
		if threrr == nil {
			return ctx, nil
		}
		err = fmt.Errorf("%w\n%w", err, threrr)
		ctx = thrctx
	}
	return ctx, err
}

type tany struct {
	thrs []Throttler
}

func NewThrottlerAny(thrs []Throttler) tany {
	return tany{thrs: thrs}
}

func (thr tany) Acquire(ctx context.Context) (context.Context, error) {
	var wg sync.WaitGroup
	results := make(chan ctxerr)
	for _, thr := range thr.thrs {
		wg.Add(1)
		go func(thr Throttler) {
			ctx, err := thr.Acquire(ctx)
			results <- ctxerr{ctx: ctx, err: err}
			wg.Done()
		}(thr)
	}
	mctx, err := multi{}, errors.New("throttler error happened")
	go func() {
		for result := range results {
			mctx = append(mctx, result.ctx)
			err = fmt.Errorf("%w\n%w", err, result.err)
		}
	}()
	wg.Wait()
	close(results)
	return ctx, err
}

func (thr tany) Release(ctx context.Context) (context.Context, error) {
	var wg sync.WaitGroup
	results := make(chan ctxerr)
	for _, thr := range thr.thrs {
		wg.Add(1)
		go func(thr Throttler) {
			ctx, err := thr.Release(ctx)
			results <- ctxerr{ctx: ctx, err: err}
			wg.Done()
		}(thr)
	}
	mctx, err := multi{}, errors.New("throttler error happened")
	go func() {
		for result := range results {
			mctx = append(mctx, result.ctx)
			err = fmt.Errorf("%w\n%w", err, result.err)
		}
	}()
	wg.Wait()
	close(results)
	return ctx, err
}
