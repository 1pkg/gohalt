package gohalt

import (
	"container/heap"
	"context"
	"errors"
	"fmt"
	"log"
	"math"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"
)

type Throttler interface {
	accept(context.Context, tvisitor) interface{}
	Acquire(context.Context) error
	Release(context.Context) error
}

type tvisitor interface {
	tvisitEcho(context.Context, techo) interface{}
	tvisitWait(context.Context, twait) interface{}
	tvisitPanic(context.Context, tpanic) interface{}
	tvisitEach(context.Context, teach) interface{}
	tvisitAfter(context.Context, tafter) interface{}
	tvisitChance(context.Context, tchance) interface{}
	tvisitFixed(context.Context, tfixed) interface{}
	tvisitRunning(context.Context, trunning) interface{}
	tvisitBuffered(context.Context, tbuffered) interface{}
	tvisitPriority(context.Context, tpriority) interface{}
	tvisitTimed(context.Context, ttimed) interface{}
	tvisitMonitor(context.Context, tmonitor) interface{}
	tvisitMetric(context.Context, tmetric) interface{}
	tvisitLatency(context.Context, tlatency) interface{}
	tvisitPercentile(context.Context, tpercentile) interface{}
	tvisitAdaptive(context.Context, tadaptive) interface{}
	tvisitContext(context.Context, tcontext) interface{}
	tvisitEnqueue(context.Context, tenqueue) interface{}
	tvisitKeyed(context.Context, tkeyed) interface{}
	tvisitAll(context.Context, tall) interface{}
	tvisitAny(context.Context, tany) interface{}
	tvisitNot(context.Context, tnot) interface{}
}

type techo struct {
	err error
}

func NewThrottlerEcho(err error) techo {
	return techo{err: err}
}

func (thr techo) accept(ctx context.Context, v tvisitor) interface{} {
	return v.tvisitEcho(ctx, thr)
}

func (thr techo) Acquire(context.Context) error {
	return thr.err
}

func (thr techo) Release(context.Context) error {
	return thr.err
}

type twait struct {
	duration time.Duration
}

func NewThrottlerWait(duration time.Duration) twait {
	return twait{duration: duration}
}

func (thr twait) accept(ctx context.Context, v tvisitor) interface{} {
	return v.tvisitWait(ctx, thr)
}

func (thr twait) Acquire(context.Context) error {
	time.Sleep(thr.duration)
	return nil
}

func (thr twait) Release(context.Context) error {
	return nil
}

type tpanic struct{}

func NewThrottlerPanic() tpanic {
	return tpanic{}
}

func (thr tpanic) accept(ctx context.Context, v tvisitor) interface{} {
	return v.tvisitPanic(ctx, thr)
}

func (thr tpanic) Acquire(context.Context) error {
	log.Fatal("throttler panic has happened")
	return nil
}

func (thr tpanic) Release(context.Context) error {
	return nil
}

type teach struct {
	current   uint64
	threshold uint64
}

func NewThrottlerEach(threshold uint64) *teach {
	return &teach{threshold: threshold}
}

func (thr teach) accept(ctx context.Context, v tvisitor) interface{} {
	return v.tvisitEach(ctx, thr)
}

func (thr *teach) Acquire(context.Context) error {
	atomic.AddUint64(&thr.current, 1)
	if current := atomic.LoadUint64(&thr.current); current%thr.threshold == 0 {
		return fmt.Errorf("throttler has reached periodic skip %d", current)
	}
	return nil
}

func (thr *teach) Release(context.Context) error {
	return nil
}

type tafter struct {
	current   uint64
	threshold uint64
}

func NewThrottlerAfter(threshold uint64) *tafter {
	return &tafter{threshold: threshold}
}

func (thr tafter) accept(ctx context.Context, v tvisitor) interface{} {
	return v.tvisitAfter(ctx, thr)
}

func (thr *tafter) Acquire(context.Context) error {
	atomic.AddUint64(&thr.current, 1)
	if current := atomic.LoadUint64(&thr.current); current < thr.threshold {
		return fmt.Errorf("throttler has not reached pass yet %d", current)
	}
	return nil
}

func (thr *tafter) Release(context.Context) error {
	return nil
}

type tchance struct {
	percentage float64
}

func NewThrottlerChance(percentage float64) tchance {
	percentage = math.Abs(percentage)
	if percentage > 1.0 {
		percentage = 1.0
	}
	return tchance{percentage: percentage}
}

func (thr tchance) accept(ctx context.Context, v tvisitor) interface{} {
	return v.tvisitChance(ctx, thr)
}

func (thr tchance) Acquire(context.Context) error {
	if thr.percentage > 1.0-rand.Float64() {
		return errors.New("throttler has missed a chance")
	}
	return nil
}

func (thr tchance) Release(context.Context) error {
	return nil
}

type tfixed struct {
	current uint64
	limit   uint64
}

func NewThrottlerFixed(limit uint64) *tfixed {
	return &tfixed{limit: limit}
}

func (thr tfixed) accept(ctx context.Context, v tvisitor) interface{} {
	return v.tvisitFixed(ctx, thr)
}

func (thr *tfixed) Acquire(context.Context) error {
	if current := atomic.LoadUint64(&thr.current); current > thr.limit {
		return fmt.Errorf("throttler has exceed fixed limit %d", current)
	}
	atomic.AddUint64(&thr.current, 1)
	return nil
}

func (thr *tfixed) Release(context.Context) error {
	return nil
}

type trunning struct {
	running uint64
	limit   uint64
}

func NewThrottlerRunning(limit uint64) *trunning {
	return &trunning{limit: limit}
}

func (thr trunning) accept(ctx context.Context, v tvisitor) interface{} {
	return v.tvisitRunning(ctx, thr)
}

func (thr *trunning) Acquire(context.Context) error {
	if running := atomic.LoadUint64(&thr.running); running > thr.limit {
		return fmt.Errorf("throttler has exceed running limit %d", running)
	}
	atomic.AddUint64(&thr.running, 1)
	return nil
}

func (thr *trunning) Release(context.Context) error {
	if running := atomic.LoadUint64(&thr.running); running <= 0 {
		return errors.New("throttler has nothing to release")
	}
	atomic.AddUint64(&thr.running, ^uint64(0))
	return nil
}

type tbuffered struct {
	running chan struct{}
}

func NewThrottlerBuffered(size uint64) *tbuffered {
	return &tbuffered{running: make(chan struct{}, size)}
}

func (thr tbuffered) accept(ctx context.Context, v tvisitor) interface{} {
	return v.tvisitBuffered(ctx, thr)
}

func (thr *tbuffered) Acquire(context.Context) error {
	thr.running <- struct{}{}
	return nil
}

func (thr *tbuffered) Release(ctx context.Context) error {
	select {
	case <-thr.running:
		return nil
	case <-ctx.Done():
		return fmt.Errorf("throttler context error has happened %w", ctx.Err())
	default:
		return errors.New("throttler has nothing to release")
	}
}

type tpriority struct {
	running *sync.Map
	size    uint64
	limit   uint8
}

func NewThrottlerPriority(size uint64, limit uint8) tpriority {
	if limit == 0 {
		limit = 1
	}
	running := &sync.Map{}
	sum := float64(limit) / 2 * float64((2 + (limit - 1)))
	koef := uint64(math.Ceil(float64(size) / sum))
	for i := uint8(1); i <= limit; i++ {
		running.Store(i, make(chan struct{}, uint64(i)*koef))
	}
	thr := tpriority{size: size, limit: limit}
	thr.running = running
	return thr
}

func (thr tpriority) accept(ctx context.Context, v tvisitor) interface{} {
	return v.tvisitPriority(ctx, thr)
}

func (thr tpriority) Acquire(ctx context.Context) error {
	priority := ctxPriority(ctx, thr.limit)
	val, ok := thr.running.Load(priority)
	if !ok {
		return fmt.Errorf("throttler hasn't found priority %d", priority)
	}
	running := val.(chan struct{})
	running <- struct{}{}
	return nil
}

func (thr tpriority) Release(ctx context.Context) error {
	priority := ctxPriority(ctx, thr.limit)
	val, ok := thr.running.Load(priority)
	if !ok {
		return fmt.Errorf("throttler hasn't found priority %d", priority)
	}
	running := val.(chan struct{})
	select {
	case <-running:
		return nil
	case <-ctx.Done():
		return fmt.Errorf("throttler context error has happened %w", ctx.Err())
	default:
		return errors.New("throttler has nothing to release")
	}
}

type ttimed struct {
	*tfixed
	interval time.Duration
	slide    time.Duration
}

func NewThrottlerTimed(ctx context.Context, limit uint64, interval time.Duration, slide time.Duration) ttimed {
	thr := NewThrottlerFixed(limit)
	delta, window := limit, interval
	if slide > 0 && interval > slide {
		delta = uint64(math.Ceil(float64(delta) / float64(slide)))
		window /= slide
	}
	loop(ctx, window, func(ctx context.Context) error {
		atomic.AddUint64(&thr.current, ^uint64(delta-1))
		if current := atomic.LoadUint64(&thr.current); current < 0 {
			atomic.StoreUint64(&thr.current, 0)
		}
		return ctx.Err()
	})
	return ttimed{tfixed: thr, interval: interval, slide: slide}
}

func (thr ttimed) accept(ctx context.Context, v tvisitor) interface{} {
	return v.tvisitTimed(ctx, thr)
}

func (thr ttimed) Acquire(ctx context.Context) error {
	return thr.tfixed.Acquire(ctx)
}

func (thr ttimed) Release(ctx context.Context) error {
	return thr.tfixed.Release(ctx)
}

type tmonitor struct {
	mnt   Monitor
	limit Stats
}

func NewThrottlerMonitor(mnt Monitor, limit Stats) tmonitor {
	return tmonitor{mnt: mnt, limit: limit}
}

func (thr tmonitor) accept(ctx context.Context, v tvisitor) interface{} {
	return v.tvisitMonitor(ctx, thr)
}

func (thr tmonitor) Acquire(ctx context.Context) error {
	stats, err := thr.mnt.Stats(ctx)
	if err != nil {
		return fmt.Errorf("throttler error has happened %w", err)
	}
	if stats.MemAlloc >= thr.limit.MemAlloc || stats.MemSystem >= thr.limit.MemSystem ||
		stats.CpuPause >= thr.limit.CpuPause || stats.CpuUsage >= thr.limit.CpuUsage {
		return fmt.Errorf(
			`throttler has exceed stats limits
alloc %d mb, system %d mb, avg gc cpu pause %s, avg cpu usage %.2f%%`,
			stats.MemAlloc/1024,
			stats.MemSystem/1024,
			time.Duration(stats.CpuPause),
			stats.CpuUsage,
		)
	}
	return nil
}

func (thr tmonitor) Release(context.Context) error {
	return nil
}

type tmetric struct {
	mtc Metric
}

func NewThrottlerMetric(mtc Metric) tmetric {
	return tmetric{mtc: mtc}
}

func (thr tmetric) accept(ctx context.Context, v tvisitor) interface{} {
	return v.tvisitMetric(ctx, thr)
}

func (thr tmetric) Acquire(ctx context.Context) error {
	val, err := thr.mtc.Query(ctx)
	if err != nil {
		return fmt.Errorf("throttler error has happened %w", err)
	}
	if val {
		return errors.New("throttler metric has been reached")
	}
	return nil
}

func (thr tmetric) Release(context.Context) error {
	return nil
}

type tlatency struct {
	latency   uint64
	limit     time.Duration
	retention time.Duration
}

func NewThrottlerLatency(limit time.Duration, retention time.Duration) *tlatency {
	return &tlatency{limit: limit, retention: retention}
}

func (thr tlatency) accept(ctx context.Context, v tvisitor) interface{} {
	return v.tvisitLatency(ctx, thr)
}

func (thr tlatency) Acquire(context.Context) error {
	if latency := time.Duration(atomic.LoadUint64(&thr.latency)); latency > thr.limit {
		return fmt.Errorf("throttler has exceed latency limit %s", latency)
	}
	return nil
}

func (thr *tlatency) Release(ctx context.Context) error {
	if latency := atomic.LoadUint64(&thr.latency); latency < uint64(thr.limit) {
		latency := uint64(ctxTimestamp(ctx) - time.Now().UTC().UnixNano())
		atomic.StoreUint64(&thr.latency, latency)
		once(ctx, thr.retention, func(context.Context) error {
			atomic.StoreUint64(&thr.latency, 0)
			return nil
		})
	}
	return nil
}

type tpercentile struct {
	latencies  *blatheap
	limit      time.Duration
	percentile float64
	retention  time.Duration
}

func NewThrottlerPercentile(limit time.Duration, percentile float64, retention time.Duration) *tpercentile {
	percentile = math.Abs(percentile)
	if percentile > 1.0 {
		percentile = 1.0
	}
	return &tpercentile{
		latencies:  &blatheap{},
		limit:      limit,
		percentile: percentile,
		retention:  retention,
	}
}

func (thr tpercentile) accept(ctx context.Context, v tvisitor) interface{} {
	return v.tvisitPercentile(ctx, thr)
}

func (thr *tpercentile) Acquire(ctx context.Context) error {
	at := int(math.Round(float64(thr.latencies.Len()) * thr.percentile))
	if latency := time.Duration(thr.latencies.At(at)); latency > thr.limit {
		once(ctx, thr.retention, func(context.Context) error {
			thr.latencies.Prune()
			return nil
		})
		return fmt.Errorf("throttler has exceed latency limit %s", latency)
	}
	return nil
}

func (thr *tpercentile) Release(ctx context.Context) error {
	latency := ctxTimestamp(ctx) - time.Now().UTC().UnixNano()
	heap.Push(thr.latencies, latency)
	return nil
}

type tadaptive struct {
	ttimed
	step uint64
	thr  Throttler
}

func NewThrottlerAdaptive(
	ctx context.Context,
	limit uint64,
	interval time.Duration,
	slide time.Duration,
	step uint64,
	thr Throttler,
) tadaptive {
	return tadaptive{
		ttimed: NewThrottlerTimed(ctx, limit, interval, slide),
		step:   step,
		thr:    thr,
	}
}

func (thr tadaptive) accept(ctx context.Context, v tvisitor) interface{} {
	return v.tvisitAdaptive(ctx, thr)
}

func (thr tadaptive) Acquire(ctx context.Context) error {
	err := thr.thr.Acquire(ctx)
	if err != nil {
		atomic.AddUint64(&thr.ttimed.limit, ^uint64(thr.step*thr.step))
	} else {
		atomic.AddUint64(&thr.ttimed.limit, thr.step)
	}
	return thr.ttimed.Acquire(ctx)
}

func (thr tadaptive) Release(ctx context.Context) error {
	return thr.ttimed.Release(ctx)
}

type tcontext struct{}

func NewThrottlerContext() tcontext {
	return tcontext{}
}

func (thr tcontext) accept(ctx context.Context, v tvisitor) interface{} {
	return v.tvisitContext(ctx, thr)
}

func (thr tcontext) Acquire(ctx context.Context) error {
	select {
	case <-ctx.Done():
		return fmt.Errorf("throttler context error has happened %w", ctx.Err())
	default:
		return nil
	}
}

func (thr tcontext) Release(ctx context.Context) error {
	select {
	case <-ctx.Done():
		return fmt.Errorf("throttler context error has happened %w", ctx.Err())
	default:
		return nil
	}
}

type tenqueue struct {
	enq Enqueuer
}

func NewThrottlerEnqueue(enq Enqueuer) tenqueue {
	return tenqueue{enq: enq}
}

func (thr tenqueue) accept(ctx context.Context, v tvisitor) interface{} {
	return v.tvisitEnqueue(ctx, thr)
}

func (thr tenqueue) Acquire(ctx context.Context) error {
	if marshaler, data := ctxMarshaler(ctx), ctxData(ctx); marshaler != nil && data != nil {
		message, err := marshaler(data)
		if err != nil {
			return fmt.Errorf("throttler can't enqueue %w", err)
		}
		if err := thr.enq.Enqueue(ctx, message); err != nil {
			return fmt.Errorf("throttler can't enqueue %w", err)
		}
		return nil
	}
	return errors.New("throttler can't find any data")
}

func (thr tenqueue) Release(ctx context.Context) error {
	return nil
}

type tkeyed struct {
	keys *sync.Map
	gen  Generator
}

func NewThrottlerKeyed(gen Generator) tkeyed {
	return tkeyed{keys: &sync.Map{}, gen: gen}
}

func (thr tkeyed) accept(ctx context.Context, v tvisitor) interface{} {
	return v.tvisitKeyed(ctx, thr)
}

func (thr tkeyed) Acquire(ctx context.Context) error {
	if key := ctxKey(ctx); key != nil {
		r, _ := thr.keys.LoadOrStore(key, thr.gen.Generate(ctx, key))
		return r.(Throttler).Acquire(ctx)
	}
	return errors.New("throttler can't find any key")
}

func (thr tkeyed) Release(ctx context.Context) error {
	if key := ctxKey(ctx); key != nil {
		if r, ok := thr.keys.Load(key); ok {
			return r.(Throttler).Release(ctx)
		}
		return errors.New("throttler has nothing to release")
	}
	return errors.New("throttler can't find any key")
}

type tall []Throttler

func NewThrottlerAll(thrs ...Throttler) tall {
	return tall(thrs)
}

func (thr tall) accept(ctx context.Context, v tvisitor) interface{} {
	return v.tvisitAll(ctx, thr)
}

func (thrs tall) Acquire(ctx context.Context) error {
	err := errors.New("throttler error has happened")
	for _, thr := range thrs {
		if threrr := thr.Acquire(ctx); threrr != nil {
			err = fmt.Errorf("%w %w", err, threrr)
			continue
		}
		return nil
	}
	return err
}

func (thrs tall) Release(ctx context.Context) error {
	err := errors.New("throttler error has happened")
	for _, thr := range thrs {
		if threrr := thr.Release(ctx); threrr != nil {
			err = fmt.Errorf("%w %w", err, threrr)
			continue
		}
		return nil
	}
	return err
}

type tany []Throttler

func NewThrottlerAny(thrs ...Throttler) tany {
	return tany(thrs)
}

func (thr tany) accept(ctx context.Context, v tvisitor) interface{} {
	return v.tvisitAny(ctx, thr)
}

func (thrs tany) Acquire(ctx context.Context) error {
	var wg sync.WaitGroup
	errs := make(chan error)
	for _, thr := range thrs {
		wg.Add(1)
		go func(thr Throttler) {
			if err := thr.Acquire(ctx); err != nil {
				errs <- err
			}
			wg.Done()
		}(thr)
	}
	err := errors.New("throttler error has happened")
	go func() {
		for threrr := range errs {
			err = fmt.Errorf("%w\n%w", err, threrr)
		}
	}()
	wg.Wait()
	close(errs)
	return err
}

func (thrs tany) Release(ctx context.Context) error {
	var wg sync.WaitGroup
	errs := make(chan error)
	for _, thr := range thrs {
		wg.Add(1)
		go func(thr Throttler) {
			if err := thr.Release(ctx); err != nil {
				errs <- err
			}
			wg.Done()
		}(thr)
	}
	err := errors.New("throttler error has happened")
	go func() {
		for threrr := range errs {
			err = fmt.Errorf("%w\n%w", err, threrr)
		}
	}()
	wg.Wait()
	close(errs)
	return err
}

type tnot struct {
	thr Throttler
}

func NewThrottlerNot(thr Throttler) tnot {
	return tnot{thr: thr}
}

func (thr tnot) accept(ctx context.Context, v tvisitor) interface{} {
	return v.tvisitNot(ctx, thr)
}

func (thr tnot) Acquire(ctx context.Context) error {
	if err := thr.thr.Acquire(ctx); err != nil {
		return nil
	}
	return errors.New("throttler error has happened")
}

func (thr tnot) Release(ctx context.Context) error {
	if err := thr.thr.Release(ctx); err != nil {
		return nil
	}
	return errors.New("throttler error has happened")
}
