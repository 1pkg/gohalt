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
	accept(context.Context, tvisitor)
	Acquire(context.Context) error
	Release(context.Context) error
}

type tvisitor interface {
	tvisitEcho(context.Context, *techo)
	tvisitWait(context.Context, *twait)
	tvisitPanic(context.Context, *tpanic)
	tvisitEach(context.Context, *teach)
	tvisitBefore(context.Context, *tbefore)
	tvisitChance(context.Context, *tchance)
	tvisitAfter(context.Context, *tafter)
	tvisitRunning(context.Context, *trunning)
	tvisitBuffered(context.Context, *tbuffered)
	tvisitPriority(context.Context, *tpriority)
	tvisitTimed(context.Context, *ttimed)
	tvisitMonitor(context.Context, *tmonitor)
	tvisitMetric(context.Context, *tmetric)
	tvisitLatency(context.Context, *tlatency)
	tvisitPercentile(context.Context, *tpercentile)
	tvisitAdaptive(context.Context, *tadaptive)
	tvisitContext(context.Context, *tcontext)
	tvisitEnqueue(context.Context, *tenqueue)
	tvisitKeyed(context.Context, *tkeyed)
	tvisitAll(context.Context, *tall)
	tvisitAny(context.Context, *tany)
	tvisitNot(context.Context, *tnot)
	tvisitSuppress(context.Context, *tsuppress)
}

type techo struct {
	err error
}

func NewThrottlerEcho(err error) techo {
	return techo{err: err}
}

func (thr techo) accept(ctx context.Context, v tvisitor) {
	v.tvisitEcho(ctx, &thr)
}

func (thr techo) Acquire(context.Context) error {
	return thr.err
}

func (thr techo) Release(context.Context) error {
	return nil
}

type twait struct {
	duration time.Duration
}

func NewThrottlerWait(duration time.Duration) twait {
	return twait{duration: duration}
}

func (thr twait) accept(ctx context.Context, v tvisitor) {
	v.tvisitWait(ctx, &thr)
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

func (thr tpanic) accept(ctx context.Context, v tvisitor) {
	v.tvisitPanic(ctx, &thr)
}

func (thr tpanic) Acquire(context.Context) error {
	panic("throttler has reached panic")
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

func (thr *teach) accept(ctx context.Context, v tvisitor) {
	v.tvisitEach(ctx, thr)
}

func (thr *teach) Acquire(context.Context) error {
	if current := atomic.AddUint64(&thr.current, 1); current%thr.threshold == 0 {
		return errors.New("throttler has reached periodic threshold")
	}
	return nil
}

func (thr *teach) Release(context.Context) error {
	return nil
}

type tbefore struct {
	current   uint64
	threshold uint64
}

func NewThrottlerBefore(threshold uint64) *tbefore {
	return &tbefore{threshold: threshold}
}

func (thr *tbefore) accept(ctx context.Context, v tvisitor) {
	v.tvisitBefore(ctx, thr)
}

func (thr *tbefore) Acquire(context.Context) error {
	if current := atomic.AddUint64(&thr.current, 1); current <= thr.threshold {
		return errors.New("throttler has not reached threshold yet")
	}
	return nil
}

func (thr *tbefore) Release(context.Context) error {
	return nil
}

type tchance struct {
	threshold float64
}

func NewThrottlerChance(threshold float64) tchance {
	threshold = math.Abs(threshold)
	if threshold > 1.0 {
		threshold = 1.0
	}
	return tchance{threshold: threshold}
}

func (thr tchance) accept(ctx context.Context, v tvisitor) {
	v.tvisitChance(ctx, &thr)
}

func (thr tchance) Acquire(context.Context) error {
	if thr.threshold > 1.0-rand.Float64() {
		return errors.New("throttler has reached chance threshold")
	}
	return nil
}

func (thr tchance) Release(context.Context) error {
	return nil
}

type tafter struct {
	current   uint64
	threshold uint64
}

func NewThrottlerAfter(threshold uint64) *tafter {
	return &tafter{threshold: threshold}
}

func (thr *tafter) accept(ctx context.Context, v tvisitor) {
	v.tvisitAfter(ctx, thr)
}

func (thr *tafter) Acquire(context.Context) error {
	if current := atomic.AddUint64(&thr.current, 1); current > thr.threshold {
		return errors.New("throttler has exceed threshold")
	}
	return nil
}

func (thr *tafter) Release(context.Context) error {
	return nil
}

type trunning struct {
	running   uint64
	threshold uint64
}

func NewThrottlerRunning(threshold uint64) *trunning {
	return &trunning{threshold: threshold}
}

func (thr *trunning) accept(ctx context.Context, v tvisitor) {
	v.tvisitRunning(ctx, thr)
}

func (thr *trunning) Acquire(context.Context) error {
	if running := atomic.AddUint64(&thr.running, 1); running > thr.threshold {
		return errors.New("throttler has exceed running threshold")
	}
	return nil
}

func (thr *trunning) Release(context.Context) error {
	if running := atomic.AddUint64(&thr.running, ^uint64(0)); int64(running) < 0 {
		// fix running discrepancies
		atomic.AddUint64(&thr.running, 1)
	}
	return nil
}

type tbuffered struct {
	running chan struct{}
}

func NewThrottlerBuffered(threshold uint64) *tbuffered {
	return &tbuffered{running: make(chan struct{}, threshold)}
}

func (thr *tbuffered) accept(ctx context.Context, v tvisitor) {
	v.tvisitBuffered(ctx, thr)
}

func (thr *tbuffered) Acquire(context.Context) error {
	thr.running <- struct{}{}
	return nil
}

func (thr *tbuffered) Release(ctx context.Context) error {
	select {
	case <-thr.running:
		return nil
	default:
		return nil
	}
}

type tpriority struct {
	running   *sync.Map
	threshold uint64
	levels    uint8
}

func NewThrottlerPriority(threshold uint64, levels uint8) tpriority {
	if levels == 0 {
		levels = 1
	}
	running := &sync.Map{}
	koef := float64(threshold) / (float64(levels) / 2 * float64((2 + (levels - 1))))
	for i := uint8(1); i <= levels; i++ {
		slots := uint64(math.Round(float64(i) * koef))
		running.Store(i, make(chan struct{}, slots))
	}
	return tpriority{running: running, threshold: threshold, levels: levels}
}

func (thr tpriority) accept(ctx context.Context, v tvisitor) {
	v.tvisitPriority(ctx, &thr)
}

func (thr tpriority) Acquire(ctx context.Context) error {
	priority := ctxPriority(ctx, thr.levels)
	val, _ := thr.running.Load(priority)
	running := val.(chan struct{})
	running <- struct{}{}
	return nil
}

func (thr tpriority) Release(ctx context.Context) error {
	priority := ctxPriority(ctx, thr.levels)
	val, _ := thr.running.Load(priority)
	running := val.(chan struct{})
	select {
	case <-running:
		return nil
	default:
		return nil
	}
}

type ttimed struct {
	*tafter
	loop     Runnable
	interval time.Duration
	quantum  time.Duration
}

func NewThrottlerTimed(threshold uint64, interval time.Duration, quantum time.Duration) ttimed {
	thr := NewThrottlerAfter(threshold)
	delta, window := threshold, interval
	if quantum > 0 && interval > quantum {
		delta = uint64(math.Ceil(float64(delta) / float64(quantum)))
		window /= quantum
	}
	loop := once(
		loop(window, func(ctx context.Context) error {
			if current := atomic.AddUint64(&thr.current, ^(delta - 1)); int64(current) < 0 {
				// fix running discrepancies
				atomic.StoreUint64(&thr.current, 0)
			}
			return ctx.Err()
		}),
	)
	return ttimed{tafter: thr, loop: loop, interval: interval, quantum: quantum}
}

func (thr ttimed) accept(ctx context.Context, v tvisitor) {
	v.tvisitTimed(ctx, &thr)
}

func (thr ttimed) Acquire(ctx context.Context) error {
	// start loop on first acquire
	gorun(ctx, thr.loop)
	return thr.tafter.Acquire(ctx)
}

func (thr ttimed) Release(ctx context.Context) error {
	return thr.tafter.Release(ctx)
}

type tmonitor struct {
	mnt       Monitor
	threshold Stats
}

func NewThrottlerMonitor(mnt Monitor, threshold Stats) tmonitor {
	return tmonitor{mnt: mnt, threshold: threshold}
}

func (thr tmonitor) accept(ctx context.Context, v tvisitor) {
	v.tvisitMonitor(ctx, &thr)
}

func (thr tmonitor) Acquire(ctx context.Context) error {
	stats, err := thr.mnt.Stats(ctx)
	if err != nil {
		return fmt.Errorf("throttler hasn't found any stats %w", err)
	}
	if stats.MEMAlloc >= thr.threshold.MEMAlloc || stats.MEMSystem >= thr.threshold.MEMSystem ||
		stats.CPUPause >= thr.threshold.CPUPause || stats.CPUUsage >= thr.threshold.CPUUsage {
		return errors.New("throttler has exceed stats threshold")
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

func (thr tmetric) accept(ctx context.Context, v tvisitor) {
	v.tvisitMetric(ctx, &thr)
}

func (thr tmetric) Acquire(ctx context.Context) error {
	val, err := thr.mtc.Query(ctx)
	if err != nil {
		return fmt.Errorf("throttler hasn't found any metric %w", err)
	}
	if val {
		return errors.New("throttler has reached metric threshold")
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

func (thr *tlatency) accept(ctx context.Context, v tvisitor) {
	v.tvisitLatency(ctx, thr)
}

func (thr tlatency) Acquire(context.Context) error {
	if latency := time.Duration(atomic.LoadUint64(&thr.latency)); latency > thr.limit {
		return errors.New("throttler has exceed latency threshold")
	}
	return nil
}

func (thr *tlatency) Release(ctx context.Context) error {
	if latency := atomic.LoadUint64(&thr.latency); latency < uint64(thr.limit) {
		latency := uint64(ctxTimestamp(ctx) - time.Now().UTC().UnixNano())
		atomic.StoreUint64(&thr.latency, latency)
		gorun(ctx, delayed(thr.retention, func(context.Context) error {
			atomic.StoreUint64(&thr.latency, 0)
			return nil
		}))
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

func (thr *tpercentile) accept(ctx context.Context, v tvisitor) {
	v.tvisitPercentile(ctx, thr)
}

func (thr *tpercentile) Acquire(ctx context.Context) error {
	at := int(math.Round(float64(thr.latencies.Len()) * thr.percentile))
	if latency := time.Duration(thr.latencies.At(at)); latency > thr.limit {
		gorun(ctx, delayed(thr.retention, func(context.Context) error {
			thr.latencies.Prune()
			return nil
		}))
		return errors.New("throttler has exceed latency threshold")
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
	limit uint64,
	interval time.Duration,
	quantum time.Duration,
	step uint64,
	thr Throttler,
) *tadaptive {
	return &tadaptive{
		ttimed: NewThrottlerTimed(limit, interval, quantum),
		step:   step,
		thr:    thr,
	}
}

func (thr *tadaptive) accept(ctx context.Context, v tvisitor) {
	v.tvisitAdaptive(ctx, thr)
}

func (thr *tadaptive) Acquire(ctx context.Context) error {
	err := thr.thr.Acquire(ctx)
	if err != nil {
		atomic.AddUint64(&thr.ttimed.threshold, ^(thr.step*thr.step - 1))
	} else {
		atomic.AddUint64(&thr.ttimed.threshold, thr.step)
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

func (thr tcontext) accept(ctx context.Context, v tvisitor) {
	v.tvisitContext(ctx, &thr)
}

func (thr tcontext) Acquire(ctx context.Context) error {
	select {
	case <-ctx.Done():
		return fmt.Errorf("throttler has received context error %w", ctx.Err())
	default:
		return nil
	}
}

func (thr tcontext) Release(ctx context.Context) error {
	return nil
}

type tenqueue struct {
	enq Enqueuer
}

func NewThrottlerEnqueue(enq Enqueuer) tenqueue {
	return tenqueue{enq: enq}
}

func (thr tenqueue) accept(ctx context.Context, v tvisitor) {
	v.tvisitEnqueue(ctx, &thr)
}

func (thr tenqueue) Acquire(ctx context.Context) error {
	marshaler, data := ctxMarshaler(ctx), ctxData(ctx)
	if marshaler == nil || data == nil {
		return errors.New("throttler hasn't found any message")
	}
	message, err := marshaler(data)
	if err != nil {
		return fmt.Errorf("throttler hasn't sent any message %w", err)
	}
	if err := thr.enq.Enqueue(ctx, message); err != nil {
		return fmt.Errorf("throttler hasn't sent any message %w", err)
	}
	return nil
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

func (thr tkeyed) accept(ctx context.Context, v tvisitor) {
	v.tvisitKeyed(ctx, &thr)
}

func (thr tkeyed) Acquire(ctx context.Context) error {
	if key := ctxKey(ctx); key != nil {
		r, _ := thr.keys.LoadOrStore(key, thr.gen.Generate(ctx, key))
		return r.(Throttler).Acquire(ctx)
	}
	return errors.New("throttler hasn't found any key")
}

func (thr tkeyed) Release(ctx context.Context) error {
	if key := ctxKey(ctx); key != nil {
		if r, ok := thr.keys.Load(key); ok {
			return r.(Throttler).Release(ctx)
		}
	}
	return nil
}

type tall []Throttler

func NewThrottlerAll(thrs ...Throttler) tall {
	return tall(thrs)
}

func (thrs tall) accept(ctx context.Context, v tvisitor) {
	v.tvisitAll(ctx, &thrs)
}

func (thrs tall) Acquire(ctx context.Context) error {
	for _, thr := range thrs {
		if err := thr.Acquire(ctx); err == nil {
			return nil
		}
	}
	return errors.New("throttler has received one derived error")
}

func (thrs tall) Release(ctx context.Context) error {
	for _, thr := range thrs {
		if err := thr.Release(ctx); err == nil {
			return nil
		}
	}
	return nil
}

type tany []Throttler

func NewThrottlerAny(thrs ...Throttler) tany {
	return tany(thrs)
}

func (thrs tany) accept(ctx context.Context, v tvisitor) {
	v.tvisitAny(ctx, &thrs)
}

func (thrs tany) Acquire(ctx context.Context) error {
	errch := make(chan error, len(thrs))
	var wg sync.WaitGroup
	for _, thr := range thrs {
		wg.Add(1)
		go func(thr Throttler) {
			if err := thr.Acquire(ctx); err != nil {
				errch <- errors.New("throttler has received multiple derived errors")
			}
			wg.Done()
		}(thr)
	}
	wg.Wait()
	close(errch)
	for err := range errch {
		return err
	}
	return nil
}

func (thrs tany) Release(ctx context.Context) error {
	var wg sync.WaitGroup
	for _, thr := range thrs {
		wg.Add(1)
		go func(thr Throttler) {
			_ = thr.Release(ctx)
			wg.Done()
		}(thr)
	}
	wg.Wait()
	return nil
}

type tnot struct {
	thr Throttler
}

func NewThrottlerNot(thr Throttler) tnot {
	return tnot{thr: thr}
}

func (thr tnot) accept(ctx context.Context, v tvisitor) {
	v.tvisitNot(ctx, &thr)
}

func (thr tnot) Acquire(ctx context.Context) error {
	if err := thr.thr.Acquire(ctx); err != nil {
		return nil
	}
	return errors.New("throttler hasn't received any derived errors")
}

func (thr tnot) Release(ctx context.Context) error {
	_ = thr.thr.Release(ctx)
	return nil
}

type tsuppress struct {
	thr Throttler
}

func NewThrottlerSuppress(thr Throttler) tsuppress {
	return tsuppress{thr: thr}
}

func (thr tsuppress) accept(ctx context.Context, v tvisitor) {
	v.tvisitSuppress(ctx, &thr)
}

func (thr tsuppress) Acquire(ctx context.Context) error {
	_ = thr.thr.Acquire(ctx)
	return nil
}

func (thr tsuppress) Release(ctx context.Context) error {
	_ = thr.thr.Release(ctx)
	return nil
}
