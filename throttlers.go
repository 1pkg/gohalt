package gohalt

import (
	"context"
	"errors"
	"fmt"
	"math"
	"math/rand"
	"regexp"
	"sync"
	"time"
)

// Throttler defines core gohalt throttler abstraction and exposes pair of counterpart methods: `Acquire` and `Release`.
type Throttler interface {
	// Acquire takes a part of throttling quota or returns error if throttling quota is drained
	// it needs to be called right before shared resource acquire.
	Acquire(context.Context) error
	// Release puts a part of throttling quota back or returns error if this is not possible
	// it needs to be called just after shared resource release.
	Release(context.Context) error
}

type tmock struct {
	aerr error
	rerr error
}

func (thr tmock) Acquire(context.Context) error {
	return thr.aerr
}

func (thr tmock) Release(context.Context) error {
	return thr.rerr
}

type techo struct {
	err error
}

// NewThrottlerEcho creates new throttler instance that
// always throttles with the specified error back.
func NewThrottlerEcho(err error) Throttler {
	return techo{err: err}
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

// NewThrottlerWait creates new throttler instance that
// always waits for the specified duration.
func NewThrottlerWait(duration time.Duration) Throttler {
	return twait{duration: duration}
}

func (thr twait) Acquire(context.Context) error {
	time.Sleep(thr.duration)
	return nil
}

func (thr twait) Release(context.Context) error {
	return nil
}

type tsquare struct {
	duration time.Duration
	limit    time.Duration
	current  uint64
	reset    bool
}

// NewThrottlerSquare creates new throttler instance that
// always waits for square growing [1, 4, 9, 16, ...] multiplier on the specified duration,
// up until the specified duration limit is reached.
// If reset is set then after throttler riches the specified duration limit
// next multiplier value will be reseted.
func NewThrottlerSquare(duration time.Duration, limit time.Duration, reset bool) Throttler {
	return &tsquare{duration: duration, limit: limit, reset: reset}
}

func (thr *tsquare) Acquire(context.Context) error {
	current := atomicBIncr(&thr.current)
	duration := thr.duration * time.Duration(current*current)
	if thr.limit > 0 && duration > thr.limit {
		duration = thr.limit
		if thr.reset {
			atomicSet(&thr.current, 0)
		}
	}
	time.Sleep(duration)
	return nil
}

func (thr *tsquare) Release(context.Context) error {
	atomicBDecr(&thr.current)
	return nil
}

type tcontext struct{}

// NewThrottlerContext creates new throttler instance that
// always throttless on done context.
func NewThrottlerContext() Throttler {
	return tcontext{}
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

type tpanic struct{}

// NewThrottlerPanic creates new throttler instance that always panics.
func NewThrottlerPanic() Throttler {
	return tpanic{}
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

// NewThrottlerEach creates new throttler instance that
// throttles each periodic i-th call defined by the specified threshold.
func NewThrottlerEach(threshold uint64) Throttler {
	return &teach{threshold: threshold}
}

func (thr *teach) Acquire(context.Context) error {
	if current := atomicIncr(&thr.current); current%thr.threshold == 0 {
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

// NewThrottlerBefore creates new throttler instance that
// throttles each call below the i-th call defined by the specified threshold.
func NewThrottlerBefore(threshold uint64) Throttler {
	return &tbefore{threshold: threshold}
}

func (thr *tbefore) Acquire(context.Context) error {
	if current := atomicBIncr(&thr.current); current <= thr.threshold {
		return errors.New("throttler has not reached threshold yet")
	}
	return nil
}

func (thr *tbefore) Release(context.Context) error {
	return nil
}

type tafter struct {
	current   uint64
	threshold uint64
}

// NewThrottlerAfter creates new throttler instance that
// throttles each call after the i-th call defined by the specified threshold.
func NewThrottlerAfter(threshold uint64) Throttler {
	return &tafter{threshold: threshold}
}

func (thr *tafter) Acquire(context.Context) error {
	if current := atomicBIncr(&thr.current); current > thr.threshold {
		return errors.New("throttler has exceed threshold")
	}
	return nil
}

func (thr *tafter) Release(context.Context) error {
	return nil
}

type tchance struct {
	threshold float64
}

// NewThrottlerChance creates new throttler instance that
// throttles each call with the chance p defined by the specified threshold.
// Chance value is normalized to [0.0, 1.0] range.
// Implementation uses `math/rand` as PRNG function and expects rand seeding by a client.
func NewThrottlerChance(threshold float64) Throttler {
	threshold = math.Abs(threshold)
	if threshold > 1.0 {
		threshold = 1.0
	}
	return tchance{threshold: threshold}
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

type trunning struct {
	running   uint64
	threshold uint64
}

// NewThrottlerRunning creates new throttler instance that
// throttles each call which exeeds the running quota acquired - release
// q defined by the specified threshold.
func NewThrottlerRunning(threshold uint64) Throttler {
	return &trunning{threshold: threshold}
}

func (thr *trunning) Acquire(context.Context) error {
	if running := atomicBIncr(&thr.running); running > thr.threshold {
		return errors.New("throttler has exceed running threshold")
	}
	return nil
}

func (thr *trunning) Release(context.Context) error {
	atomicBDecr(&thr.running)
	return nil
}

type tbuffered struct {
	running chan struct{}
}

// NewThrottlerBuffered creates new throttler instance that
// waits on call which exeeds the running quota acquired - release
// q defined by the specified threshold until the running quota is available again.
func NewThrottlerBuffered(threshold uint64) Throttler {
	return &tbuffered{running: make(chan struct{}, threshold)}
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

// NewThrottlerPriority creates new throttler instance that
// waits on call which exeeds the running quota acquired - release
// q defined by the specified threshold until the running quota is available again.
// Running quota is not equally distributed between n levels of priority
// defined by the specified levels.
// Use `WithPriority` to override context call priority, 1 by default.
func NewThrottlerPriority(threshold uint64, levels uint8) Throttler {
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
	loop Runnable
}

// NewThrottlerTimed creates new throttler instance that
// throttles each call which exeeds the running quota acquired - release
// q defined by the specified threshold in the specified interval.
// Periodically each specified interval the running quota number is reseted.
// If quantum is set then quantum will be used instead of interval to provide the running quota delta updates.
func NewThrottlerTimed(threshold uint64, interval time.Duration, quantum time.Duration) Throttler {
	tafter := NewThrottlerAfter(threshold).(*tafter)
	delta, window := threshold, interval
	if quantum > 0 && interval > quantum {
		delta = uint64(math.Ceil(float64(threshold) / (float64(interval) / float64(quantum))))
		window = quantum
	}
	thr := ttimed{tafter: tafter}
	thr.loop = once(
		loop(window, func(ctx context.Context) error {
			atomicBSub(&thr.current, delta)
			return ctx.Err()
		}),
	)
	return thr
}

func (thr ttimed) Acquire(ctx context.Context) error {
	// start loop on first acquire
	gorun(ctx, thr.loop)
	err := thr.tafter.Acquire(ctx)
	if current := atomicGet(&thr.current); current > thr.threshold {
		atomicSet(&thr.current, thr.threshold)
	}
	return err
}

func (thr ttimed) Release(ctx context.Context) error {
	_ = thr.tafter.Release(ctx)
	return nil
}

type tlatency struct {
	reset     Runnable
	latency   uint64
	threshold time.Duration
}

// NewThrottlerLatency creates new throttler instance that
// throttles each call after the call latency l defined by the specified threshold was exeeded once.
// If retention is set then throttler state will be reseted after retention duration.
// Use `WithTimestamp` to specify running duration between throttler acquire and release.
func NewThrottlerLatency(threshold time.Duration, retention time.Duration) Throttler {
	thr := &tlatency{threshold: threshold}
	thr.reset = delayed(retention, func(context.Context) error {
		atomicSet(&thr.latency, 0)
		return nil
	})
	return thr
}

func (thr *tlatency) Acquire(context.Context) error {
	if latency := atomicGet(&thr.latency); latency > uint64(thr.threshold) {
		return errors.New("throttler has exceed latency threshold")
	}
	return nil
}

func (thr *tlatency) Release(ctx context.Context) error {
	nowTs := time.Now().UTC().UnixNano()
	ctxTs := ctxTimestamp(ctx).UnixNano()
	latency := uint64(nowTs - ctxTs)
	if latency >= uint64(thr.threshold) && atomicGet(&thr.latency) == 0 {
		atomicSet(&thr.latency, latency)
		gorun(ctx, thr.reset)
	}
	return nil
}

type tpercentile struct {
	reset      Runnable
	latencies  *percentiles
	threshold  time.Duration
	percentile float64
}

// NewThrottlerPercentile creates new throttler instance that
// throttles each call after the call latency l defined by the specified threshold
// was exeeded once considering the specified percentile.
// Percentile values are kept in bounded buffer with capacity c defined by the specified capacity.
// If retention is set then throttler state will be reseted after retention duration.
// Use `WithTimestamp` to specify running duration between throttler acquire and release.
func NewThrottlerPercentile(
	threshold time.Duration,
	capacity uint8,
	percentile float64,
	retention time.Duration,
) Throttler {
	percentile = math.Abs(percentile)
	if percentile > 1.0 {
		percentile = 1.0
	}
	thr := tpercentile{threshold: threshold, percentile: percentile}
	thr.latencies = &percentiles{cap: capacity}
	thr.latencies.Prune()
	thr.reset = locked(
		delayed(retention, func(context.Context) error {
			thr.latencies.Prune()
			return nil
		}),
	)
	return thr
}

func (thr tpercentile) Acquire(ctx context.Context) error {
	if thr.latencies.Len() > 0 {
		if latency := thr.latencies.At(thr.percentile); latency >= uint64(thr.threshold) {
			gorun(ctx, thr.reset)
			return errors.New("throttler has exceed latency threshold")
		}
	}
	return nil
}

func (thr tpercentile) Release(ctx context.Context) error {
	nowTs := time.Now().UTC().UnixNano()
	ctxTs := ctxTimestamp(ctx).UnixNano()
	latency := uint64(nowTs - ctxTs)
	thr.latencies.Push(latency)
	return nil
}

type tmonitor struct {
	mnt       Monitor
	threshold Stats
}

// NewThrottlerMonitor creates new throttler instance that
// throttles call if any of the stats returned by provided monitor exceeds
// any of the stats defined by the specified threshold or if any internal error occurred.
// Builtin `Monitor` implementations come with stats caching by default.
// Use builtin `NewMonitorSystem` to create go system monitor instance.
func NewThrottlerMonitor(mnt Monitor, threshold Stats) Throttler {
	return tmonitor{mnt: mnt, threshold: threshold}
}

func (thr tmonitor) Acquire(ctx context.Context) error {
	stats, err := thr.mnt.Stats(ctx)
	if err != nil {
		return fmt.Errorf("throttler hasn't found any stats %w", err)
	}
	if (thr.threshold.MEMAlloc > 0 && stats.MEMAlloc >= thr.threshold.MEMAlloc) ||
		(thr.threshold.MEMSystem > 0 && stats.MEMSystem >= thr.threshold.MEMSystem) ||
		(thr.threshold.CPUPause > 0 && stats.CPUPause >= thr.threshold.CPUPause) ||
		(thr.threshold.CPUUsage > 0 && stats.CPUUsage >= thr.threshold.CPUUsage) {
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

// NewThrottlerMetric creates new throttler instance that
// throttles call ifboolean  metric defined by the specified
// boolean metric is reached or if any internal error occurred.
// Builtin `Metric` implementations come with boolean metric caching by default.
// Use builtin `NewMetricPrometheus` to create Prometheus metric instance.
func NewThrottlerMetric(mtc Metric) Throttler {
	return tmetric{mtc: mtc}
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

type tenqueue struct {
	enq Enqueuer
}

// NewThrottlerEnqueue creates new throttler instance that
// always enqueues message to the specified queue throttles only if any internal error occurred.
// Use `WithMessage` to specify context message for enqueued message and
// `WithMarshaler` to specify context message marshaler.
// Builtin `Enqueuer` implementations come with connection reuse and retries by default.
// Use builtin `NewEnqueuerRabbit` to create RabbitMQ enqueuer instance
// or `NewEnqueuerKafka` to create Kafka enqueuer instance.
func NewThrottlerEnqueue(enq Enqueuer) Throttler {
	return tenqueue{enq: enq}
}

func (thr tenqueue) Acquire(ctx context.Context) error {
	marshaler := ctxMarshaler(ctx)
	if marshaler == nil {
		return errors.New("throttler hasn't found any marshaler")
	}
	message := ctxMessage(ctx)
	if message == nil {
		return errors.New("throttler hasn't found any message")
	}
	msg, err := marshaler(message)
	if err != nil {
		return fmt.Errorf("throttler hasn't sent any message %w", err)
	}
	if err := thr.enq.Enqueue(ctx, msg); err != nil {
		return fmt.Errorf("throttler hasn't sent any message %w", err)
	}
	return nil
}

func (thr tenqueue) Release(ctx context.Context) error {
	return nil
}

type tadaptive struct {
	ttimed
	step uint64
	thr  Throttler
}

// NewThrottlerAdaptive creates new throttler instance that
// throttles each call which exeeds the running quota acquired - release q
// defined by the specified threshold in the specified interval.
// Periodically each specified interval the running quota number is reseted.
// If quantum is set then quantum will be used instead of interval
// to provide the running quota delta updates.
// Provided adapted throttler adjusts the running quota of adapter throttler by changing the value by d
// defined by the specified step, it subtracts *d^2* from the running quota
// if adapted throttler throttles or adds *d* to the running quota if it doesn't.
func NewThrottlerAdaptive(
	threshold uint64,
	interval time.Duration,
	quantum time.Duration,
	step uint64,
	thr Throttler,
) Throttler {
	return &tadaptive{
		ttimed: NewThrottlerTimed(threshold, interval, quantum).(ttimed),
		step:   step,
		thr:    thr,
	}
}

func (thr *tadaptive) Acquire(ctx context.Context) error {
	err := thr.thr.Acquire(ctx)
	if err != nil {
		atomicBSub(&thr.ttimed.threshold, thr.step*thr.step)
	} else {
		atomicBAdd(&thr.ttimed.threshold, thr.step)
	}
	return thr.ttimed.Acquire(ctx)
}

func (thr tadaptive) Release(ctx context.Context) error {
	_ = thr.ttimed.Release(ctx)
	return nil
}

// Pattern defines a pair of regexp and related throttler.
type Pattern struct {
	Pattern   *regexp.Regexp
	Throttler Throttler
}

type tpattern []Pattern

// NewThrottlerPattern creates new throttler instance that
// throttles if matching throttler from provided patterns throttles.
// Use `WithKey` to specify key for regexp pattern throttler matching.
// See `Pattern` which defines a pair of regexp and related throttler.
func NewThrottlerPattern(patterns ...Pattern) Throttler {
	return tpattern(patterns)
}

func (thr tpattern) Acquire(ctx context.Context) error {
	for _, pattern := range thr {
		if key := ctxKey(ctx); pattern.Pattern.MatchString(key) {
			return pattern.Throttler.Acquire(ctx)
		}
	}
	return errors.New("throttler hasn't found any key")
}

func (thr tpattern) Release(ctx context.Context) error {
	for _, pattern := range thr {
		if key := ctxKey(ctx); pattern.Pattern.MatchString(key) {
			_ = pattern.Throttler.Release(ctx)
			return nil
		}
	}
	return nil
}

type tring struct {
	thrs    []Throttler
	acquire uint64
	release uint64
}

// NewThrottlerRing creates new throttler instance that
// throttles if the i-th call throttler from provided list throttle.
func NewThrottlerRing(thrs ...Throttler) Throttler {
	return &tring{thrs: thrs}
}

func (thr *tring) Acquire(ctx context.Context) error {
	if length := len(thr.thrs); length > 0 {
		acquire := atomicIncr(&thr.acquire) - 1
		index := int(acquire) % length
		return thr.thrs[index].Acquire(ctx)
	}
	return errors.New("throttler hasn't found any index")
}

func (thr *tring) Release(ctx context.Context) error {
	if length := len(thr.thrs); length > 0 {
		release := atomicIncr(&thr.release) - 1
		index := int(release) % length
		_ = thr.thrs[index].Release(ctx)
	}
	return nil
}

type tall []Throttler

// NewThrottlerAll creates new throttler instance that
// throttles call if all provided throttlers throttle.
func NewThrottlerAll(thrs ...Throttler) Throttler {
	return tall(thrs)
}

func (thrs tall) Acquire(ctx context.Context) error {
	if length := len(thrs); length > 0 {
		for _, thr := range thrs {
			if err := thr.Acquire(ctx); err == nil {
				return nil
			}
		}
		return errors.New("throttler has received internal errors")
	}
	return nil
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

// NewThrottlerAny creates new throttler instance that
// throttles call if any of provided throttlers throttle.
func NewThrottlerAny(thrs ...Throttler) Throttler {
	return tany(thrs)
}

func (thrs tany) Acquire(ctx context.Context) error {
	runs := make([]Runnable, 0, len(thrs))
	for _, thr := range thrs {
		thr := thr
		runs = append(runs, func(ctx context.Context) error {
			if err := thr.Acquire(ctx); err != nil {
				return errors.New("throttler has received internal errors")
			}
			return nil
		})
	}
	return all(runs...)(ctx)
}

func (thrs tany) Release(ctx context.Context) error {
	runs := make([]Runnable, 0, len(thrs))
	for _, thr := range thrs {
		thr := thr
		runs = append(runs, func(ctx context.Context) error {
			_ = thr.Release(ctx)
			return nil
		})
	}
	return all(runs...)(ctx)
}

type tnot struct {
	thr Throttler
}

// NewThrottlerNot creates new throttler instance that
// throttles call if provided throttler doesn't throttle.
func NewThrottlerNot(thr Throttler) Throttler {
	return tnot{thr: thr}
}

func (thr tnot) Acquire(ctx context.Context) error {
	if err := thr.thr.Acquire(ctx); err != nil {
		return nil
	}
	return errors.New("throttler hasn't received any internal error")
}

func (thr tnot) Release(ctx context.Context) error {
	_ = thr.thr.Release(ctx)
	return nil
}

type tsuppress struct {
	thr Throttler
}

// NewThrottlerSuppress creates new throttler instance that
// suppresses provided throttler to never throttle.
func NewThrottlerSuppress(thr Throttler) Throttler {
	return tsuppress{thr: thr}
}

func (thr tsuppress) Acquire(ctx context.Context) error {
	if err := thr.thr.Acquire(ctx); err != nil {
		log("throttler error is suppressed %v", err)
	}
	return nil
}

func (thr tsuppress) Release(ctx context.Context) error {
	_ = thr.thr.Release(ctx)
	return nil
}

type tretry struct {
	thr     Throttler
	retries uint64
}

// NewThrottlerRetry creates new throttler instance that
// retries provided throttler error up until the provided retries threshold.
// Internally retry uses square throttler with `DefaultRetriedDuration` initial duration.
func NewThrottlerRetry(thr Throttler, retries uint64) Throttler {
	return tretry{thr: thr, retries: retries}
}

func (thr tretry) Acquire(ctx context.Context) error {
	return retried(thr.retries, func(ctx context.Context) error {
		return thr.thr.Acquire(ctx)
	})(ctx)
}

func (thr tretry) Release(ctx context.Context) error {
	_ = thr.thr.Release(ctx)
	return nil
}

type tcache struct {
	thr     Throttler
	acquire Runnable
	reset   Runnable
}

// NewThrottlerCache creates new throttler instance that
// caches provided throttler calls for the provided cache duration,
// throttler release resulting resets cache.
// Only non throttling calls are cached for the provided cache duration.
func NewThrottlerCache(thr Throttler, cache time.Duration) Throttler {
	tcache := tcache{thr: thr}
	tcache.acquire, tcache.reset = cached(cache, func(ctx context.Context) error {
		return thr.Acquire(ctx)
	})
	return tcache
}

func (thr tcache) Acquire(ctx context.Context) error {
	return thr.acquire(ctx)
}

func (thr tcache) Release(ctx context.Context) error {
	_ = thr.thr.Release(ctx)
	_ = thr.reset(ctx)
	return nil
}
