package gohalt

import (
	"context"
	"math"
	"regexp"
	"sync"
	"time"

	"golang.org/x/sync/semaphore"
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
// - could return any specified error;
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

func (thr twait) Acquire(ctx context.Context) error {
	return sleep(ctx, thr.duration)
}

func (thr twait) Release(context.Context) error {
	return nil
}

type tsquare struct {
	initial time.Duration
	limit   time.Duration
	current uint64
	reset   bool
}

// NewThrottlerSquare creates new throttler instance that
// always waits for square growing [1, 4, 9, 16, ...] multiplier on the specified initial duration,
// up until the specified duration limit is reached.
// If reset is set then after throttler riches the specified duration limit
// next multiplier value will be reseted.
func NewThrottlerSquare(initial time.Duration, limit time.Duration, reset bool) Throttler {
	return &tsquare{initial: initial, limit: limit, reset: reset}
}

func (thr *tsquare) Acquire(ctx context.Context) error {
	current := atomicBIncr(&thr.current)
	duration := thr.initial * time.Duration(current*current)
	if thr.limit > 0 && duration > thr.limit {
		duration = thr.limit
		if thr.reset {
			atomicSet(&thr.current, 0)
		}
	}
	return sleep(ctx, duration)
}

func (thr *tsquare) Release(context.Context) error {
	atomicBDecr(&thr.current)
	return nil
}

type tjitter struct {
	initial time.Duration
	limit   time.Duration
	current uint64
	reset   bool
	jitter  float64
}

// NewThrottlerJitter creates new throttler instance that
// waits accordingly to undelying square throttler but also
// adds the provided jitter delta distribution on top.
// Jitter value is normalized to [0.0, 1.0] range and defines
// which part of square delay could be randomized in percents.
// Implementation uses secure `crypto/rand` as PRNG function.
func NewThrottlerJitter(initial time.Duration, limit time.Duration, reset bool, jitter float64) Throttler {
	jitter = math.Abs(jitter)
	if jitter > 1.0 {
		jitter = 1.0
	}
	jitter = 1.0 - jitter
	thr := &tjitter{initial: initial, limit: limit, reset: reset, jitter: jitter}
	return thr
}

func (thr *tjitter) Acquire(ctx context.Context) error {
	current := atomicBIncr(&thr.current)
	duration := thr.initial * time.Duration(current*current)
	if thr.limit > 0 && duration > thr.limit {
		duration = thr.limit
		if thr.reset {
			atomicSet(&thr.current, 0)
		}
	}
	base := float64(duration) * thr.jitter
	side := (float64(duration) - base) * rndf64(1.0)
	return sleep(ctx, time.Duration(base+side))
}

func (thr *tjitter) Release(ctx context.Context) error {
	atomicBDecr(&thr.current)
	return nil
}

type tcontext struct{}

// NewThrottlerContext creates new throttler instance that
// always throttless on done context.
// - could return `ErrorInternal`;
func NewThrottlerContext() Throttler {
	return tcontext{}
}

func (thr tcontext) Acquire(ctx context.Context) error {
	select {
	case <-ctx.Done():
		return ErrorInternal{
			Throttler: "context",
			Message:   ctx.Err().Error(),
		}
	default:
		return nil
	}
}

func (thr tcontext) Release(ctx context.Context) error {
	return nil
}

type tpanic struct{}

// NewThrottlerPanic creates new throttler instance that always panics with `ErrorInternal`.
func NewThrottlerPanic() Throttler {
	return tpanic{}
}

func (thr tpanic) Acquire(context.Context) error {
	panic(ErrorInternal{Throttler: "panic"})
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
// - could return `ErrorThreshold`;
func NewThrottlerEach(threshold uint64) Throttler {
	return &teach{threshold: threshold}
}

func (thr *teach) Acquire(context.Context) error {
	if current := atomicIncr(&thr.current); current%thr.threshold == 0 {
		return ErrorThreshold{
			Throttler: "each",
			Threshold: strpair{current: current, threshold: thr.threshold},
		}
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
// Use `WithWeight` to override context call qunatity, 1 by default.
// - could return `ErrorThreshold`;
func NewThrottlerBefore(threshold uint64) Throttler {
	return &tbefore{threshold: threshold}
}

func (thr *tbefore) Acquire(ctx context.Context) error {
	if current := atomicBSingAdd(&thr.current, ctxWeight(ctx)); current <= thr.threshold {
		return ErrorThreshold{
			Throttler: "before",
			Threshold: strpair{current: current, threshold: thr.threshold},
		}
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
// Use `WithWeight` to override context call qunatity, 1 by default.
// - could return `ErrorThreshold`;
func NewThrottlerAfter(threshold uint64) Throttler {
	return &tafter{threshold: threshold}
}

func (thr *tafter) Acquire(ctx context.Context) error {
	if current := atomicBSingAdd(&thr.current, ctxWeight(ctx)); current > thr.threshold {
		return ErrorThreshold{
			Throttler: "after",
			Threshold: strpair{current: current, threshold: thr.threshold},
		}
	}
	return nil
}

func (thr *tafter) Release(context.Context) error {
	return nil
}

type tpast struct {
	threshold time.Time
}

// NewThrottlerPast creates new throttler instance that
// throttles each call before timestamp defined by the specified UTC time threshold.
// - could return `ErrorThreshold`;
func NewThrottlerPast(threshold time.Time) Throttler {
	return tpast{threshold: threshold.UTC()}
}

func (thr tpast) Acquire(ctx context.Context) error {
	if ts := ctxTimestamp(ctx); ts.UnixNano() <= thr.threshold.UnixNano() {
		return ErrorThreshold{
			Throttler: "past",
			Threshold: strtimes{current: ts, threshold: thr.threshold},
		}
	}
	return nil
}

func (thr tpast) Release(context.Context) error {
	return nil
}

type tfuture struct {
	threshold time.Time
}

// NewThrottlerFuture creates new throttler instance that
// throttles each call after timestamp defined by the specified UTC time threshold.
// - could return `ErrorThreshold`;
func NewThrottlerFuture(threshold time.Time) Throttler {
	return tfuture{threshold: threshold.UTC()}
}

func (thr tfuture) Acquire(ctx context.Context) error {
	if ts := ctxTimestamp(ctx); ts.UnixNano() > thr.threshold.UnixNano() {
		return ErrorThreshold{
			Throttler: "future",
			Threshold: strtimes{current: ts, threshold: thr.threshold},
		}
	}
	return nil
}

func (thr tfuture) Release(context.Context) error {
	return nil
}

type tchance struct {
	threshold float64
}

// NewThrottlerChance creates new throttler instance that
// throttles each call with the chance p defined by the specified threshold.
// Chance value is normalized to [0.0, 1.0] range.
// Implementation uses secure `crypto/rand` as PRNG function.
// - could return `ErrorThreshold`;
func NewThrottlerChance(threshold float64) Throttler {
	threshold = math.Abs(threshold)
	if threshold > 1.0 {
		threshold = 1.0
	}
	return tchance{threshold: threshold}
}

func (thr tchance) Acquire(context.Context) error {
	if thr.threshold > 1.0-rndf64(0.0) {
		return ErrorThreshold{
			Throttler: "chance",
			Threshold: strpercent(thr.threshold),
		}
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
// - could return `ErrorThreshold`;
func NewThrottlerRunning(threshold uint64) Throttler {
	return &trunning{threshold: threshold}
}

func (thr *trunning) Acquire(context.Context) error {
	if running := atomicBIncr(&thr.running); running > thr.threshold {
		return ErrorThreshold{
			Throttler: "running",
			Threshold: strpair{current: running, threshold: thr.threshold},
		}
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
// Use `WithWeight` to override context call qunatity, 1 by default.
// - could return `ErrorThreshold`;
func NewThrottlerTimed(threshold uint64, interval time.Duration, quantum time.Duration) Throttler {
	tafter := NewThrottlerAfter(threshold).(*tafter)
	delta, window := threshold, interval
	if quantum > 0 && interval > quantum {
		delta = uint64(math.Ceil(float64(threshold) / (float64(interval) / float64(quantum))))
		window = quantum
	}
	thr := ttimed{tafter: tafter}
	thr.loop = once(
		async(
			loop(window, func(ctx context.Context) error {
				atomicBSub(&thr.current, delta)
				return ctx.Err()
			}),
		),
	)
	return thr
}

func (thr ttimed) Acquire(ctx context.Context) error {
	// start loop on first acquire
	_ = thr.loop(ctx)
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
// - could return `ErrorThreshold`;
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
		return ErrorThreshold{
			Throttler: "latency",
			Threshold: strdurations{current: time.Duration(latency), threshold: thr.threshold},
		}
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
// - could return `ErrorThreshold`;
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
			return ErrorThreshold{
				Throttler: "percentile",
				Threshold: strdurations{current: time.Duration(latency), threshold: thr.threshold},
			}
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
// - could return `ErrorInternal`;
// - could return `ErrorThreshold`;
func NewThrottlerMonitor(mnt Monitor, threshold Stats) Throttler {
	return tmonitor{mnt: mnt, threshold: threshold}
}

func (thr tmonitor) Acquire(ctx context.Context) error {
	stats, err := thr.mnt.Stats(ctx)
	if err != nil {
		return ErrorInternal{
			Throttler: "monitor",
			Message:   err.Error(),
		}
	}
	if thr.threshold.Compare(stats) {
		return ErrorThreshold{
			Throttler: "monitor",
			Threshold: strstats{current: stats, threshold: thr.threshold},
		}
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
// - could return `ErrorInternal`;
// - could return `ErrorThreshold`;
func NewThrottlerMetric(mtc Metric) Throttler {
	return tmetric{mtc: mtc}
}

func (thr tmetric) Acquire(ctx context.Context) error {
	val, err := thr.mtc.Query(ctx)
	if err != nil {
		return ErrorInternal{
			Throttler: "metric",
			Message:   err.Error(),
		}
	}
	if val {
		return ErrorThreshold{
			Throttler: "metric",
			Threshold: strbool(val),
		}
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
// - could return `ErrorInternal`;
func NewThrottlerEnqueue(enq Enqueuer) Throttler {
	return tenqueue{enq: enq}
}

func (thr tenqueue) Acquire(ctx context.Context) error {
	marshaler := ctxMarshaler(ctx)
	if marshaler == nil {
		return ErrorInternal{
			Throttler: "enqueue",
			Message:   "context doesn't contain required marshaler",
		}
	}
	message := ctxMessage(ctx)
	if message == nil {
		return ErrorInternal{
			Throttler: "enqueue",
			Message:   "context doesn't contain required message",
		}
	}
	msg, err := marshaler(message)
	if err != nil {
		return ErrorInternal{
			Throttler: "enqueue",
			Message:   err.Error(),
		}
	}
	if err := thr.enq.Enqueue(ctx, msg); err != nil {
		return ErrorInternal{
			Throttler: "enqueue",
			Message:   err.Error(),
		}
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
// Use `WithWeight` to override context call qunatity, 1 by default.
// - could return `ErrorThreshold`;
func NewThrottlerAdaptive(
	threshold uint64,
	interval time.Duration,
	quantum time.Duration,
	step uint64,
	thr Throttler,
) Throttler {
	tadaptive := &tadaptive{step: step, thr: thr}
	tadaptive.ttimed = NewThrottlerTimed(threshold, interval, quantum).(ttimed)
	return tadaptive
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
// - could return `ErrorInternal`;
// - could return any underlying throttler error;
func NewThrottlerPattern(patterns ...Pattern) Throttler {
	return tpattern(patterns)
}

func (thr tpattern) Acquire(ctx context.Context) error {
	for _, pattern := range thr {
		if key := ctxKey(ctx); pattern.Pattern.MatchString(key) {
			return pattern.Throttler.Acquire(ctx)
		}
	}
	return ErrorInternal{
		Throttler: "pattern",
		Message:   "known key is not found",
	}
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
// - could return `ErrorInternal`;
// - could return any underlying throttler error;
func NewThrottlerRing(thrs ...Throttler) Throttler {
	return &tring{thrs: thrs}
}

func (thr *tring) Acquire(ctx context.Context) error {
	if length := len(thr.thrs); length > 0 {
		acquire := atomicIncr(&thr.acquire) - 1
		index := int(acquire) % length
		return thr.thrs[index].Acquire(ctx)
	}
	return ErrorInternal{
		Throttler: "ring",
		Message:   "known index is not found",
	}
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
// - could return `ErrorInternal`;
func NewThrottlerAll(thrs ...Throttler) Throttler {
	return tall(thrs)
}

func (thrs tall) Acquire(ctx context.Context) error {
	if length := len(thrs); length > 0 {
		var err error
		for _, thr := range thrs {
			if err = thr.Acquire(ctx); err == nil {
				return nil
			}
		}
		return ErrorInternal{
			Throttler: "all",
			Message:   err.Error(),
		}
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
// - could return `ErrorInternal`;
func NewThrottlerAny(thrs ...Throttler) Throttler {
	return tany(thrs)
}

func (thrs tany) Acquire(ctx context.Context) error {
	runs := make([]Runnable, 0, len(thrs))
	for _, thr := range thrs {
		thr := thr
		runs = append(runs, func(ctx context.Context) error {
			if err := thr.Acquire(ctx); err != nil {
				return ErrorInternal{
					Throttler: "any",
					Message:   err.Error(),
				}
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
// - could return `ErrorInternal`;
func NewThrottlerNot(thr Throttler) Throttler {
	return tnot{thr: thr}
}

func (thr tnot) Acquire(ctx context.Context) error {
	if err := thr.thr.Acquire(ctx); err != nil {
		return nil
	}
	return ErrorInternal{
		Throttler: "not",
		Message:   "no error happened",
	}
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
		log("throttler error is suppressed: %v", err)
	}
	return nil
}

func (thr tsuppress) Release(ctx context.Context) error {
	_ = thr.thr.Release(ctx)
	return nil
}

type tretry struct {
	thr         Throttler
	retries     uint64
	onthreshold bool
}

// NewThrottlerRetry creates new throttler instance that
// retries provided throttler error up until the provided retries threshold.
// If provided onthreshold flag is set even `ErrorThreshold` errors will be retried.
// Internally retry uses square throttler with `DefaultRetriedDuration` initial duration.
// - could return any underlying throttler error;
func NewThrottlerRetry(thr Throttler, retries uint64, onthreshold bool) Throttler {
	return tretry{thr: thr, retries: retries, onthreshold: onthreshold}
}

func (thr tretry) Acquire(ctx context.Context) (err error) {
	_ = retried(thr.retries, func(ctx context.Context) error {
		err = thr.thr.Acquire(ctx)
		switch err.(type) {
		case ErrorThreshold:
			if thr.onthreshold {
				return err
			}
			return nil
		default:
			return err
		}
	})(ctx)
	return
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
// - could return any underlying throttler error;
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

type tgenerator struct {
	gen      Generator
	thrs     sync.Map
	size     uint64
	capacity uint64
	evict    Runnable
}

// NewThrottlerGenerator creates new throttler instance that
// throttles if found key matching throttler throttles.
// If no key matching throttler has been found generator see `Generator` used insted
// to provide new throttler that will be added to existing throttlers map.
// Generated throttlers are kept in bounded map with capacity c defined by the specified capacity
// and eviction rate e defined by specified eviction value is normalized to [0.0, 1.0], where eviction rate affects number of
// throttlers that will be removed from the map after bounds overflow.
// Use `WithKey` to specify key for throttler matching and generation.
// - could return `ErrorInternal`;
// - could return any underlying throttler error;
func NewThrottlerGenerator(gen Generator, capacity uint64, eviction float64) Throttler {
	thr := &tgenerator{gen: gen, capacity: capacity}
	eviction = math.Abs(eviction)
	if eviction > 1.0 {
		eviction = 1.0
	}
	num := uint64(math.Ceil(float64(capacity) * eviction))
	thr.evict = locked(func(c context.Context) error {
		var i uint64
		thr.thrs.Range(func(key interface{}, _ interface{}) bool {
			thr.thrs.Delete(key)
			atomicBDecr(&thr.size)
			i++
			return i < num
		})
		return nil
	})
	return thr
}

func (thr *tgenerator) Acquire(ctx context.Context) error {
	key := ctxKey(ctx)
	if thr, ok := thr.thrs.Load(key); ok {
		return thr.(Throttler).Acquire(ctx)
	}
	gthr, err := thr.gen(key)
	if err != nil {
		return ErrorInternal{
			Throttler: "generator",
			Message:   err.Error(),
		}
	}
	if size := atomicGet(&thr.size) + 1; size > thr.capacity {
		gorun(ctx, thr.evict)
	}
	thr.thrs.Store(key, gthr)
	atomicBIncr(&thr.size)
	return gthr.Acquire(ctx)
}

func (thr *tgenerator) Release(ctx context.Context) error {
	if thr, ok := thr.thrs.Load(ctxKey(ctx)); ok {
		return thr.(Throttler).Release(ctx)
	}
	return nil
}

type tsemaphore struct {
	sem *semaphore.Weighted
}

// NewThrottlerSemaphore creates new throttler instance that
// throttles call if underlying semaphore throttles.
// Use `WithWeight` to override context call weight, 1 by default.
// - could return `ErrorThreshold`;
func NewThrottlerSemaphore(weight int64) Throttler {
	return tsemaphore{sem: semaphore.NewWeighted(weight)}
}

func (thr tsemaphore) Acquire(ctx context.Context) error {
	if ok := thr.sem.TryAcquire(ctxWeight(ctx)); !ok {
		return ErrorThreshold{
			Throttler: "semaphore",
			Threshold: strbool(ok),
		}
	}
	return nil
}

func (thr tsemaphore) Release(ctx context.Context) error {
	// prevent over releasing panic.
	defer func() { _ = recover() }()
	thr.sem.Release(ctxWeight(ctx))
	return nil
}

type tcellrate struct {
	current   uint64
	threshold uint64
	quantum   time.Duration
	monotone  bool
}

// NewThrottlerCellRate creates new throttler instance that
// uses generic cell rate algorithm to throttles call within provided interval and threshold.
// If provided monotone flag is set class to release will have no effect on throttler.
// Use `WithWeight` to override context call qunatity, 1 by default.
// - could return `ErrorThreshold`;
func NewThrottlerCellRate(threshold uint64, interval time.Duration, monotone bool) Throttler {
	quantum := time.Duration(math.Ceil(float64(interval) / float64(threshold)))
	return &tcellrate{threshold: threshold, quantum: quantum, monotone: monotone}
}

func (thr *tcellrate) Acquire(ctx context.Context) error {
	current := atomicGet(&thr.current)
	nowTs := uint64(time.Now().UTC().UnixNano())
	if current < nowTs {
		current = nowTs
	}
	updated := current + (uint64(thr.quantum) * uint64(ctxWeight(ctx)))
	max := nowTs + (uint64(thr.quantum) * thr.threshold)
	if updated > max {
		current := uint64(math.Round(float64(updated-nowTs) / float64(thr.quantum)))
		return ErrorThreshold{
			Throttler: "cellrate",
			Threshold: strpair{current: current, threshold: thr.threshold},
		}
	}
	atomicSet(&thr.current, updated)
	return nil
}

func (thr *tcellrate) Release(ctx context.Context) error {
	// don't decrement calls for monotone cell.
	if thr.monotone {
		return nil
	}
	updated := atomicGet(&thr.current) - (uint64(thr.quantum) * uint64(ctxWeight(ctx)))
	atomicSet(&thr.current, updated)
	return nil
}
