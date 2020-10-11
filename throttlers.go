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

type Throttler interface {
	Acquire(context.Context) error
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

func NewThrottlerSquare(duration time.Duration, limit time.Duration, reset bool) Throttler {
	return &tsquare{duration: duration, limit: limit, reset: reset}
}

func (thr *tsquare) Acquire(context.Context) error {
	current := atomicBIncr(&thr.current)
	duration := thr.duration * time.Duration(current*current)
	if duration > thr.limit {
		duration = thr.limit
		if thr.reset {
			atomicSet(&thr.current, 0)
		}
	}
	time.Sleep(duration)
	return nil
}

func (thr *tsquare) Release(context.Context) error {
	return nil
}

type tcontext struct{}

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
	return thr.tafter.Release(ctx)
}

type tlatency struct {
	reset     Runnable
	latency   uint64
	threshold time.Duration
}

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

func NewThrottlerEnqueue(enq Enqueuer) Throttler {
	return tenqueue{enq: enq}
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

type tadaptive struct {
	ttimed
	step uint64
	thr  Throttler
}

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
	return thr.ttimed.Release(ctx)
}

type Pattern struct {
	Pattern   *regexp.Regexp
	Throttler Throttler
}

type tpattern []Pattern

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
			return pattern.Throttler.Release(ctx)
		}
	}
	return nil
}

type tring struct {
	thrs    []Throttler
	acquire uint64
	release uint64
}

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
		return thr.thrs[index].Release(ctx)
	}
	return nil
}

type tall []Throttler

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

func NewThrottlerSuppress(thr Throttler) Throttler {
	return tsuppress{thr: thr}
}

func (thr tsuppress) Acquire(ctx context.Context) error {
	_ = thr.thr.Acquire(ctx)
	return nil
}

func (thr tsuppress) Release(ctx context.Context) error {
	_ = thr.thr.Release(ctx)
	return nil
}
