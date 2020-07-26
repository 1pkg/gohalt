package gohalt

import (
	"container/ring"
	"context"
	"regexp"
	"sync"
)

type Generator interface {
	Generate(context.Context, interface{}) Throttler
}

type generator struct {
	thr Throttler
}

func NewGenerator(thr Throttler) *generator {
	return &generator{thr: thr}
}

func (gen *generator) Generate(ctx context.Context, key interface{}) Throttler {
	gengen := NewGenerator(nil)
	gen.thr.accept(ctx, gengen)
	return gengen.thr
}

func (gen *generator) tvisitEcho(ctx context.Context, thr *techo) {
	gen.thr = NewThrottlerEcho(thr.err)
}

func (gen *generator) tvisitWait(ctx context.Context, thr *twait) {
	gen.thr = NewThrottlerWait(thr.duration)
}

func (gen *generator) tvisitPanic(ctx context.Context, thr *tpanic) {
	gen.thr = NewThrottlerPanic()
}

func (gen *generator) tvisitEach(ctx context.Context, thr *teach) {
	gen.thr = NewThrottlerEach(thr.threshold)
}

func (gen *generator) tvisitBefore(ctx context.Context, thr *tbefore) {
	gen.thr = NewThrottlerBefore(thr.threshold)
}

func (gen *generator) tvisitChance(ctx context.Context, thr *tchance) {
	gen.thr = NewThrottlerChance(thr.threshold)
}

func (gen *generator) tvisitAfter(ctx context.Context, thr *tafter) {
	gen.thr = NewThrottlerAfter(thr.threshold)
}

func (gen *generator) tvisitRunning(ctx context.Context, thr *trunning) {
	gen.thr = NewThrottlerRunning(thr.threshold)
}

func (gen *generator) tvisitBuffered(ctx context.Context, thr *tbuffered) {
	gen.thr = NewThrottlerBuffered(uint64(len(thr.running)))
}

func (gen *generator) tvisitPriority(ctx context.Context, thr *tpriority) {
	gen.thr = NewThrottlerPriority(thr.size, thr.limit)
}

func (gen *generator) tvisitTimed(ctx context.Context, thr *ttimed) {
	gen.thr = NewThrottlerTimed(ctx, thr.threshold, thr.interval, thr.slide)
}

func (gen *generator) tvisitMonitor(ctx context.Context, thr *tmonitor) {
	gen.thr = NewThrottlerMonitor(thr.mnt, thr.limit)
}

func (gen *generator) tvisitMetric(ctx context.Context, thr *tmetric) {
	gen.thr = NewThrottlerMetric(thr.mtc)
}

func (gen *generator) tvisitLatency(ctx context.Context, thr *tlatency) {
	gen.thr = NewThrottlerLatency(thr.limit, thr.retention)
}

func (gen *generator) tvisitPercentile(ctx context.Context, thr *tpercentile) {
	gen.thr = NewThrottlerPercentile(thr.limit, thr.percentile, thr.retention)
}

func (gen *generator) tvisitAdaptive(ctx context.Context, thr *tadaptive) {
	gen = NewGenerator(thr.thr)
	gen.thr = NewThrottlerAdaptive(
		ctx,
		thr.threshold,
		thr.interval,
		thr.slide,
		thr.step,
		gen.Generate(ctx, nil),
	)
}

func (gen *generator) tvisitContext(ctx context.Context, thr *tcontext) {
	gen.thr = NewThrottlerContext()
}

func (gen *generator) tvisitEnqueue(ctx context.Context, thr *tenqueue) {
	gen.thr = NewThrottlerEnqueue(thr.enq)
}

func (gen *generator) tvisitKeyed(ctx context.Context, thr *tkeyed) {
	gen.thr = NewThrottlerKeyed(thr.gen)
}

func (gen *generator) tvisitAll(ctx context.Context, thr *tall) {
	thrs := *thr
	genthrs := make([]Throttler, 0, len(thrs))
	for _, thr := range thrs {
		gen := NewGenerator(thr)
		genthrs = append(genthrs, gen.Generate(ctx, nil))
	}
	gen.thr = NewThrottlerAll(genthrs...)
}

func (gen *generator) tvisitAny(ctx context.Context, thr *tany) {
	thrs := *thr
	genthrs := make([]Throttler, 0, len(thrs))
	for _, thr := range thrs {
		gen := NewGenerator(thr)
		genthrs = append(genthrs, gen.Generate(ctx, nil))
	}
	gen.thr = NewThrottlerAny(genthrs...)
}

func (gen *generator) tvisitNot(ctx context.Context, thr *tnot) {
	gen = NewGenerator(thr.thr)
	gen.thr = NewThrottlerNot(gen.Generate(ctx, nil))
}

func (gen *generator) tvisitSuppress(ctx context.Context, thr *tsuppress) {
	gen = NewGenerator(thr.thr)
	gen.thr = NewThrottlerSuppress(gen.Generate(ctx, nil))
}

type genring struct {
	ring *ring.Ring
	lock sync.Mutex
}

func NewGeneratorRing(gens ...Generator) *genring {
	glen := len(gens)
	ring := ring.New(glen)
	for i := 0; i < glen; i++ {
		ring.Value = gens[i]
		ring = ring.Next()
	}
	return &genring{ring: ring}
}

func (gen *genring) Generate(ctx context.Context, key interface{}) Throttler {
	gen.lock.Lock()
	defer gen.lock.Unlock()
	thr := gen.ring.Value.(Generator).Generate(ctx, key)
	gen.ring = gen.ring.Next()
	return thr
}

type Pattern struct {
	Pattern   *regexp.Regexp
	Generator Generator
}

type genpattern struct {
	patterns []Pattern
}

func NewGeneratorPattern(patterns ...Pattern) genpattern {
	return genpattern{patterns: patterns}
}

func (gen genpattern) Generate(ctx context.Context, key interface{}) Throttler {
	for _, pattern := range gen.patterns {
		if str, ok := key.(string); ok && pattern.Pattern.MatchString(str) {
			return pattern.Generator.Generate(ctx, key)
		}
	}
	return NewThrottlerEcho(nil)
}
