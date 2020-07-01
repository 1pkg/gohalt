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

func NewGenerator(thr Throttler) generator {
	return generator{thr: thr}
}

func (gen generator) Generate(ctx context.Context, key interface{}) Throttler {
	return gen.thr.accept(ctx, gen).(Throttler)
}

func (gen generator) tvisitEcho(ctx context.Context, thr techo) interface{} {
	return NewThrottlerEcho(thr.err)
}

func (gen generator) tvisitWait(ctx context.Context, thr twait) interface{} {
	return NewThrottlerWait(thr.duration)
}

func (gen generator) tvisitPanic(ctx context.Context, thr tpanic) interface{} {
	return NewThrottlerPanic()
}

func (gen generator) tvisitEach(ctx context.Context, thr teach) interface{} {
	return NewThrottlerEach(thr.threshold)
}

func (gen generator) tvisitAfter(ctx context.Context, thr tafter) interface{} {
	return NewThrottlerAfter(thr.threshold)
}

func (gen generator) tvisitChance(ctx context.Context, thr tchance) interface{} {
	return NewThrottlerChance(thr.percentage)
}

func (gen generator) tvisitFixed(ctx context.Context, thr tfixed) interface{} {
	return NewThrottlerFixed(thr.limit)
}

func (gen generator) tvisitRunning(ctx context.Context, thr trunning) interface{} {
	return NewThrottlerRunning(thr.limit)
}

func (gen generator) tvisitBuffered(ctx context.Context, thr tbuffered) interface{} {
	return NewThrottlerBuffered(uint64(len(thr.running)))
}

func (gen generator) tvisitPriority(ctx context.Context, thr tpriority) interface{} {
	return NewThrottlerPriority(thr.size, thr.limit)
}

func (gen generator) tvisitTimed(ctx context.Context, thr ttimed) interface{} {
	return NewThrottlerTimed(ctx, thr.limit, thr.interval, thr.slide)
}

func (gen generator) tvisitMonitor(ctx context.Context, thr tmonitor) interface{} {
	return NewThrottlerMonitor(thr.mnt, thr.limit)
}

func (gen generator) tvisitMetric(ctx context.Context, thr tmetric) interface{} {
	return NewThrottlerMetric(thr.mtc)
}

func (gen generator) tvisitLatency(ctx context.Context, thr tlatency) interface{} {
	return NewThrottlerLatency(thr.limit, thr.retention)
}

func (gen generator) tvisitPercentile(ctx context.Context, thr tpercentile) interface{} {
	return NewThrottlerPercentile(thr.limit, thr.percentile, thr.retention)
}

func (gen generator) tvisitAdaptive(ctx context.Context, thr tadaptive) interface{} {
	gen = NewGenerator(thr.thr)
	return NewThrottlerAdaptive(
		ctx,
		thr.limit,
		thr.interval,
		thr.slide,
		thr.step,
		gen.Generate(ctx, nil),
	)
}

func (gen generator) tvisitContext(ctx context.Context, thr tcontext) interface{} {
	return NewThrottlerContext()
}

func (gen generator) tvisitEnqueue(ctx context.Context, thr tenqueue) interface{} {
	return NewThrottlerEnqueue(thr.enq)
}

func (gen generator) tvisitKeyed(ctx context.Context, thr tkeyed) interface{} {
	return NewThrottlerKeyed(thr.gen)
}

func (gen generator) tvisitAll(ctx context.Context, thrs tall) interface{} {
	genthrs := make([]Throttler, 0, len(thrs))
	for _, thr := range thrs {
		gen := NewGenerator(thr)
		genthrs = append(genthrs, gen.Generate(ctx, nil))
	}
	return NewThrottlerAll(genthrs...)
}

func (gen generator) tvisitAny(ctx context.Context, thrs tany) interface{} {
	genthrs := make([]Throttler, 0, len(thrs))
	for _, thr := range thrs {
		gen := NewGenerator(thr)
		genthrs = append(genthrs, gen.Generate(ctx, nil))
	}
	return NewThrottlerAny(genthrs...)
}

func (gen generator) tvisitNot(ctx context.Context, thr tnot) interface{} {
	gen = NewGenerator(thr)
	return NewThrottlerNot(gen.Generate(ctx, nil))
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
