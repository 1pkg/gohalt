package gohalt

import (
	"context"
	"time"
)

type Generator interface {
	Generate(context.Context, interface{}) Throttler
}

type gecho techo

func (gen gecho) Generate(context.Context, interface{}) Throttler {
	return NewThrottlerEcho(gen.err)
}

type gwait twait

func (gen gwait) Generate(context.Context, interface{}) Throttler {
	return NewThrottlerWait(gen.dur)
}

type gpanic tpanic

func (gen gpanic) Generate(context.Context, interface{}) Throttler {
	return NewThrottlerPanic()
}

type geach teach

func (gen geach) Generate(context.Context, interface{}) Throttler {
	return NewThrottlerEach(gen.num)
}

type gafter tafter

func (gen gafter) Generate(context.Context, interface{}) Throttler {
	return NewThrottlerAfter(gen.num)
}

type gchance tchance

func (gen gchance) Generate(context.Context, interface{}) Throttler {
	return NewThrottlerChance(gen.pos)
}

type gfixed tfixed

func (gen gfixed) Generate(context.Context, interface{}) Throttler {
	return NewThrottlerFixed(gen.max)
}

type grunning trunning

func (gen grunning) Generate(context.Context, interface{}) Throttler {
	return NewThrottlerRunning(gen.max)
}

type gbuffered tbuffered

func (gen gbuffered) Generate(context.Context, interface{}) Throttler {
	return NewThrottlerBuffered(uint64(len(gen.run)))
}

type gpriority tpriority

func (gen gpriority) Generate(context.Context, interface{}) Throttler {
	return NewThrottlerPriority(gen.size, gen.lim)
}

type gtimed ttimed

func (gen gtimed) Generate(ctx context.Context, key interface{}) Throttler {
	return NewThrottlerTimed(ctx, gen.max, gen.wnd, gen.sld)
}

type gmonitor tmonitor

func (gen gmonitor) Generate(ctx context.Context, key interface{}) Throttler {
	return NewThrottlerMonitor(gen.monitor, gen.limit)
}

type gmetric tmetric

func (gen gmetric) Generate(context.Context, interface{}) Throttler {
	return NewThrottlerMetric(gen.metric)
}

type glatency tlatency

func (gen glatency) Generate(context.Context, interface{}) Throttler {
	return NewThrottlerLatency(time.Duration(gen.max), gen.ret)
}

type gpercentile tpercentile

func (gen gpercentile) Generate(context.Context, interface{}) Throttler {
	return NewThrottlerPercentile(time.Duration(gen.max), gen.pnt, gen.ret)
}

type gadaptive tadaptive

func (gen gadaptive) Generate(ctx context.Context, key interface{}) Throttler {
	return NewThrottlerAdaptive(
		ctx,
		gen.max,
		gen.wnd,
		gen.sld,
		gen.step,
		gen.thr,
	)
}

type gcontext tcontext

func (gen gcontext) Generate(context.Context, interface{}) Throttler {
	return NewThrottlerContext()
}

type genqueue tenqueue

func (gen genqueue) Generate(context.Context, interface{}) Throttler {
	return NewThrottlerEnqueue(gen.enq)
}

type gkeyed tkeyed

func (gen gkeyed) Generate(context.Context, interface{}) Throttler {
	return NewThrottlerKeyed(gen.gen)
}

type gall tall

func (gen gall) Generate(context.Context, interface{}) Throttler {
	return NewThrottlerAll(gen)
}

type gany tany

func (gen gany) Generate(context.Context, interface{}) Throttler {
	return NewThrottlerAny(gen)
}

type gnot tnot

func (gen gnot) Generate(context.Context, interface{}) Throttler {
	return NewThrottlerNot(gen.thr)
}
