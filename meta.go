package gohalt

import (
	"context"
	"math"
	"strconv"
)

type Meta struct {
	Limit     uint64
	Remaining uint64
	Reset     uint64
}

var DefaultMeta Meta = Meta{
	Limit:     math.MaxUint64,
	Remaining: math.MaxUint64,
}

func NewMeta(ctx context.Context, thr Throttler) Meta {
	mt := DefaultMeta
	thr.accept(ctx, &mt)
	return mt
}

func (m Meta) Headers() map[string]string {
	return map[string]string{
		"X-RateLimit-Limit":     strconv.FormatUint(m.Limit, 10),
		"X-RateLimit-Remaining": strconv.FormatUint(m.Remaining, 10),
		"X-RateLimit-Reset":     strconv.FormatUint(m.Reset, 10),
	}
}

func (m *Meta) tvisitEcho(ctx context.Context, thr *techo) {
	if thr.err != nil {
		m.Limit = 0
		m.Remaining = 0
		m.Reset = math.MaxUint64
	}
}

func (m *Meta) tvisitWait(ctx context.Context, thr *twait) {
}

func (m *Meta) tvisitPanic(ctx context.Context, thr *tpanic) {
	m.Limit = 0
	m.Remaining = 0
	m.Reset = math.MaxUint64
}

func (m *Meta) tvisitEach(ctx context.Context, thr *teach) {
}

func (m *Meta) tvisitBefore(ctx context.Context, thr *tbefore) {
}

func (m *Meta) tvisitChance(ctx context.Context, thr *tchance) {
}

func (m *Meta) tvisitFixed(ctx context.Context, thr *tfixed) {
	m.Limit = thr.limit
	m.Remaining = thr.limit - thr.current
}

func (m *Meta) tvisitRunning(ctx context.Context, thr *trunning) {
}

func (m *Meta) tvisitBuffered(ctx context.Context, thr *tbuffered) {
}

func (m *Meta) tvisitPriority(ctx context.Context, thr *tpriority) {
}

func (m *Meta) tvisitTimed(ctx context.Context, thr *ttimed) {
	m.Limit = thr.limit
	m.Remaining = thr.limit - thr.current
	m.Reset = uint64(thr.interval)
}

func (m *Meta) tvisitMonitor(ctx context.Context, thr *tmonitor) {
}

func (m *Meta) tvisitMetric(ctx context.Context, thr *tmetric) {
}

func (m *Meta) tvisitLatency(ctx context.Context, thr *tlatency) {
}

func (m *Meta) tvisitPercentile(ctx context.Context, thr *tpercentile) {
}

func (m *Meta) tvisitAdaptive(ctx context.Context, thr *tadaptive) {
	m.Limit = thr.limit
	m.Remaining = thr.limit - thr.current
	m.Reset = uint64(thr.interval)
}

func (m *Meta) tvisitContext(ctx context.Context, thr *tcontext) {
}

func (m *Meta) tvisitEnqueue(ctx context.Context, thr *tenqueue) {
}

func (m *Meta) tvisitKeyed(ctx context.Context, thr *tkeyed) {
}

func (m *Meta) tvisitAll(ctx context.Context, thr *tall) {
	thrs := *thr
	for _, thr := range thrs {
		tm := DefaultMeta
		thr.accept(ctx, &tm)
		if tm.Limit < m.Limit {
			m.Limit = tm.Limit
		}
		if tm.Remaining < m.Remaining {
			m.Remaining = tm.Remaining
		}
		if tm.Reset > m.Reset {
			m.Reset = tm.Reset
		}
	}
}

func (m *Meta) tvisitAny(ctx context.Context, thr *tany) {
	thrs := *thr
	for _, thr := range thrs {
		tm := DefaultMeta
		thr.accept(ctx, &tm)
		if tm.Limit > m.Limit {
			m.Limit = tm.Limit
		}
		if tm.Remaining > m.Remaining {
			m.Remaining = tm.Remaining
		}
		if tm.Reset < m.Reset {
			m.Reset = tm.Reset
		}
	}
}

func (m *Meta) tvisitNot(ctx context.Context, thr *tnot) {
}

func (m *Meta) tvisitSuppress(ctx context.Context, thr *tsuppress) {
}
