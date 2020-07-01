package gohalt

import (
	"context"
	"math"
	"time"
)

type Meta struct {
	Limit     uint64
	Remaining uint64
	Reset     time.Duration
}

var metadef Meta = Meta{
	Limit:     math.MaxUint64,
	Remaining: math.MaxUint64,
}

func (m *Meta) tvisitEcho(ctx context.Context, thr techo) interface{} {
	if thr.err != nil {
		m.Limit = 0
		m.Remaining = 0
		m.Reset = math.MaxInt64
	}
	return nil
}

func (m *Meta) tvisitWait(ctx context.Context, thr twait) interface{} {
	return nil
}

func (m *Meta) tvisitPanic(ctx context.Context, thr tpanic) interface{} {
	m.Limit = 0
	m.Remaining = 0
	m.Reset = math.MaxInt64
	return nil
}

func (m *Meta) tvisitEach(ctx context.Context, thr teach) interface{} {
	return nil
}

func (m *Meta) tvisitAfter(ctx context.Context, thr tafter) interface{} {
	return nil
}

func (m *Meta) tvisitChance(ctx context.Context, thr tchance) interface{} {
	return nil
}

func (m *Meta) tvisitFixed(ctx context.Context, thr tfixed) interface{} {
	m.Limit = thr.limit
	m.Remaining = thr.limit - thr.current
	return nil
}

func (m *Meta) tvisitRunning(ctx context.Context, thr trunning) interface{} {
	return nil
}

func (m *Meta) tvisitBuffered(ctx context.Context, thr tbuffered) interface{} {
	return nil
}

func (m *Meta) tvisitPriority(ctx context.Context, thr tpriority) interface{} {
	return nil
}

func (m *Meta) tvisitTimed(ctx context.Context, thr ttimed) interface{} {
	m.Limit = thr.limit
	m.Remaining = thr.limit - thr.current
	m.Reset = thr.interval
	return nil
}

func (m *Meta) tvisitMonitor(ctx context.Context, thr tmonitor) interface{} {
	return nil
}

func (m *Meta) tvisitMetric(ctx context.Context, thr tmetric) interface{} {
	return nil
}

func (m *Meta) tvisitLatency(ctx context.Context, thr tlatency) interface{} {
	return nil
}

func (m *Meta) tvisitPercentile(ctx context.Context, thr tpercentile) interface{} {
	return nil
}

func (m *Meta) tvisitAdaptive(ctx context.Context, thr tadaptive) interface{} {
	m.Limit = thr.limit
	m.Remaining = thr.limit - thr.current
	m.Reset = thr.interval
	return nil
}

func (m *Meta) tvisitContext(ctx context.Context, thr tcontext) interface{} {
	return nil
}

func (m *Meta) tvisitEnqueue(ctx context.Context, thr tenqueue) interface{} {
	return nil
}

func (m *Meta) tvisitKeyed(ctx context.Context, thr tkeyed) interface{} {
	return nil
}

func (m *Meta) tvisitAll(ctx context.Context, thrs tall) interface{} {
	for _, thr := range thrs {
		meta := metadef
		if err := thr.accept(ctx, &meta); err != nil {
			return nil
		}
		if meta.Limit < m.Limit {
			m.Limit = meta.Limit
		}
		if meta.Remaining < m.Remaining {
			m.Remaining = meta.Remaining
		}
		if meta.Reset > m.Reset {
			m.Reset = meta.Reset
		}
	}
	return nil
}

func (m *Meta) tvisitAny(ctx context.Context, thrs tany) interface{} {
	for _, thr := range thrs {
		meta := metadef
		if err := thr.accept(ctx, &meta); err != nil {
			return nil
		}
		if meta.Limit > m.Limit {
			m.Limit = meta.Limit
		}
		if meta.Remaining > m.Remaining {
			m.Remaining = meta.Remaining
		}
		if meta.Reset < m.Reset {
			m.Reset = meta.Reset
		}
	}
	return nil
}

func (m *Meta) tvisitNot(ctx context.Context, thr tnot) interface{} {
	return nil
}
