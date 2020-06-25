package gohalt

import (
	"context"
	"encoding/json"
)

type Marshaler func(interface{}) ([]byte, error)

var DefaultMarshaler Marshaler = json.Marshal

type carry struct {
	Type   string
	Object interface{}
}

func (m Marshaler) MarshalThrottler(ctx context.Context, thr Throttler) ([]byte, error) {
	obj := thr.accept(ctx, m)
	return m(obj)
}

func (m Marshaler) tvisitEcho(ctx context.Context, thr techo) interface{} {
	return nil
}

func (m Marshaler) tvisitWait(ctx context.Context, thr twait) interface{} {
	return nil
}

func (m Marshaler) tvisitPanic(ctx context.Context, thr tpanic) interface{} {
	return nil
}

func (m Marshaler) tvisitEach(ctx context.Context, thr teach) interface{} {
	return carry{
		Type:   "teach",
		Object: thr.cur,
	}
}

func (m Marshaler) tvisitAfter(ctx context.Context, thr tafter) interface{} {
	return carry{
		Type:   "tafter",
		Object: thr.cur,
	}
}

func (m Marshaler) tvisitChance(ctx context.Context, thr tchance) interface{} {
	return nil
}

func (m Marshaler) tvisitFixed(ctx context.Context, thr tfixed) interface{} {
	return carry{
		Type:   "tfixed",
		Object: thr.cur,
	}
}

func (m Marshaler) tvisitRunning(ctx context.Context, thr trunning) interface{} {
	return nil
}

func (m Marshaler) tvisitBuffered(ctx context.Context, thr tbuffered) interface{} {
	return nil
}

func (m Marshaler) tvisitPriority(ctx context.Context, thr tpriority) interface{} {
	return nil
}

func (m Marshaler) tvisitTimed(ctx context.Context, thr ttimed) interface{} {
	return carry{
		Type:   "ttimed",
		Object: thr.cur,
	}
}

func (m Marshaler) tvisitMonitor(ctx context.Context, thr tmonitor) interface{} {
	return nil
}

func (m Marshaler) tvisitMetric(ctx context.Context, thr tmetric) interface{} {
	return nil
}

func (m Marshaler) tvisitLatency(ctx context.Context, thr tlatency) interface{} {
	return nil
}

func (m Marshaler) tvisitPercentile(ctx context.Context, thr tpercentile) interface{} {
	return nil
}

func (m Marshaler) tvisitAdaptive(ctx context.Context, thr tadaptive) interface{} {
	return []interface{}{
		carry{
			Type:   "tadaptive",
			Object: thr.cur,
		},
		thr.accept(ctx, m),
	}
}

func (m Marshaler) tvisitContext(ctx context.Context, thr tcontext) interface{} {
	return nil
}

func (m Marshaler) tvisitEnqueue(ctx context.Context, thr tenqueue) interface{} {
	return nil
}

func (m Marshaler) tvisitKeyed(ctx context.Context, thr tkeyed) interface{} {
	obj := make(map[interface{}]interface{})
	thr.store.Range(func(key interface{}, val interface{}) bool {
		obj[key] = val.(Throttler).accept(ctx, m)
		return true
	})
	return carry{
		Type:   "tkeyed",
		Object: obj,
	}
}

func (m Marshaler) tvisitAll(ctx context.Context, thrs tall) interface{} {
	obj := make([]interface{}, 0, len(thrs))
	for _, thr := range thrs {
		obj = append(obj, thr.accept(ctx, m))
	}
	return carry{
		Type:   "tall",
		Object: obj,
	}
}

func (m Marshaler) tvisitAny(ctx context.Context, thrs tany) interface{} {
	obj := make([]interface{}, 0, len(thrs))
	for _, thr := range thrs {
		obj = append(obj, thr.accept(ctx, m))
	}
	return carry{
		Type:   "tany",
		Object: obj,
	}
}

func (m Marshaler) tvisitNot(ctx context.Context, thr tnot) interface{} {
	return carry{
		Type:   "tnot",
		Object: thr.thr.accept(ctx, m),
	}
}
