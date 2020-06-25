package gohalt

import (
	"context"
	"encoding/json"
)

type Marshaler func(interface{}) ([]byte, error)
type Unmarshaler func([]byte, interface{}) error

var DefaultMarshaler Marshaler = json.Marshal
var DefaultUnmarshaler Unmarshaler = json.Unmarshal

type object struct {
	obj interface{}
}

func (m Marshaler) MarshalThrottler(ctx context.Context, thr Throttler) ([]byte, error) {
	obj := thr.accept(ctx, m)
	return m(obj)
}

func (um Unmarshaler) UnmarshalThrottler(ctx context.Context, bytes []byte, thr Throttler) (Throttler, error) {
	var obj interface{}
	if err := um(bytes, obj); err != nil {
		return thr, err
	}
	return thr.accept(ctx, object{obj: obj}).(Throttler), nil
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
	return thr.cur
}

func (m Marshaler) tvisitAfter(ctx context.Context, thr tafter) interface{} {
	return thr.cur
}

func (m Marshaler) tvisitChance(ctx context.Context, thr tchance) interface{} {
	return nil
}

func (m Marshaler) tvisitFixed(ctx context.Context, thr tfixed) interface{} {
	return thr.cur
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
	return thr.cur
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
		thr.cur,
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
	return nil
}

func (m Marshaler) tvisitAll(ctx context.Context, thrs tall) interface{} {
	obj := make([]interface{}, 0, len(thrs))
	for _, thr := range thrs {
		obj = append(obj, thr.accept(ctx, m))
	}
	return obj
}

func (m Marshaler) tvisitAny(ctx context.Context, thrs tany) interface{} {
	obj := make([]interface{}, 0, len(thrs))
	for _, thr := range thrs {
		obj = append(obj, thr.accept(ctx, m))
	}
	return obj
}

func (m Marshaler) tvisitNot(ctx context.Context, thr tnot) interface{} {
	return thr.thr.accept(ctx, m)
}

func (obj object) tvisitEcho(ctx context.Context, thr techo) interface{} {
	return thr
}

func (obj object) tvisitWait(ctx context.Context, thr twait) interface{} {
	return thr
}

func (obj object) tvisitPanic(ctx context.Context, thr tpanic) interface{} {
	return thr
}

func (obj object) tvisitEach(ctx context.Context, thr teach) interface{} {
	thr.cur = uint64(obj.obj.(float64))
	return thr
}

func (obj object) tvisitAfter(ctx context.Context, thr tafter) interface{} {
	thr.cur = uint64(obj.obj.(float64))
	return thr
}

func (obj object) tvisitChance(ctx context.Context, thr tchance) interface{} {
	return thr
}

func (obj object) tvisitFixed(ctx context.Context, thr tfixed) interface{} {
	thr.cur = uint64(obj.obj.(float64))
	return thr
}

func (obj object) tvisitRunning(ctx context.Context, thr trunning) interface{} {
	return thr
}

func (obj object) tvisitBuffered(ctx context.Context, thr tbuffered) interface{} {
	return thr
}

func (obj object) tvisitPriority(ctx context.Context, thr tpriority) interface{} {
	return thr
}

func (obj object) tvisitTimed(ctx context.Context, thr ttimed) interface{} {
	thr.cur = uint64(obj.obj.(float64))
	return thr
}

func (obj object) tvisitMonitor(ctx context.Context, thr tmonitor) interface{} {
	return thr
}

func (obj object) tvisitMetric(ctx context.Context, thr tmetric) interface{} {
	return thr
}

func (obj object) tvisitLatency(ctx context.Context, thr tlatency) interface{} {
	return thr
}

func (obj object) tvisitPercentile(ctx context.Context, thr tpercentile) interface{} {
	return thr
}

func (obj object) tvisitAdaptive(ctx context.Context, thr tadaptive) interface{} {
	objs := obj.obj.([]interface{})
	thr.cur = uint64(objs[0].(float64))
	thr.thr = thr.thr.accept(ctx, object{obj: objs[1]}).(Throttler)
	return thr
}

func (obj object) tvisitContext(ctx context.Context, thr tcontext) interface{} {
	return thr
}

func (obj object) tvisitEnqueue(ctx context.Context, thr tenqueue) interface{} {
	return thr
}

func (obj object) tvisitKeyed(ctx context.Context, thr tkeyed) interface{} {
	return thr
}

func (obj object) tvisitAll(ctx context.Context, thrs tall) interface{} {
	objs := obj.obj.([]interface{})
	nthrs := make([]Throttler, len(thrs))
	for i, thr := range thrs {
		nthrs[i] = thr.accept(ctx, object{obj: objs[i]}).(Throttler)
	}
	return nthrs
}

func (obj object) tvisitAny(ctx context.Context, thrs tany) interface{} {
	objs := obj.obj.([]interface{})
	nthrs := make([]Throttler, len(thrs))
	for i, thr := range thrs {
		nthrs[i] = thr.accept(ctx, object{obj: objs[i]}).(Throttler)
	}
	return nthrs
}

func (obj object) tvisitNot(ctx context.Context, thr tnot) interface{} {
	thr.thr = thr.thr.accept(ctx, obj).(Throttler)
	return thr
}
