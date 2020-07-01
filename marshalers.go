package gohalt

import (
	"context"
	"encoding/json"
)

type Marshaler func(interface{}) ([]byte, error)
type Unmarshaler func([]byte, interface{}) error

var DefaultMarshaler Marshaler = json.Marshal
var DefaultUnmarshaler Unmarshaler = json.Unmarshal

type mobject struct {
	obj interface{}
}

type umobject struct {
	obj interface{}
	thr Throttler
}

func (m Marshaler) Marshal(ctx context.Context, thr Throttler) ([]byte, error) {
	var mobj mobject
	thr.accept(ctx, &mobj)
	return m(mobj.obj)
}

func (um Unmarshaler) Unmarshal(ctx context.Context, bytes []byte, thr Throttler) (Throttler, error) {
	var umobj umobject
	if err := um(bytes, &umobj.obj); err != nil {
		return nil, err
	}
	thr.accept(ctx, &umobj)
	return umobj.thr, nil
}

func (mobj *mobject) tvisitEcho(ctx context.Context, thr techo) {
}

func (mobj *mobject) tvisitWait(ctx context.Context, thr twait) {
}

func (mobj *mobject) tvisitPanic(ctx context.Context, thr tpanic) {
}

func (mobj *mobject) tvisitEach(ctx context.Context, thr teach) {
	mobj.obj = thr.current
}

func (mobj *mobject) tvisitAfter(ctx context.Context, thr tafter) {
	mobj.obj = thr.current
}

func (mobj *mobject) tvisitChance(ctx context.Context, thr tchance) {
}

func (mobj *mobject) tvisitFixed(ctx context.Context, thr tfixed) {
	mobj.obj = thr.current
}

func (mobj *mobject) tvisitRunning(ctx context.Context, thr trunning) {
}

func (mobj *mobject) tvisitBuffered(ctx context.Context, thr tbuffered) {
}

func (mobj *mobject) tvisitPriority(ctx context.Context, thr tpriority) {
}

func (mobj *mobject) tvisitTimed(ctx context.Context, thr ttimed) {
	mobj.obj = thr.current
}

func (mobj *mobject) tvisitMonitor(ctx context.Context, thr tmonitor) {
}

func (mobj *mobject) tvisitMetric(ctx context.Context, thr tmetric) {
}

func (mobj *mobject) tvisitLatency(ctx context.Context, thr tlatency) {
}

func (mobj *mobject) tvisitPercentile(ctx context.Context, thr tpercentile) {
}

func (mobj *mobject) tvisitAdaptive(ctx context.Context, thr tadaptive) {
	var obj mobject
	thr.thr.accept(ctx, &obj)
	mobj.obj = []interface{}{
		thr.current,
		obj.obj,
	}
}

func (mobj *mobject) tvisitContext(ctx context.Context, thr tcontext) {
}

func (mobj *mobject) tvisitEnqueue(ctx context.Context, thr tenqueue) {
}

func (mobj *mobject) tvisitKeyed(ctx context.Context, thr tkeyed) {
}

func (mobj *mobject) tvisitAll(ctx context.Context, thrs tall) {
	objs := make([]interface{}, 0, len(thrs))
	for _, thr := range thrs {
		var obj mobject
		thr.accept(ctx, &obj)
		objs = append(objs, obj.obj)
	}
	mobj.obj = objs
}

func (mobj *mobject) tvisitAny(ctx context.Context, thrs tany) {
	objs := make([]interface{}, 0, len(thrs))
	for _, thr := range thrs {
		var obj mobject
		thr.accept(ctx, &obj)
		objs = append(objs, obj.obj)
	}
	mobj.obj = objs
}

func (mobj *mobject) tvisitNot(ctx context.Context, thr tnot) {
	thr.thr.accept(ctx, mobj)
}

func (umobj *umobject) tvisitEcho(ctx context.Context, thr techo) {
}

func (umobj *umobject) tvisitWait(ctx context.Context, thr twait) {
}

func (umobj *umobject) tvisitPanic(ctx context.Context, thr tpanic) {
}

func (umobj *umobject) tvisitEach(ctx context.Context, thr teach) {
	thr.current = uint64(umobj.obj.(float64))
	umobj.thr = &thr
}

func (umobj *umobject) tvisitAfter(ctx context.Context, thr tafter) {
	thr.current = uint64(umobj.obj.(float64))
	umobj.thr = &thr
}

func (umobj *umobject) tvisitChance(ctx context.Context, thr tchance) {
}

func (umobj *umobject) tvisitFixed(ctx context.Context, thr tfixed) {
	thr.current = uint64(umobj.obj.(float64))
	umobj.thr = &thr
}

func (umobj *umobject) tvisitRunning(ctx context.Context, thr trunning) {
}

func (umobj *umobject) tvisitBuffered(ctx context.Context, thr tbuffered) {
}

func (umobj *umobject) tvisitPriority(ctx context.Context, thr tpriority) {
}

func (umobj *umobject) tvisitTimed(ctx context.Context, thr ttimed) {
	thr.current = uint64(umobj.obj.(float64))
	umobj.thr = &thr
}

func (umobj *umobject) tvisitMonitor(ctx context.Context, thr tmonitor) {
}

func (umobj *umobject) tvisitMetric(ctx context.Context, thr tmetric) {
}

func (umobj *umobject) tvisitLatency(ctx context.Context, thr tlatency) {
}

func (umobj *umobject) tvisitPercentile(ctx context.Context, thr tpercentile) {
}

func (umobj *umobject) tvisitAdaptive(ctx context.Context, thr tadaptive) {
	objs := umobj.obj.([]interface{})
	thr.current = uint64(objs[0].(float64))
	obj := umobject{obj: objs[1]}
	thr.thr.accept(ctx, &obj)
	thr.thr = obj.thr
	umobj.thr = &thr
}

func (umobj *umobject) tvisitContext(ctx context.Context, thr tcontext) {
}

func (umobj *umobject) tvisitEnqueue(ctx context.Context, thr tenqueue) {
}

func (umobj *umobject) tvisitKeyed(ctx context.Context, thr tkeyed) {
}

func (umobj *umobject) tvisitAll(ctx context.Context, thrs tall) {
	objs := umobj.obj.([]interface{})
	for i, thr := range thrs {
		obj := umobject{obj: objs[i]}
		thr.accept(ctx, &obj)
		thrs[i] = obj.thr
	}
	umobj.thr = thrs
}

func (umobj *umobject) tvisitAny(ctx context.Context, thrs tany) {
	objs := umobj.obj.([]interface{})
	for i, thr := range thrs {
		obj := umobject{obj: objs[i]}
		thr.accept(ctx, &obj)
		thrs[i] = obj.thr
	}
	umobj.thr = thrs
}

func (umobj *umobject) tvisitNot(ctx context.Context, thr tnot) {
	thr.thr.accept(ctx, umobj)
}
