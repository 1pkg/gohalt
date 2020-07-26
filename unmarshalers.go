package gohalt

import (
	"context"
	"encoding/json"
)

type Unmarshaler func([]byte, interface{}) error

var DefaultUnmarshaler Unmarshaler = json.Unmarshal

type umobject struct {
	obj interface{}
}

func (um Unmarshaler) Unmarshal(ctx context.Context, bytes []byte, thr Throttler) error {
	var umobj umobject
	if err := um(bytes, &umobj.obj); err != nil {
		return err
	}
	thr.accept(ctx, umobj)
	return nil
}

func (umobj umobject) tvisitEcho(ctx context.Context, thr *techo) {
}

func (umobj umobject) tvisitWait(ctx context.Context, thr *twait) {
}

func (umobj umobject) tvisitPanic(ctx context.Context, thr *tpanic) {
}

func (umobj umobject) tvisitEach(ctx context.Context, thr *teach) {
	thr.current = uint64(umobj.obj.(float64))
}

func (umobj umobject) tvisitBefore(ctx context.Context, thr *tbefore) {
	thr.current = uint64(umobj.obj.(float64))
}

func (umobj umobject) tvisitChance(ctx context.Context, thr *tchance) {
}

func (umobj umobject) tvisitAfter(ctx context.Context, thr *tafter) {
	thr.current = uint64(umobj.obj.(float64))
}

func (umobj umobject) tvisitRunning(ctx context.Context, thr *trunning) {
}

func (umobj umobject) tvisitBuffered(ctx context.Context, thr *tbuffered) {
}

func (umobj umobject) tvisitPriority(ctx context.Context, thr *tpriority) {
}

func (umobj umobject) tvisitTimed(ctx context.Context, thr *ttimed) {
	thr.current = uint64(umobj.obj.(float64))
}

func (umobj umobject) tvisitMonitor(ctx context.Context, thr *tmonitor) {
}

func (umobj umobject) tvisitMetric(ctx context.Context, thr *tmetric) {
}

func (umobj umobject) tvisitLatency(ctx context.Context, thr *tlatency) {
}

func (umobj umobject) tvisitPercentile(ctx context.Context, thr *tpercentile) {
}

func (umobj umobject) tvisitAdaptive(ctx context.Context, thr *tadaptive) {
	objs := umobj.obj.([]interface{})
	thr.current = uint64(objs[0].([]interface{})[0].(float64))
	thr.step = uint64(objs[0].([]interface{})[1].(float64))
	tobj := umobject{obj: objs[1]}
	thr.thr.accept(ctx, tobj)
}

func (umobj umobject) tvisitContext(ctx context.Context, thr *tcontext) {
}

func (umobj umobject) tvisitEnqueue(ctx context.Context, thr *tenqueue) {
}

func (umobj umobject) tvisitKeyed(ctx context.Context, thr *tkeyed) {
}

func (umobj umobject) tvisitAll(ctx context.Context, thr *tall) {
	thrs := *thr
	objs := umobj.obj.([]interface{})
	for i := range thrs {
		tobj := umobject{obj: objs[i]}
		thrs[i].accept(ctx, tobj)
	}
}

func (umobj umobject) tvisitAny(ctx context.Context, thr *tany) {
	thrs := *thr
	objs := umobj.obj.([]interface{})
	for i := range thrs {
		tobj := umobject{obj: objs[i]}
		thrs[i].accept(ctx, tobj)
	}
}

func (umobj umobject) tvisitNot(ctx context.Context, thr *tnot) {
	thr.thr.accept(ctx, umobj)
}

func (umobj umobject) tvisitSuppress(ctx context.Context, thr *tsuppress) {
	thr.thr.accept(ctx, umobj)
}
