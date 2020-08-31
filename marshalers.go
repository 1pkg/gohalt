package gohalt

import (
	"context"
	"encoding/json"
)

type Marshaler func(interface{}) ([]byte, error)

var DefaultMarshaler Marshaler = json.Marshal

type marshalerMock struct {
	err error
}

func (m marshalerMock) Marshal(interface{}) ([]byte, error) {
	return nil, m.err
}

type mobject struct {
	obj interface{}
}

func (m Marshaler) Marshal(ctx context.Context, thr Throttler) ([]byte, error) {
	var mobj mobject
	thr.accept(ctx, &mobj)
	return m(mobj.obj)
}

func (mobj *mobject) tvisitEcho(ctx context.Context, thr *techo) {
}

func (mobj *mobject) tvisitWait(ctx context.Context, thr *twait) {
}

func (mobj *mobject) tvisitBackoff(ctx context.Context, thr *tbackoff) {
	mobj.obj = thr.current
}

func (mobj *mobject) tvisitPanic(ctx context.Context, thr *tpanic) {
}

func (mobj *mobject) tvisitEach(ctx context.Context, thr *teach) {
	mobj.obj = thr.current
}

func (mobj *mobject) tvisitBefore(ctx context.Context, thr *tbefore) {
	mobj.obj = thr.current
}

func (mobj *mobject) tvisitChance(ctx context.Context, thr *tchance) {
}

func (mobj *mobject) tvisitAfter(ctx context.Context, thr *tafter) {
	mobj.obj = thr.current
}

func (mobj *mobject) tvisitRunning(ctx context.Context, thr *trunning) {
}

func (mobj *mobject) tvisitBuffered(ctx context.Context, thr *tbuffered) {
}

func (mobj *mobject) tvisitPriority(ctx context.Context, thr *tpriority) {
}

func (mobj *mobject) tvisitTimed(ctx context.Context, thr *ttimed) {
	mobj.obj = thr.current
}

func (mobj *mobject) tvisitMonitor(ctx context.Context, thr *tmonitor) {
}

func (mobj *mobject) tvisitMetric(ctx context.Context, thr *tmetric) {
}

func (mobj *mobject) tvisitLatency(ctx context.Context, thr *tlatency) {
}

func (mobj *mobject) tvisitPercentile(ctx context.Context, thr *tpercentile) {
}

func (mobj *mobject) tvisitAdaptive(ctx context.Context, thr *tadaptive) {
	var tobj mobject
	thr.thr.accept(ctx, &tobj)
	mobj.obj = []interface{}{
		[]interface{}{thr.current, thr.step},
		tobj.obj,
	}
}

func (mobj *mobject) tvisitContext(ctx context.Context, thr *tcontext) {
}

func (mobj *mobject) tvisitEnqueue(ctx context.Context, thr *tenqueue) {
}

func (mobj *mobject) tvisitPattern(ctx context.Context, thr *tpattern) {
	patterns := *thr
	tobjs := make([]interface{}, 0, len(patterns))
	for _, pattern := range patterns {
		var tobj mobject
		pattern.Throttler.accept(ctx, &tobj)
		tobjs = append(tobjs, tobj.obj)
	}
	mobj.obj = tobjs
}

func (mobj *mobject) tvisitRing(ctx context.Context, thr *tring) {
	tobjs := make([]interface{}, 0, len(thr.thrs))
	for _, thr := range thr.thrs {
		var tobj mobject
		thr.accept(ctx, &tobj)
		tobjs = append(tobjs, tobj.obj)
	}
	mobj.obj = []interface{}{
		tobjs,
		thr.acquire,
		thr.release,
	}
}

func (mobj *mobject) tvisitAll(ctx context.Context, thr *tall) {
	thrs := *thr
	tobjs := make([]interface{}, 0, len(thrs))
	for _, thr := range thrs {
		var tobj mobject
		thr.accept(ctx, &tobj)
		tobjs = append(tobjs, tobj.obj)
	}
	mobj.obj = tobjs
}

func (mobj *mobject) tvisitAny(ctx context.Context, thr *tany) {
	thrs := *thr
	tobjs := make([]interface{}, 0, len(thrs))
	for _, thr := range thrs {
		var tobj mobject
		thr.accept(ctx, &tobj)
		tobjs = append(tobjs, tobj.obj)
	}
	mobj.obj = tobjs
}

func (mobj *mobject) tvisitNot(ctx context.Context, thr *tnot) {
	thr.thr.accept(ctx, mobj)
}

func (mobj *mobject) tvisitSuppress(ctx context.Context, thr *tsuppress) {
	thr.thr.accept(ctx, mobj)
}
