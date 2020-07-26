package gohalt

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

type tcase struct {
	thr  Throttler
	ctx  context.Context
	act  Runnable
	errs []error
	durs []time.Duration
}

func (t tcase) run() (err error, dur time.Duration) {
	defer func() {
		if msg := recover(); msg != nil {
			err = errors.New(msg.(string))
		}
	}()
	ts := time.Now()
	err = t.thr.Acquire(t.ctx)
	dur = time.Since(ts)
	if t.act != nil {
		_ = t.act(t.ctx)
	}
	if err := t.thr.Release(t.ctx); err != nil {
		return err, dur
	}
	return
}

func (t tcase) result(index int) (err error, dur time.Duration) {
	if index < len(t.errs) {
		err = t.errs[index]
	}
	if index < len(t.durs) {
		dur = t.durs[index]
	}
	return
}

func TestThrottlerPattern(t *testing.T) {
	table := map[string]tcase{
		"Throttler echo should not throttle on nil input": {
			thr: NewThrottlerEcho(nil),
			ctx: context.Background(),
			errs: []error{
				nil,
				nil,
				nil,
			},
		},
		"Throttler echo should throttle on not nil input": {
			thr: NewThrottlerEcho(errors.New("test")),
			ctx: context.Background(),
			errs: []error{
				errors.New("test"),
				errors.New("test"),
				errors.New("test"),
			},
		},
		"Throttler wait should sleep for millisecond": {
			thr: NewThrottlerWait(time.Millisecond),
			errs: []error{
				nil,
				nil,
				nil,
			},
			durs: []time.Duration{
				time.Millisecond,
				time.Millisecond,
				time.Millisecond,
			},
		},
		"Throttler panic should panic": {
			thr: NewThrottlerPanic(),
			ctx: context.Background(),
			errs: []error{
				errors.New("throttler panic has happened"),
				errors.New("throttler panic has happened"),
				errors.New("throttler panic has happened"),
			},
		},
		"Throttler each should throttle on threshold": {
			thr: NewThrottlerEach(3),
			ctx: context.Background(),
			errs: []error{
				nil,
				nil,
				errors.New("throttler has reached periodic threshold"),
				nil,
				nil,
				errors.New("throttler has reached periodic threshold"),
			},
		},
		"Throttler before should throttle before threshold": {
			thr: NewThrottlerBefore(3),
			ctx: context.Background(),
			errs: []error{
				errors.New("throttler has not reached threshold yet"),
				errors.New("throttler has not reached threshold yet"),
				errors.New("throttler has not reached threshold yet"),
				nil,
				nil,
				nil,
			},
		},
		"Throttler chance should throttle on 1": {
			thr: NewThrottlerChance(1),
			ctx: context.Background(),
			errs: []error{
				errors.New("throttler has reached chance threshold"),
				errors.New("throttler has reached chance threshold"),
				errors.New("throttler has reached chance threshold"),
			},
		},
		"Throttler chance should not throttle on 0": {
			thr: NewThrottlerChance(0),
			ctx: context.Background(),
			errs: []error{
				nil,
				nil,
				nil,
			},
		},
		"Throttler after should throttle after threshold": {
			thr: NewThrottlerAfter(3),
			ctx: context.Background(),
			errs: []error{
				nil,
				nil,
				nil,
				errors.New("throttler has exceed threshold"),
				errors.New("throttler has exceed threshold"),
				errors.New("throttler has exceed threshold"),
			},
		},
		"Throttler running should throttle on threshold": {
			thr: NewThrottlerRunning(1),
			ctx: context.Background(),
			act: once(time.Millisecond, nope),
			errs: []error{
				nil,
				errors.New("throttler has exceed running threshold"),
				errors.New("throttler has exceed running threshold"),
			},
		},
		"Throttler buffered should throttle on threshold": {
			thr: NewThrottlerBuffered(1),
			ctx: context.Background(),
			act: once(time.Millisecond, nope),
			errs: []error{
				nil,
				nil,
				nil,
			},
			durs: []time.Duration{
				0,
				time.Millisecond,
				time.Millisecond,
			},
		},
	}
	for tname, tcase := range table {
		t.Run(tname, func(t *testing.T) {
			var index int64
			for i := range tcase.errs {
				t.Run(fmt.Sprintf("run %d", i+1), func(t *testing.T) {
					t.Parallel()
					err, dur := tcase.run()
					index := int(atomic.AddInt64(&index, 1) - 1)
					resErr, resDur := tcase.result(index)
					assert.Equal(t, resErr, err)
					assert.LessOrEqual(t, int64(resDur), int64(dur))
				})
			}
		})
	}
}
