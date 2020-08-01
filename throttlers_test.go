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
	tms  uint64
	thr  Throttler
	act  Runnable
	ctxs []context.Context
	errs []error
	durs []time.Duration
}

func (t tcase) run(index int) (err error, dur time.Duration) {
	// get context with fallback
	ctx := context.Background()
	if index < len(t.ctxs) {
		ctx = t.ctxs[index]
	}
	ts := time.Now()
	// try catch panic into error
	func() {
		defer func() {
			if msg := recover(); msg != nil {
				err = errors.New(msg.(string))
			}
		}()
		err = t.thr.Acquire(ctx)
	}()
	dur = time.Since(ts)
	// run additional payload only if present
	if t.act != nil {
		_ = t.act(ctx)
	}
	// imitate over releasing
	for i := 0; i < index+1; i++ {
		if err := t.thr.Release(ctx); err != nil {
			return err, dur
		}
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
			tms: 3,
			thr: NewThrottlerEcho(nil),
		},
		"Throttler echo should throttle on not nil input": {
			tms: 3,
			thr: NewThrottlerEcho(errors.New("test")),
			errs: []error{
				errors.New("test"),
				errors.New("test"),
				errors.New("test"),
			},
		},
		"Throttler wait should sleep for millisecond": {
			tms: 3,
			thr: NewThrottlerWait(time.Millisecond),
			durs: []time.Duration{
				time.Millisecond,
				time.Millisecond,
				time.Millisecond,
			},
		},
		"Throttler panic should panic": {
			tms: 3,
			thr: NewThrottlerPanic(),
			errs: []error{
				errors.New("throttler has reached panic"),
				errors.New("throttler has reached panic"),
				errors.New("throttler has reached panic"),
			},
		},
		"Throttler each should throttle on threshold": {
			tms: 6,
			thr: NewThrottlerEach(3),
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
			tms: 6,
			thr: NewThrottlerBefore(3),
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
			tms: 3,
			thr: NewThrottlerChance(1),
			errs: []error{
				errors.New("throttler has reached chance threshold"),
				errors.New("throttler has reached chance threshold"),
				errors.New("throttler has reached chance threshold"),
			},
		},
		"Throttler chance should throttle on >1": {
			tms: 3,
			thr: NewThrottlerChance(10.10),
			errs: []error{
				errors.New("throttler has reached chance threshold"),
				errors.New("throttler has reached chance threshold"),
				errors.New("throttler has reached chance threshold"),
			},
		},
		"Throttler chance should not throttle on 0": {
			tms: 3,
			thr: NewThrottlerChance(0),
		},
		"Throttler after should throttle after threshold": {
			tms: 6,
			thr: NewThrottlerAfter(3),
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
			tms: 3,
			thr: NewThrottlerRunning(1),
			act: once(time.Millisecond, nope),
			errs: []error{
				nil,
				errors.New("throttler has exceed running threshold"),
				errors.New("throttler has exceed running threshold"),
			},
		},
		"Throttler buffered should throttle on threshold": {
			tms: 3,
			thr: NewThrottlerBuffered(1),
			act: once(time.Millisecond, nope),
			durs: []time.Duration{
				0,
				time.Millisecond,
				time.Millisecond,
			},
		},
		"Throttler priority should throttle on threshold": {
			tms: 3,
			thr: NewThrottlerPriority(1, 0),
			act: once(time.Millisecond, nope),
			durs: []time.Duration{
				0,
				time.Millisecond,
				time.Millisecond,
			},
		},
		"Throttler priority should not throttle on priority": {
			tms: 7,
			thr: NewThrottlerPriority(5, 2),
			act: once(time.Millisecond, nope),
			ctxs: []context.Context{
				WithPriority(context.Background(), 1),
				WithPriority(context.Background(), 1),
				WithPriority(context.Background(), 1),
				WithPriority(context.Background(), 2),
				WithPriority(context.Background(), 2),
				WithPriority(context.Background(), 2),
				WithPriority(context.Background(), 2),
			},
			durs: []time.Duration{
				0,
				0,
				time.Millisecond,
				0,
				0,
				0,
				time.Millisecond,
			},
		},
		"Throttler monitor should throttle with error on internal error": {
			tms: 3,
			thr: NewThrottlerMonitor(
				mntmock{err: errors.New("test")},
				Stats{},
			),
			errs: []error{
				fmt.Errorf("throttler hasn't found any stats %w", errors.New("test")),
				fmt.Errorf("throttler hasn't found any stats %w", errors.New("test")),
				fmt.Errorf("throttler hasn't found any stats %w", errors.New("test")),
			},
		},
		"Throttler monitor should not throttle on stats below threshold": {
			tms: 3,
			thr: NewThrottlerMonitor(
				mntmock{
					stats: Stats{
						MEMAlloc:  100,
						MEMSystem: 1000,
						CPUPause:  100,
						CPUUsage:  0.1,
					},
				},
				Stats{
					MEMAlloc:  1000,
					MEMSystem: 2000,
					CPUPause:  500,
					CPUUsage:  0.3,
				},
			),
		},
		"Throttler monitor should throttle on stats above threshold": {
			tms: 3,
			thr: NewThrottlerMonitor(
				mntmock{
					stats: Stats{
						MEMAlloc:  500,
						MEMSystem: 5000,
						CPUPause:  500,
						CPUUsage:  0.1,
					},
				},
				Stats{
					MEMAlloc:  1000,
					MEMSystem: 2000,
					CPUPause:  500,
					CPUUsage:  0.3,
				},
			),
			errs: []error{
				errors.New("throttler has exceed stats threshold"),
				errors.New("throttler has exceed stats threshold"),
				errors.New("throttler has exceed stats threshold"),
			},
		},
		"Throttler metric should throttle with error on internal error": {
			tms: 3,
			thr: NewThrottlerMetric(
				mtcmock{err: errors.New("test")},
			),
			errs: []error{
				fmt.Errorf("throttler hasn't found any metric %w", errors.New("test")),
				fmt.Errorf("throttler hasn't found any metric %w", errors.New("test")),
				fmt.Errorf("throttler hasn't found any metric %w", errors.New("test")),
			},
		},
		"Throttler monitor should not throttle on metric below threshold": {
			tms: 3,
			thr: NewThrottlerMetric(
				mtcmock{metric: false},
			),
		},
		"Throttler monitor should throttle on metric above threshold": {
			tms: 3,
			thr: NewThrottlerMetric(
				mtcmock{metric: true},
			),
			errs: []error{
				errors.New("throttler has reached metric threshold"),
				errors.New("throttler has reached metric threshold"),
				errors.New("throttler has reached metric threshold"),
			},
		},
	}
	for tname, tcase := range table {
		t.Run(tname, func(t *testing.T) {
			var index int64
			for i := 0; i < int(tcase.tms); i++ {
				t.Run(fmt.Sprintf("run %d", i+1), func(t *testing.T) {
					t.Parallel()
					index := int(atomic.AddInt64(&index, 1) - 1)
					resErr, resDur := tcase.result(index)
					err, dur := tcase.run(index)
					assert.Equal(t, resErr, err)
					assert.LessOrEqual(t, int64(resDur), int64(dur))
				})
			}
		})
	}
}
