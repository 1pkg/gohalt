package gohalt

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestThrottlerPattern(t *testing.T) {
	table := map[string]struct {
		thr      Throttler
		ctx      context.Context
		run      Runnable
		errs     []error
		wait     time.Duration
		panic    bool
		parallel bool
	}{
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
			wait: time.Millisecond,
		},
		"Throttler panic should panic": {
			thr:   NewThrottlerPanic(),
			ctx:   context.Background(),
			panic: true,
		},
		"Throttler each should throttle on threshold": {
			thr: NewThrottlerEach(3),
			ctx: context.Background(),
			errs: []error{
				nil,
				nil,
				errors.New("throttler has reached periodic threshold 3"),
				nil,
				nil,
				errors.New("throttler has reached periodic threshold 6"),
			},
		},
		"Throttler before should throttle before threshold": {
			thr: NewThrottlerBefore(3),
			ctx: context.Background(),
			errs: []error{
				errors.New("throttler has not reached threshold yet 1"),
				errors.New("throttler has not reached threshold yet 2"),
				errors.New("throttler has not reached threshold yet 3"),
				nil,
				nil,
				nil,
			},
		},
		"Throttler chance should throttle on 1": {
			thr: NewThrottlerChance(1),
			ctx: context.Background(),
			errs: []error{
				errors.New("throttler has caught chance threshold"),
				errors.New("throttler has caught chance threshold"),
				errors.New("throttler has caught chance threshold"),
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
				errors.New("throttler has exceed threshold 4"),
				errors.New("throttler has exceed threshold 5"),
				errors.New("throttler has exceed threshold 6"),
			},
		},
		"Throttler running should throttle on threshold": {
			thr: NewThrottlerRunning(1),
			ctx: context.Background(),
			run: once(time.Millisecond, nope),
			errs: []error{
				nil,
				errors.New("throttler has exceed running threshold 2"),
				errors.New("throttler has exceed running threshold 3"),
			},
			parallel: true,
		},
		"Throttler buffered should throttle on threshold": {
			thr: NewThrottlerBuffered(1),
			ctx: context.Background(),
			run: once(time.Millisecond, nope),
			errs: []error{
				nil,
				nil,
				nil,
			},
			wait:     time.Millisecond,
			parallel: true,
		},
	}
	for tname, tcase := range table {
		tcase := tcase
		t.Run(tname, func(t *testing.T) {
			for i, err := range tcase.errs {
				terr := err
				t.Run(fmt.Sprintf("run %d", i+1), func(t *testing.T) {
					if tcase.parallel {
						t.Parallel()
					}
					ts := time.Now()
					assert.Equal(t, terr, tcase.thr.Acquire(tcase.ctx))
					assert.Less(t, int64(tcase.wait), int64(time.Since(ts)))
					if tcase.run != nil {
						_ = tcase.run(tcase.ctx)
					}
					assert.Equal(t, nil, tcase.thr.Release(tcase.ctx))
				})
			}
			if tcase.panic {
				assert.Panics(t, func() { _ = tcase.thr.Acquire(tcase.ctx) })
				assert.Equal(t, nil, tcase.thr.Release(tcase.ctx))
			}
		})
	}
}
