package gohalt

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestThrottlerPattern(t *testing.T) {
	table := map[string]struct {
		thr   Throttler
		ctx   context.Context
		errs  []error
		err   error
		wait  time.Duration
		panic bool
	}{
		"Throttler echo should not throttle on nil input": {
			thr: NewThrottlerEcho(nil),
			ctx: context.Background(),
			errs: []error{
				nil,
				nil,
				nil,
			},
			err: nil,
		},
		"Throttler echo should throttle on not nil input": {
			thr: NewThrottlerEcho(errors.New("test")),
			ctx: context.Background(),
			errs: []error{
				errors.New("test"),
				errors.New("test"),
				errors.New("test"),
			},
			err: errors.New("test"),
		},
		"Throttler wait should sleep for millisecond": {
			thr: NewThrottlerWait(time.Millisecond),
			errs: []error{
				nil,
				nil,
				nil,
			},
			err:  nil,
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
				errors.New("throttler has reached threshold 3"),
				nil,
				nil,
				errors.New("throttler has reached threshold 6"),
			},
			err: nil,
		},
		"Throttler before should throttle after threshold": {
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
			err: nil,
		},
	}
	for tname, tcase := range table {
		t.Run(tname, func(t *testing.T) {
			for _, err := range tcase.errs {
				ts := time.Now()
				assert.Equal(t, err, tcase.thr.Acquire(tcase.ctx))
				assert.Less(t, int64(tcase.wait), int64(time.Since(ts)))
				assert.Equal(t, tcase.err, tcase.thr.Release(tcase.ctx))
			}
			if tcase.panic {
				assert.Panics(t, func() { _ = tcase.thr.Acquire(tcase.ctx) })
				assert.Equal(t, tcase.err, tcase.thr.Release(tcase.ctx))
			}
		})
	}
}
