package gohalt

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestThrottlerEcho(t *testing.T) {
	table := map[string]struct {
		thr techo
		ctx context.Context
		err error
	}{
		"Throttler echo should not throttle on nil input": {
			thr: NewThrottlerEcho(nil),
		},
		"Throttler echo should throttle on not nil input": {
			thr: NewThrottlerEcho(errors.New("error")),
			err: errors.New("error"),
		},
	}
	for tname, tcase := range table {
		t.Run(tname, func(t *testing.T) {
			assert.Equal(t, tcase.err, tcase.thr.Acquire(tcase.ctx))
			assert.Equal(t, tcase.err, tcase.thr.Release(tcase.ctx))
		})
	}
}

func TestThrottlerWait(t *testing.T) {
	table := map[string]struct {
		thr twait
		ctx context.Context
		err error
	}{
		"Throttler wait should sleep for millisecond": {
			thr: NewThrottlerWait(time.Millisecond),
		},
		"Throttler wait should sleep for nanosecond": {
			thr: NewThrottlerWait(time.Nanosecond),
		},
	}
	for tname, tcase := range table {
		t.Run(tname, func(t *testing.T) {
			ts := time.Now()
			assert.Equal(t, tcase.err, tcase.thr.Acquire(tcase.ctx))
			assert.Greater(t, int64(time.Since(ts)), int64(tcase.thr.duration))
			assert.Equal(t, tcase.err, tcase.thr.Release(tcase.ctx))
		})
	}
}

func TestThrottlerPanic(t *testing.T) {
	table := map[string]struct {
		thr tpanic
		ctx context.Context
		err error
	}{
		"Throttler panic should panic": {
			thr: NewThrottlerPanic(),
		},
	}
	for tname, tcase := range table {
		t.Run(tname, func(t *testing.T) {
			assert.Panics(t, func() { _ = tcase.thr.Acquire(tcase.ctx) })
			assert.Equal(t, tcase.err, tcase.thr.Release(tcase.ctx))
		})
	}
}
