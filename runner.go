package gohalt

import (
	"context"
	"fmt"
	"sync"
)

type Runnable func(context.Context) error

type Runner struct {
	thr Throttler

	ctx context.Context
	wg  sync.WaitGroup

	rep func(error)
	err error
}

func NewRunner(ctx context.Context, thr Throttler) *Runner {
	ctx, cancel := context.WithCancel(ctx)
	r := Runner{thr: thr, ctx: ctx}
	var once sync.Once
	r.rep = func(err error) {
		once.Do(func() {
			r.err = err
			cancel()
		})
	}
	return &r
}

func (r *Runner) Go(run Runnable, key interface{}) {
	r.wg.Add(1)
	go func() {
		ctx := WithKey(r.ctx, key)
		defer func() {
			if _, err := r.thr.Release(ctx); err != nil {
				r.rep(fmt.Errorf("throttler error happened %w", err))
			}
			r.wg.Done()
			return
		}()
		if thrctx, err := r.thr.Acquire(ctx); err != nil {
			r.rep(fmt.Errorf("throttler error happened %w", err))
			return
		} else {
			ctx = thrctx
		}
		select {
		case <-ctx.Done():
			r.rep(fmt.Errorf("context error happened %w", ctx.Err()))
			return
		default:
		}
		if err := run(ctx); err != nil {
			r.rep(fmt.Errorf("run error happened %w", err))
			return
		}
	}()
}

func (r *Runner) Wait() error {
	r.wg.Wait()
	r.rep(nil)
	return r.err
}
