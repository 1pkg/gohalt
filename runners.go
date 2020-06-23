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

func NewRunner(ctx context.Context, thr Throttler) (*Runner, context.Context) {
	ctx, cancel := context.WithCancel(ctx)
	r := Runner{thr: thr, ctx: ctx}
	var once sync.Once
	r.rep = func(err error) {
		once.Do(func() {
			r.err = err
			cancel()
		})
	}
	return &r, ctx
}

func (r *Runner) Go(run Runnable) {
	r.GoWithContext(r.ctx, run)
}

func (r *Runner) GoWithParams(
	run Runnable,
	priority uint8,
	key interface{},
	data interface{},
	marshaler Marshaler,
) {
	r.ctx = WithPriority(r.ctx, priority)
	r.ctx = WithKey(r.ctx, key)
	r.ctx = WithData(r.ctx, data)
	r.ctx = WithTimestamp(r.ctx)
	r.ctx = WithMarshaler(r.ctx, marshaler)
	r.GoWithContext(r.ctx, run)
}

func (r *Runner) GoWithContext(ctx context.Context, run Runnable) {
	r.wg.Add(1)
	go func() {
		defer func() {
			if err := r.thr.Release(ctx); err != nil {
				r.rep(fmt.Errorf("throttler error has happened %w", err))
			}
			r.wg.Done()
			return
		}()
		if err := r.thr.Acquire(ctx); err != nil {
			r.rep(fmt.Errorf("throttler error has happened %w", err))
			return
		}
		select {
		case <-ctx.Done():
			r.rep(fmt.Errorf("context error has happened %w", ctx.Err()))
			return
		default:
		}
		if err := run(ctx); err != nil {
			r.rep(fmt.Errorf("function error has happened %w", err))
			return
		}
	}()
}

func (r *Runner) Wait() error {
	r.wg.Wait()
	r.rep(nil)
	return r.err
}
