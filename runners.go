package gohalt

import (
	"context"
	"fmt"
	"sync"
)

type Runnable func(context.Context) error

type Runner struct {
	thr    Throttler
	wg     sync.WaitGroup
	ctx    context.Context
	err    error
	report func(error)
}

func NewRunner(ctx context.Context, thr Throttler) (*Runner, context.Context) {
	ctx, cancel := context.WithCancel(ctx)
	r := Runner{thr: thr, ctx: ctx}
	var once sync.Once
	r.report = func(err error) {
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
				r.report(fmt.Errorf("throttler error has happened %w", err))
			}
			r.wg.Done()
			return
		}()
		if err := r.thr.Acquire(ctx); err != nil {
			r.report(fmt.Errorf("throttler error has happened %w", err))
			return
		}
		select {
		case <-ctx.Done():
			r.report(fmt.Errorf("context error has happened %w", ctx.Err()))
			return
		default:
		}
		if err := run(ctx); err != nil {
			r.report(fmt.Errorf("function error has happened %w", err))
			return
		}
	}()
}

func (r *Runner) Wait() error {
	r.wg.Wait()
	r.report(nil)
	return r.err
}
