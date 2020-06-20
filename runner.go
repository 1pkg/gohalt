package gohalt

import (
	"context"
	"fmt"
	"sync"
)

type Runnable func(context.Context) error
type Logger func(...interface{})

func discard(...interface{}) {}

type Runner struct {
	thr Throttler
	ctx context.Context
	wg  sync.WaitGroup

	report func(error)
	err    error
}

func NewRunner(ctx context.Context, thr Throttler) *Runner {
	return NewRunnerWithLogger(ctx, thr, discard)
}

func NewRunnerWithLogger(ctx context.Context, thr Throttler, log Logger) *Runner {
	ctx, cancel := context.WithCancel(ctx)
	r := Runner{thr: thr, ctx: ctx}
	var once sync.Once
	r.report = func(err error) {
		if err != nil {
			log(err.Error())
		}
		once.Do(func() {
			r.err = err
			cancel()
		})
	}
	return &r
}

func (r *Runner) Go(run Runnable) {
	r.wg.Add(1)
	go func() {
		defer func() {
			if err := r.thr.Release(r.ctx); err != nil {
				r.report(fmt.Errorf("throttler error happened: %w", err))
			}
			r.wg.Done()
			return
		}()
		if err := r.thr.Acquire(r.ctx); err != nil {
			r.report(fmt.Errorf("throttler error happened: %w", err))
			return
		}
		select {
		case <-r.ctx.Done():
			r.report(fmt.Errorf("context error happened: %w", r.ctx.Err()))
			return
		default:
		}
		if err := run(r.ctx); err != nil {
			r.report(fmt.Errorf("run error happened: %w", err))
			return
		}
	}()
}

func (r *Runner) Wait() error {
	r.wg.Wait()
	r.report(nil)
	return r.err
}
