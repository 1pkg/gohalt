package gohalt

import (
	"context"
	"fmt"
	"sync"
)

type Runner interface {
	Run(Runnable)
	Result() error
}

type rsync struct {
	thr    Throttler
	ctx    context.Context
	err    error
	report func(error)
}

func NewRunnerSync(ctx context.Context, thr Throttler) *rsync {
	ctx, cancel := context.WithCancel(ctx)
	r := rsync{thr: thr, ctx: ctx}
	r.report = func(err error) {
		if r.err != nil {
			r.err = err
			cancel()
		}
	}
	return &r
}

func (r *rsync) Run(run Runnable) {
	defer func() {
		if err := r.thr.Release(r.ctx); err != nil {
			r.report(fmt.Errorf("throttler error has happened %w", err))
		}
		return
	}()
	if err := r.thr.Acquire(r.ctx); err != nil {
		r.report(fmt.Errorf("throttler error has happened %w", err))
		return
	}
	select {
	case <-r.ctx.Done():
		r.report(fmt.Errorf("context error has happened %w", r.ctx.Err()))
		return
	default:
	}
	if err := run(r.ctx); err != nil {
		r.report(fmt.Errorf("function error has happened %w", err))
		return
	}
}

func (r *rsync) Result() error {
	return r.err
}

type rasync struct {
	thr    Throttler
	ctx    context.Context
	wg     sync.WaitGroup
	err    error
	report func(error)
}

func NewRunnerAsync(ctx context.Context, thr Throttler) *rasync {
	ctx, cancel := context.WithCancel(ctx)
	r := rasync{thr: thr, ctx: ctx}
	var once sync.Once
	r.report = func(err error) {
		once.Do(func() {
			r.err = err
			cancel()
		})
	}
	return &r
}

func (r *rasync) Run(run Runnable) {
	r.wg.Add(1)
	go func() {
		defer func() {
			if err := r.thr.Release(r.ctx); err != nil {
				r.report(fmt.Errorf("throttler error has happened %w", err))
			}
			r.wg.Done()
			return
		}()
		if err := r.thr.Acquire(r.ctx); err != nil {
			r.report(fmt.Errorf("throttler error has happened %w", err))
			return
		}
		select {
		case <-r.ctx.Done():
			r.report(fmt.Errorf("context error has happened %w", r.ctx.Err()))
			return
		default:
		}
		if err := run(r.ctx); err != nil {
			r.report(fmt.Errorf("function error has happened %w", err))
			return
		}
	}()
}

func (r *rasync) Result() error {
	r.wg.Wait()
	r.report(nil)
	return r.err
}
