package gohalt

import (
	"context"
	"fmt"
	"sync"
)

// Runner defines abstraction to execute a set of `Runnable`
// and return possible execution error back.
// Runner is designed to simplify work with throttlers
// by managing `Acquire`/`Release` loop.
type Runner interface {
	// Run executes single prodived `Runnable` instance.
	Run(Runnable)
	// Result returns possible execution error back.
	Result() error
}

type rsync struct {
	thr    Throttler
	ctx    context.Context
	err    error
	report func(error)
}

// NewRunnerSync creates synchronous runner instance
// that runs a set of `Runnable` consecutively
// with regard to the provided context and throttler.
// First occurred error is returned from result.
func NewRunnerSync(ctx context.Context, thr Throttler) Runner {
	ctx, cancel := context.WithCancel(ctx)
	r := rsync{thr: thr, ctx: ctx}
	r.report = func(err error) {
		if err != nil {
			if r.err == nil {
				r.err = err
				cancel()
			}
			log("sync runner error happened %v", err)
		}
	}
	return &r
}

func (r *rsync) Run(run Runnable) {
	select {
	case <-r.ctx.Done():
		r.report(fmt.Errorf("context error has happened %w", r.ctx.Err()))
		return
	default:
	}
	defer func() {
		if err := r.thr.Release(r.ctx); err != nil {
			r.report(fmt.Errorf("throttler error has happened %w", err))
		}
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
		r.report(fmt.Errorf("runnable error has happened %w", err))
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

// NewRunnerAsync creates asynchronous runner instance
// that runs a set of `Runnable` simultaneously
// with regard to the provided context and throttler.
// First occurred error is returned from result.
func NewRunnerAsync(ctx context.Context, thr Throttler) Runner {
	ctx, cancel := context.WithCancel(ctx)
	r := rasync{thr: thr, ctx: ctx}
	var once sync.Once
	r.report = func(err error) {
		if err != nil {
			once.Do(func() {
				r.err = err
				cancel()
			})
			log("async runner error happened %v", err)
		}
	}
	return &r
}

func (r *rasync) Run(run Runnable) {
	r.wg.Add(1)
	go func() {
		defer r.wg.Done()
		select {
		case <-r.ctx.Done():
			r.report(fmt.Errorf("context error has happened %w", r.ctx.Err()))
			return
		default:
		}
		defer func() {
			if err := r.thr.Release(r.ctx); err != nil {
				r.report(fmt.Errorf("throttler error has happened %w", err))
			}
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
			r.report(fmt.Errorf("runnable error has happened %w", err))
			return
		}
	}()
}

func (r *rasync) Result() error {
	r.wg.Wait()
	return r.err
}
