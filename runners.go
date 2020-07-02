package gohalt

import (
	"context"
	"fmt"
	"sync"
)

type Runner interface {
	Run(context.Context, Runnable)
	Result() error
}

type rsync struct {
	thr    Throttler
	err    error
	report func(error)
}

func NewRunnerSync(ctx context.Context, thr Throttler) (*rsync, context.Context) {
	ctx, cancel := context.WithCancel(ctx)
	r := rsync{thr: thr}
	r.report = func(err error) {
		if r.err != nil {
			r.err = err
			cancel()
		}
	}
	return &r, ctx
}

func (r *rsync) Run(ctx context.Context, run Runnable) {
	defer func() {
		if err := r.thr.Release(ctx); err != nil {
			r.report(fmt.Errorf("throttler error has happened %w", err))
		}
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
}

func (r *rsync) Result() error {
	return r.err
}

type rasync struct {
	thr    Throttler
	wg     sync.WaitGroup
	err    error
	report func(error)
}

func NewRunnerAsync(ctx context.Context, thr Throttler) (*rasync, context.Context) {
	ctx, cancel := context.WithCancel(ctx)
	r := rasync{thr: thr}
	var once sync.Once
	r.report = func(err error) {
		once.Do(func() {
			r.err = err
			cancel()
		})
	}
	return &r, ctx
}

func (r *rasync) Run(ctx context.Context, run Runnable) {
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

func (r *rasync) Result() error {
	r.wg.Wait()
	r.report(nil)
	return r.err
}

func RunWithParams(
	r Runner,
	run Runnable,
	priority uint8,
	key interface{},
	data interface{},
	marshaler Marshaler,
) error {
	ctx := context.Background()
	ctx = WithPriority(ctx, priority)
	ctx = WithKey(ctx, key)
	ctx = WithData(ctx, data)
	ctx = WithTimestamp(ctx)
	ctx = WithMarshaler(ctx, marshaler)
	r.Run(ctx, run)
	return r.Result()
}
