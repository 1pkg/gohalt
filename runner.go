package gohalt

import (
	"context"
	"fmt"
	"sync"
	"time"
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

func (r *Runner) Go(run Runnable, key interface{}) {
	r.wg.Add(1)
	go func() {
		ctx := KeyedContext(r.ctx, key)
		defer func() {
			if _, err := r.thr.Release(ctx); err != nil {
				r.report(fmt.Errorf("throttler error happened: %w", err))
			}
			r.wg.Done()
			return
		}()
		if thrctx, err := r.thr.Acquire(ctx); err != nil {
			r.report(fmt.Errorf("throttler error happened: %w", err))
			return
		} else {
			ctx = thrctx
		}
		select {
		case <-ctx.Done():
			r.report(fmt.Errorf("context error happened: %w", ctx.Err()))
			return
		default:
		}
		if err := run(ctx); err != nil {
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

func loop(ctx context.Context, duration time.Duration, run Runnable) {
	if duration > 0 {
		go func() {
			tick := time.NewTicker(duration)
			defer tick.Stop()
			for {
				<-tick.C
				if err := run(ctx); err != nil {
					return
				}
			}
		}()
	}
}

func once(ctx context.Context, duration time.Duration, run Runnable) {
	if duration > 0 {
		go func() {
			tick := time.NewTicker(duration)
			defer tick.Stop()
			<-tick.C
			run(ctx)
		}()
	}
}
