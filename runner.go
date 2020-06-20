package gohalt

import (
	"context"
)

type Runnable func(context.Context) error
type Logger func(string, ...interface{})

func discard(string, ...interface{}) {}

type Runner struct {
	t Throttler
	l Logger
}

func NewRunner(t Throttler) Runner {
	return Runner{t: t, l: discard}
}

func NewRunnerWithLogger(t Throttler, l Logger) Runner {
	return Runner{t: t, l: l}
}

func (r Runner) Go(ctx context.Context, run Runnable) (gerr error) {
	if err := r.t.Acquire(ctx); err != nil {
		r.l("throttler error happened: %w", err)
		return err
	}
	defer func() {
		if err := r.t.Release(ctx); err != nil {
			r.l("throttler error happened: %w", err)
			if gerr == nil {
				gerr = err
			}
		}
	}()
	if err := run(ctx); err != nil {
		r.l("run error happened: %w", err)
		return err
	}
	return nil
}
