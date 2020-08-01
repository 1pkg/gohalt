package gohalt

import (
	"context"
	"sync"
	"time"
)

type Runnable func(context.Context) error

func nope(context.Context) error {
	return nil
}

func loop(period time.Duration, run Runnable) Runnable {
	return func(ctx context.Context) error {
		tick := time.NewTicker(period)
		defer tick.Stop()
		for {
			<-tick.C
			if err := run(ctx); err != nil {
				return err
			}
		}
	}
}

func delayed(after time.Duration, run Runnable) Runnable {
	return func(ctx context.Context) error {
		time.Sleep(after)
		return run(ctx)
	}
}

func cached(cache time.Duration, run Runnable) Runnable {
	var ts time.Time
	return func(ctx context.Context) error {
		now := time.Now().UTC()
		if now.Sub(ts) > cache {
			if err := run(ctx); err != nil {
				return err
			}
			ts = now
			return nil
		}
		return nil
	}
}

func once(run Runnable) Runnable {
	var once sync.Once
	return func(ctx context.Context) (err error) {
		once.Do(func() {
			err = run(ctx)
		})
		return err
	}
}

func gorun(ctx context.Context, r Runnable) {
	go func() {
		_ = r(ctx)
	}()
}
