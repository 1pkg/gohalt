package gohalt

import (
	"context"
	"time"
)

type Runnable func(context.Context) error

func nope(context.Context) error {
	return nil
}

func loop(period time.Duration, run Runnable) Runnable {
	if period == 0 {
		return nope
	}
	return func(ctx context.Context) error {
		go func() {
			tick := time.NewTicker(period)
			defer tick.Stop()
			for {
				<-tick.C
				if err := run(ctx); err != nil {
					return
				}
			}
		}()
		return nil
	}
}

func once(after time.Duration, run Runnable) Runnable {
	if after == 0 {
		return func(ctx context.Context) error {
			return run(ctx)
		}
	}
	return func(ctx context.Context) error {
		go func() {
			tick := time.NewTicker(after)
			defer tick.Stop()
			<-tick.C
			_ = run(ctx)
		}()
		return nil
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
