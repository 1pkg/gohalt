package gohalt

import (
	"context"
	"time"
)

func loop(ctx context.Context, period time.Duration, run Runnable) {
	if period == 0 {
		return
	}
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
}

func once(ctx context.Context, after time.Duration, run Runnable) {
	if after == 0 {
		run(ctx)
		return
	}
	go func() {
		tick := time.NewTicker(after)
		defer tick.Stop()
		<-tick.C
		run(ctx)
	}()
}

func lazy(cache time.Duration, run Runnable) Runnable {
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
