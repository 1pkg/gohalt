package gohalt

import (
	"context"
	"time"
)

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
