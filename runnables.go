package gohalt

import (
	"context"
	"sync"
	"sync/atomic"
	"time"
)

type Runnable func(context.Context) error

func use(err error) Runnable {
	return func(ctx context.Context) error {
		return err
	}
}

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

func locked(run Runnable) Runnable {
	var lock uint64
	return func(ctx context.Context) error {
		defer atomic.AddUint64(&lock, ^uint64(0))
		if atomic.AddUint64(&lock, 1) > 1 {
			return nil
		}
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

func all(runs ...Runnable) Runnable {
	return func(ctx context.Context) error {
		var once sync.Once
		var wg sync.WaitGroup
		var result error
		for _, run := range runs {
			wg.Add(1)
			go func(run Runnable) {
				if err := run(ctx); err != nil {
					once.Do(func() {
						result = err
					})
				}
				wg.Done()
			}(run)
		}
		wg.Wait()
		return result
	}
}

func gorun(ctx context.Context, r Runnable) {
	go func() {
		_ = r(ctx)
	}()
}
