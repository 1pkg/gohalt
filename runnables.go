package gohalt

import (
	"context"
	"sync"
	"time"
)

// DefaultRetriedDuration defines default initial duration for `square` throttler used by default.
// By default DefaultRetriedDuration is set to use `100 * time.Millisecond`.
var DefaultRetriedDuration = 100 * time.Millisecond

// Runnable defined by typical abstract async func signature.
// Runnable is used by `Runner` as a subject for execution.
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
		defer atomicBDecr(&lock)
		if atomicBIncr(&lock) > 1 {
			return nil
		}
		return run(ctx)
	}
}

func cached(cache time.Duration, run Runnable) Runnable {
	var lock uint64
	return func(ctx context.Context) error {
		ts := atomicGet(&lock)
		now := uint64(time.Now().UTC().Unix())
		// on first call run no matters what
		if ts == 0 {
			if err := run(ctx); err != nil {
				return err
			}
			atomicSet(&lock, now)
			return nil
		}
		// then use cached timestamp
		if cache > 0 && time.Duration(now-ts) > cache {
			if err := run(ctx); err != nil {
				return err
			}
			atomicSet(&lock, now)
			return nil
		}
		return nil
	}
}

func retried(retries uint64, run Runnable) Runnable {
	thr := NewThrottlerSquare(DefaultRetriedDuration, 0, false)
	return func(ctx context.Context) (err error) {
		// no need neither to check error
		// nor to call release counterpart
		for i := uint64(0); i < retries; i++ {
			_ = thr.Acquire(ctx)
			err = run(ctx)
			if err == nil {
				return
			}
		}
		return
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
