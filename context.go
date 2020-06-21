package gohalt

import (
	"context"
	"time"
)

const (
	gohaltctxkey       = "gohalt_context_key"
	gohaltctxtimestamp = "gohalt_context_timestampy"
	gohaltctxpriority  = "gohalt_context_priority"
)

type ctxerr struct {
	ctx context.Context
	err error
}

type multi []context.Context

func (m multi) Deadline() (deadline time.Time, ok bool) {
	for _, ctx := range m {
		if deadline, ok = ctx.Deadline(); ok {
			return
		}
	}
	return
}

func (m multi) Done() <-chan struct{} {
	for _, ctx := range m {
		done := ctx.Done()
		select {
		case <-done:
			return done
		default:
		}
	}
	// for default case
	// we need wait for any chan
	done := make(chan struct{})
	for _, ctx := range m {
		go func(ctx context.Context) {
			select {
			case <-ctx.Done():
				close(done)
			case <-done:
			}
		}(ctx)
	}
	return done
}

func (m multi) Err() error {
	for _, ctx := range m {
		if err := ctx.Err(); err != nil {
			return err
		}
	}
	return nil
}

func (m multi) Value(key interface{}) interface{} {
	for _, ctx := range m {
		if val := ctx.Value(key); val != nil {
			return val
		}
	}
	return nil
}

func WithKey(ctx context.Context, key interface{}) context.Context {
	return context.WithValue(ctx, gohaltctxkey, key)
}

func ctxKey(ctx context.Context) interface{} {
	return ctx.Value(gohaltctxkey)
}

func withTimestamp(ctx context.Context) context.Context {
	return context.WithValue(ctx, gohaltctxtimestamp, time.Now().UTC().UnixNano())
}

func ctxTimestamp(ctx context.Context) int64 {
	if val := ctx.Value(gohaltctxtimestamp); val != nil {
		if timestamp, ok := val.(int64); ok {
			return timestamp
		}
	}
	return time.Now().UTC().UnixNano()
}

func WithPriority(ctx context.Context, priority uint8) context.Context {
	if priority == 0 {
		priority = 1
	}
	return context.WithValue(ctx, gohaltctxpriority, priority)
}

func ctxPriority(ctx context.Context, max uint8) uint8 {
	if val := ctx.Value(gohaltctxpriority); val != nil {
		if priority, ok := val.(uint8); ok && priority > 0 && priority <= max {
			return priority
		}
	}
	return 1
}
