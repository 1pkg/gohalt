package gohalt

import (
	"context"
	"time"
)

type ghctxid string

const (
	ghctxpriority  ghctxid = "gohalt_context_priority"
	ghctxkey       ghctxid = "gohalt_context_key"
	ghctxdata      ghctxid = "gohalt_context_data"
	ghctxtimestamp ghctxid = "gohalt_context_timestamp"
	ghctxmarshaler ghctxid = "gohalt_context_marshaler"
)

func WithTimestamp(ctx context.Context, ts time.Time) context.Context {
	return context.WithValue(ctx, ghctxtimestamp, ts)
}

func ctxTimestamp(ctx context.Context) time.Time {
	if val := ctx.Value(ghctxtimestamp); val != nil {
		if timestamp, ok := val.(time.Time); ok {
			return timestamp.UTC()
		}
	}
	return time.Now().UTC()
}

func WithPriority(ctx context.Context, priority uint8) context.Context {
	return context.WithValue(ctx, ghctxpriority, priority)
}

func ctxPriority(ctx context.Context, limit uint8) uint8 {
	if val := ctx.Value(ghctxpriority); val != nil {
		if priority, ok := val.(uint8); ok && priority > 0 && priority <= limit {
			return priority
		}
	}
	return 1
}

func WithKey(ctx context.Context, key string) context.Context {
	return context.WithValue(ctx, ghctxkey, key)
}

func ctxKey(ctx context.Context) string {
	if val, ok := ctx.Value(ghctxkey).(string); ok {
		return val
	}
	return ""
}

func WithData(ctx context.Context, data interface{}) context.Context {
	return context.WithValue(ctx, ghctxdata, data)
}

func ctxData(ctx context.Context) interface{} {
	return ctx.Value(ghctxdata)
}

func WithMarshaler(ctx context.Context, mrsh Marshaler) context.Context {
	return context.WithValue(ctx, ghctxmarshaler, mrsh)
}

func ctxMarshaler(ctx context.Context) Marshaler {
	if val := ctx.Value(ghctxmarshaler); val != nil {
		if marshaler, ok := val.(Marshaler); ok {
			return marshaler
		}
	}
	return DefaultMarshaler
}

func WithParams(
	ctx context.Context,
	ts time.Time,
	priority uint8,
	key string,
	data interface{},
	marshaler Marshaler,
) context.Context {
	ctx = WithTimestamp(ctx, ts)
	ctx = WithPriority(ctx, priority)
	ctx = WithKey(ctx, key)
	ctx = WithData(ctx, data)
	ctx = WithMarshaler(ctx, marshaler)
	return ctx
}

type ctxthr struct {
	context.Context
	thr  Throttler
	freq time.Duration
}

func WithThrottler(ctx context.Context, thr Throttler, freq time.Duration) context.Context {
	return ctxthr{Context: ctx, thr: thr, freq: freq}
}

func (ctx ctxthr) Done() <-chan struct{} {
	ch := make(chan struct{})
	// proactively test throttler once
	if err := ctx.Err(); err != nil {
		close(ch)
		return ch
	}
	// run long throttler error pooling
	gorun(ctx, loop(ctx.freq, func(ctx context.Context) error {
		err := ctx.Err()
		if err != nil {
			close(ch)
		}
		return err
	}))
	return ch
}

func (ctx ctxthr) Err() (err error) {
	r := NewRunnerSync(ctx.Context, ctx.thr)
	r.Run(nope)
	return r.Result()
}
