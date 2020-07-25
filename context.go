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

func WithParams(
	ctx context.Context,
	priority uint8,
	key interface{},
	data interface{},
	marshaler Marshaler,
) context.Context {
	ctx = WithPriority(ctx, priority)
	ctx = WithKey(ctx, key)
	ctx = WithData(ctx, data)
	ctx = WithTimestamp(ctx)
	ctx = WithMarshaler(ctx, marshaler)
	return ctx
}

func WithPriority(ctx context.Context, priority uint8) context.Context {
	if priority == 0 {
		priority = 1
	}
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

func WithKey(ctx context.Context, key interface{}) context.Context {
	return context.WithValue(ctx, ghctxkey, key)
}

func ctxKey(ctx context.Context) interface{} {
	return ctx.Value(ghctxkey)
}

func WithData(ctx context.Context, data interface{}) context.Context {
	return context.WithValue(ctx, ghctxdata, data)
}

func ctxData(ctx context.Context) interface{} {
	return ctx.Value(ghctxdata)
}

func WithTimestamp(ctx context.Context) context.Context {
	return context.WithValue(ctx, ghctxtimestamp, time.Now().UTC().UnixNano())
}

func ctxTimestamp(ctx context.Context) int64 {
	if val := ctx.Value(ghctxtimestamp); val != nil {
		if timestamp, ok := val.(int64); ok {
			return timestamp
		}
	}
	return time.Now().UTC().UnixNano()
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
	if err := ctx.Err(); err != nil {
		close(ch)
		return ch
	}
	_ = loop(ctx.freq, func(ctx context.Context) error {
		err := ctx.Err()
		if err != nil {
			close(ch)
		}
		return err
	})(ctx)
	return ch
}

func (ctx ctxthr) Err() (err error) {
	r := NewRunnerSync(ctx, ctx.thr)
	r.Run(nope)
	return r.Result()
}
