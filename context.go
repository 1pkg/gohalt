package gohalt

import (
	"context"
	"time"
)

type ghctxid string

const (
	ghctxpriority  ghctxid = "gohalt_context_priority"
	ghctxkey       ghctxid = "gohalt_context_key"
	ghctxmessage   ghctxid = "gohalt_context_message"
	ghctxtimestamp ghctxid = "gohalt_context_timestamp"
	ghctxmarshaler ghctxid = "gohalt_context_marshaler"
)

// WithTimestamp adds the provided timestamp to the provided context
// to determine latency between `Acquire` and `Release`.
// Resulted context is used by: `latency` and `percentile` throtttlers.
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

// WithPriority adds the provided priority to the provided context
// to differ `Acquire` priority levels.
// Resulted context is used by: `priority` throtttler.
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

// WithKey adds the provided key to the provided context
// to add additional call identifier to context.
// Resulted context is used by: `pattern` throtttler.
func WithKey(ctx context.Context, key string) context.Context {
	return context.WithValue(ctx, ghctxkey, key)
}

func ctxKey(ctx context.Context) string {
	if val, ok := ctx.Value(ghctxkey).(string); ok {
		return val
	}
	return ""
}

// WithMessage adds the provided message to the provided context
// to add additional message that need to be used to context.
// Resulted context is used by: `enqueue` throtttler.
// Used in pair with `WithMarshaler`.
func WithMessage(ctx context.Context, message interface{}) context.Context {
	return context.WithValue(ctx, ghctxmessage, message)
}

func ctxMessage(ctx context.Context) interface{} {
	return ctx.Value(ghctxmessage)
}

// WithMarshaler adds the provided marshaler to the provided context
// to add additional message marshaler that need to be used to context.
// Resulted context is used by: `enqueue` throtttler.
// Used in pair with `WithMessage`.
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

// WithParams facade call that respectively calls:
// - `WithTimestamp`
// - `WithPriority`
// - `WithKey`
// - `WithMessage`
// - `WithMarshaler`
func WithParams(
	ctx context.Context,
	ts time.Time,
	priority uint8,
	key string,
	message interface{},
	marshaler Marshaler,
) context.Context {
	ctx = WithTimestamp(ctx, ts)
	ctx = WithPriority(ctx, priority)
	ctx = WithKey(ctx, key)
	ctx = WithMessage(ctx, message)
	ctx = WithMarshaler(ctx, marshaler)
	return ctx
}

type ctxthr struct {
	context.Context
	thr  Throttler
	freq time.Duration
}

// WithThrottler adds the provided thr to the provided context
// and defines context implementation that uses parrent context plus throttler internally
// that closes context done chanel if internal throttler throttles.
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
			log("context is canceled due %v", err)
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

func (ctx ctxthr) Throttler() Throttler {
	return ctx.thr
}
