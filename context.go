package gohalt

import (
	"context"
	"time"
)

const (
	gohaltctxpriority  = "gohalt_context_priority"
	gohaltctxkey       = "gohalt_context_key"
	gohaltctxdata      = "gohalt_context_data"
	gohaltctxtimestamp = "gohalt_context_timestamp"
	gohaltctxmarshaler = "gohalt_context_marshaler"
)

func WithPriority(ctx context.Context, priority uint8) context.Context {
	if priority == 0 {
		priority = 1
	}
	return context.WithValue(ctx, gohaltctxpriority, priority)
}

func ctxPriority(ctx context.Context, limit uint8) uint8 {
	if val := ctx.Value(gohaltctxpriority); val != nil {
		if priority, ok := val.(uint8); ok && priority > 0 && priority <= limit {
			return priority
		}
	}
	return 1
}

func WithKey(ctx context.Context, key interface{}) context.Context {
	return context.WithValue(ctx, gohaltctxkey, key)
}

func ctxKey(ctx context.Context) interface{} {
	return ctx.Value(gohaltctxkey)
}

func WithData(ctx context.Context, data interface{}) context.Context {
	return context.WithValue(ctx, gohaltctxdata, data)
}

func ctxData(ctx context.Context) interface{} {
	return ctx.Value(gohaltctxdata)
}

func WithTimestamp(ctx context.Context) context.Context {
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

func WithMarshaler(ctx context.Context, mrsh Marshaler) context.Context {
	return context.WithValue(ctx, gohaltctxmarshaler, mrsh)
}

func ctxMarshaler(ctx context.Context) Marshaler {
	if val := ctx.Value(gohaltctxmarshaler); val != nil {
		if marshaler, ok := val.(Marshaler); ok {
			return marshaler
		}
	}
	return DefaultMarshaler
}
