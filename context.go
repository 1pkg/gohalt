package gohalt

import (
	"context"
	"time"
)

const (
	gohaltctxpriority  = "gohalt_context_priority"
	gohaltctxkey       = "gohalt_context_key"
	gohaltctxdata      = "gohalt_context_data"
	gohaltctxtimestamp = "gohalt_context_timestampy"
)

func WithThrottling(ctx context.Context, priority uint8, key interface{}, data interface{}) context.Context {
	ctx = WithPriority(ctx, priority)
	ctx = WithKey(ctx, key)
	ctx = WithData(ctx, data)
	ctx = WithTimestamp(ctx)
	return ctx
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
