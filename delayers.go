package gohalt

import (
	"context"
	"time"
)

type delayer func(context.Context, time.Duration)

func sleep(_ context.Context, dur time.Duration) {
	time.Sleep(dur)
}

func vigil(context.Context, time.Duration) {
}
