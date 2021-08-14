package gohalt

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestContext(t *testing.T) {
	cctx, cancel := context.WithCancel(context.Background())
	cancel()
	testerr := errors.New("test")
	ctx := WithParams(
		context.Background(),
		time.Now(),
		1,
		0,
		"",
		nil,
		nil,
	)
	table := map[string]struct {
		ctx context.Context
		err error
	}{
		"Context with throttler should be done on throttling": {
			ctx: WithThrottler(context.Background(), tmock{aerr: testerr}, ms1_0),
			err: testerr,
		},
		"Context with throttler should be done on throttling after": {
			ctx: WithThrottler(context.Background(), NewThrottlerAfter(1), ms1_0),
			err: ErrorThreshold{Throttler: "after", Threshold: strpair{current: 3, threshold: 1}},
		},
		"Context with throttler should be done with canceled context": {
			ctx: WithThrottler(cctx, tmock{}, ms1_0),
			err: cctx.Err(),
		},
		"Context with throttler should not be done after timeout": {
			ctx: WithThrottler(ctx, tmock{}, ms1_0),
		},
	}
	for tname, tcase := range table {
		t.Run(tname, func(t *testing.T) {
			tick := time.NewTicker(ms3_0)
			defer tick.Stop()
			select {
			case <-tcase.ctx.Done():
				err := tcase.ctx.Err()
				assert.Equal(t, tcase.err, err)
			case <-tick.C:
				assert.Nil(t, tcase.err)
			}
		})
	}
}
