package gohalt

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestRunners(t *testing.T) {
	cctx, cancel := context.WithCancel(context.Background())
	cancel()
	testerr := errors.New("test")
	table := map[string]struct {
		r   Runner
		run Runnable
		err error
	}{
		"Runner sync should return error on throttling": {
			r:   NewRunnerSync(context.Background(), tmock{aerr: testerr}),
			run: nope,
			err: fmt.Errorf("throttler error has happened %w", testerr),
		},
		"Runner sync should return error on realising error": {
			r:   NewRunnerSync(context.Background(), tmock{rerr: testerr}),
			run: nope,
			err: fmt.Errorf("throttler error has happened %w", testerr),
		},
		"Runner sync should return error on runnable error": {
			r:   NewRunnerSync(context.Background(), tmock{}),
			run: use(testerr),
			err: fmt.Errorf("runnable error has happened %w", testerr),
		},
		"Runner sync should return error on canceled context": {
			r:   NewRunnerSync(cctx, tmock{}),
			run: nope,
			err: fmt.Errorf("context error has happened %w", cctx.Err()),
		},
		"Runner async should return error on throttling": {
			r:   NewRunnerAsync(context.Background(), tmock{aerr: testerr}),
			run: nope,
			err: fmt.Errorf("throttler error has happened %w", testerr),
		},
		"Runner async should return error on realising error": {
			r:   NewRunnerAsync(context.Background(), tmock{rerr: testerr}),
			run: nope,
			err: fmt.Errorf("throttler error has happened %w", testerr),
		},
		"Runner async should return error on runnable error": {
			r:   NewRunnerAsync(context.Background(), tmock{}),
			run: use(testerr),
			err: fmt.Errorf("runnable error has happened %w", testerr),
		},
		"Runner async should return error on canceled context": {
			r:   NewRunnerAsync(cctx, tmock{}),
			run: nope,
			err: fmt.Errorf("context error happened %w", cctx.Err()),
		},
	}
	for tname, tcase := range table {
		t.Run(tname, func(t *testing.T) {
			tcase.r.Run(tcase.run)
			err := tcase.r.Result()
			assert.Equal(t, tcase.err, err)
		})
	}
}
