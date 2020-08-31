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
	table := map[string]struct {
		r   Runner
		run Runnable
		err error
	}{
		"Runner sync should return error on throttling": {
			r:   NewRunnerSync(context.Background(), tmock{aerr: errors.New("test")}),
			run: nope,
			err: fmt.Errorf("throttler error has happened %w", errors.New("test")),
		},
		"Runner sync should return error on realising error": {
			r:   NewRunnerSync(context.Background(), tmock{rerr: errors.New("test")}),
			run: nope,
			err: fmt.Errorf("throttler error has happened %w", errors.New("test")),
		},
		"Runner sync should return error on runnable error": {
			r:   NewRunnerSync(context.Background(), tmock{}),
			run: use(errors.New("test")),
			err: fmt.Errorf("runnable error has happened %w", errors.New("test")),
		},
		"Runner sync should return error on canceled context": {
			r:   NewRunnerSync(cctx, tmock{}),
			run: nope,
			err: fmt.Errorf("context error has happened %w", cctx.Err()),
		},
		"Runner async should return error on throttling": {
			r:   NewRunnerAsync(context.Background(), tmock{aerr: errors.New("test")}),
			run: nope,
			err: fmt.Errorf("throttler error has happened %w", errors.New("test")),
		},
		"Runner async should return error on realising error": {
			r:   NewRunnerAsync(context.Background(), tmock{rerr: errors.New("test")}),
			run: nope,
			err: fmt.Errorf("throttler error has happened %w", errors.New("test")),
		},
		"Runner async should return error on runnable error": {
			r:   NewRunnerAsync(context.Background(), tmock{}),
			run: use(errors.New("test")),
			err: fmt.Errorf("runnable error has happened %w", errors.New("test")),
		},
		"Runner async should return error on canceled context": {
			r:   NewRunnerAsync(cctx, tmock{}),
			run: nope,
			err: fmt.Errorf("context error has happened %w", cctx.Err()),
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
