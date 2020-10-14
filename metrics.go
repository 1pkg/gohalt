package gohalt

import (
	"context"
	"fmt"
	"sync"
	"time"

	client "github.com/prometheus/client_golang/api"
	prometheus "github.com/prometheus/client_golang/api/prometheus/v1"
	"github.com/prometheus/common/model"
)

// Metric defines single metric querier interface that returns the metric query result.
type Metric interface {
	// Query returns the result of the query or internal error if any happened.
	Query(context.Context) (bool, error)
}

type mtcprometheus struct {
	lock    sync.Mutex
	mempull Runnable
	value   bool
}

// NewMetricPrometheus creates prometheus metric querier instance
// with cache interval defined by the provided duration
// which executes provided prometheus metric query.
func NewMetricPrometheus(url string, query string, cache time.Duration, mstep time.Duration) Metric {
	mtc := &mtcprometheus{}
	var api prometheus.API
	mtc.mempull = cached(cache, func(ctx context.Context) (err error) {
		if api != nil {
			return mtc.pull(ctx, api, cache, mstep, query)
		}
		api, err = mtc.connect(ctx, url)
		if err != nil {
			return err
		}
		return mtc.pull(ctx, api, cache, mstep, query)
	})
	return mtc
}

func (mtc *mtcprometheus) Query(ctx context.Context) (bool, error) {
	mtc.lock.Lock()
	defer mtc.lock.Unlock()
	if err := mtc.mempull(ctx); err != nil {
		return mtc.value, err
	}
	return mtc.value, nil
}

func (mtc *mtcprometheus) connect(_ context.Context, url string) (prometheus.API, error) {
	client, err := client.NewClient(
		client.Config{
			Address:      url,
			RoundTripper: client.DefaultRoundTripper,
		},
	)
	if err != nil {
		return nil, err
	}
	return prometheus.NewAPI(client), nil
}

func (mtc *mtcprometheus) pull(
	ctx context.Context,
	api prometheus.API,
	cache time.Duration,
	mstep time.Duration,
	query string,
) error {
	timestamp := time.Now().UTC()
	val, _, err := api.QueryRange(ctx, query, prometheus.Range{
		Start: timestamp,
		End:   timestamp.Add(cache),
		Step:  mstep,
	})
	scalar, ok := val.(*model.Scalar)
	if !ok || (scalar.Value != 0 && scalar.Value != 1) {
		return fmt.Errorf("boolean metric value expected instead of %v", val)
	}
	mtc.value = scalar.Value == 1
	return err
}

type mtcmock struct {
	metric bool
	err    error
}

func (mtc mtcmock) Query(context.Context) (bool, error) {
	return mtc.metric, mtc.err
}
