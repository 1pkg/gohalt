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

// mtcv defines inner runnable type that returns metric query result and possible error.
type mtcv func(context.Context) (bool, error)

type mtcprometheus struct {
	mtcv  mtcv
	value bool
}

// NewMetricPrometheus creates prometheus metric querier instance
// with cache interval defined by the provided duration
// which executes provided prometheus boolean metric query.
func NewMetricPrometheus(url string, query string, cache time.Duration) Metric {
	mtc := &mtcprometheus{}
	var api prometheus.API
	mempull, _ := cached(cache, func(ctx context.Context) (err error) {
		if api == nil {
			api, err = mtc.connect(ctx, url)
			if err != nil {
				return err
			}
		}
		return mtc.pull(ctx, api, query)
	})
	var lock sync.Mutex
	mtc.mtcv = func(ctx context.Context) (bool, error) {
		lock.Lock()
		defer lock.Unlock()
		if err := mempull(ctx); err != nil {
			return mtc.value, err
		}
		return mtc.value, nil
	}
	return mtc
}

func (mtc *mtcprometheus) Query(ctx context.Context) (bool, error) {
	return mtc.mtcv(ctx)
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

func (mtc *mtcprometheus) pull(ctx context.Context, api prometheus.API, query string) error {
	ts := time.Now().UTC()
	val, warns, err := api.Query(ctx, query, ts)
	if err != nil {
		return err
	}
	for _, warn := range warns {
		log("prometheus warning happened %s", warn)
	}
	vec, ok := val.(model.Vector)
	if !ok || vec.Len() != 1 {
		return fmt.Errorf("vector metric with single sample expected instead of %v", val)
	}
	value := vec[0].Value
	if value != 0 && value != 1 {
		return fmt.Errorf("boolean metric value expected instead of %v", value)
	}
	mtc.value = value == 1
	return nil
}

type mtcmock struct {
	metric bool
	err    error
}

func (mtc mtcmock) Query(context.Context) (bool, error) {
	return mtc.metric, mtc.err
}
