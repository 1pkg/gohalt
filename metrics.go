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

type Metric interface {
	Query(context.Context) (bool, error)
}

type mprometheus struct {
	mempull Runnable
	value   bool
}

func NewMetricPrometheusCached(url string, query string, cache time.Duration, mstep time.Duration) *mprometheus {
	mtc := &mprometheus{}
	var lock sync.Mutex
	var api prometheus.API
	mtc.mempull = cached(cache, func(ctx context.Context) error {
		lock.Lock()
		defer lock.Unlock()
		if api == nil {
			client, err := client.NewClient(
				client.Config{
					Address:      url,
					RoundTripper: client.DefaultRoundTripper,
				},
			)
			if err != nil {
				return err
			}
			api = prometheus.NewAPI(client)
		}
		return mtc.pull(ctx, api, cache, mstep, query)
	})
	return mtc
}

func (mtc mprometheus) Query(ctx context.Context) (bool, error) {
	if err := mtc.mempull(ctx); err != nil {
		return mtc.value, err
	}
	return mtc.value, nil
}

func (mtc *mprometheus) pull(
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
