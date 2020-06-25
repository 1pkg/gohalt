package gohalt

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/prometheus/client_golang/api"
	prometheus "github.com/prometheus/client_golang/api/prometheus/v1"
	"github.com/prometheus/common/model"
)

type Metric interface {
	Query(context.Context) (bool, error)
}

type mprometheus struct {
	trypull Runnable
	value   bool
}

func NewMetricPrometheusCached(url string, query string, cache time.Duration, mstep time.Duration) *mprometheus {
	mtc := &mprometheus{}
	var lock sync.Mutex
	mtc.trypull = lazy(cache, func(ctx context.Context) error {
		lock.Lock()
		defer lock.Unlock()
		return mtc.pull(ctx, url, query, cache, mstep)
	})
	return mtc
}

func (mtc mprometheus) Query(ctx context.Context) (bool, error) {
	return mtc.value, nil
}

func (mtc *mprometheus) pull(
	ctx context.Context,
	url string,
	query string,
	cache time.Duration,
	mstep time.Duration,
) error {
	client, err := api.NewClient(
		api.Config{
			Address:      url,
			RoundTripper: api.DefaultRoundTripper,
		},
	)
	if err != nil {
		return err
	}
	api := prometheus.NewAPI(client)
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
