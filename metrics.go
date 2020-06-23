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
	url   string
	query string
	exp   time.Duration
	step  time.Duration

	api prometheus.API
	val bool
	mut sync.Mutex
}

func NewMetricPrometheusCached(
	ctx context.Context,
	url string,
	query string,
	exp time.Duration,
	step time.Duration,
) (*mprometheus, error) {
	client, err := api.NewClient(api.Config{
		Address:      url,
		RoundTripper: api.DefaultRoundTripper,
	})
	if err != nil {
		return nil, err
	}
	m := mprometheus{
		url:   url,
		query: query,
		exp:   exp,
		step:  step,
		api:   prometheus.NewAPI(client),
	}
	loop(ctx, exp, func(ctx context.Context) error {
		if err := m.pull(ctx); err != nil {
			return err
		}
		return ctx.Err()
	})
	return &m, m.pull(ctx)
}

func (m *mprometheus) Query(ctx context.Context) (bool, error) {
	m.mut.Lock()
	defer m.mut.Unlock()
	return m.val, nil
}

func (m *mprometheus) pull(ctx context.Context) error {
	timestamp := time.Now().UTC()
	val, _, err := m.api.QueryRange(ctx, m.query, prometheus.Range{
		Start: timestamp,
		End:   timestamp.Add(m.exp),
		Step:  m.step,
	})
	scalar, ok := val.(*model.Scalar)
	if !ok || (scalar.Value != 0 && scalar.Value != 1) {
		return fmt.Errorf("boolean metric value expected instead of %v", val)
	}
	m.mut.Lock()
	m.val = scalar.Value == 1
	m.mut.Unlock()
	return err
}
