package gohalt

import (
	"context"
	"runtime"
	"time"

	"github.com/shirou/gopsutil/cpu"
)

type Monitor interface {
	Stats() Stats
}

type Stats struct {
	MemAlloc  uint64
	MemSystem uint64
	CpuPause  uint64
	CpuUsage  float64
}

type mcached struct {
	stats Stats
}

func NewCachedMonitor(ctx context.Context, duration time.Duration) (*mcached, error) {
	m := &mcached{}
	loop(ctx, duration, m.sync)
	return m, m.sync(ctx)
}

func (m mcached) Stats() Stats {
	return m.stats
}

func (m *mcached) sync(ctx context.Context) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}
	var memstats runtime.MemStats
	runtime.ReadMemStats(&memstats)
	m.stats.MemAlloc = memstats.Alloc
	m.stats.MemSystem = memstats.Sys
	for _, p := range memstats.PauseNs {
		m.stats.CpuPause += p
	}
	m.stats.CpuPause /= 256
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}
	if percents, err := cpu.Percent(10*time.Millisecond, true); err != nil {
		for _, p := range percents {
			m.stats.CpuUsage += p
		}
		m.stats.CpuUsage /= float64(len(percents))
	}
	return nil
}
