package gohalt

import (
	"context"
	"runtime"
	"sync"
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

type monitor struct {
	trysync Runnable
	stats   Stats
}

func NewMonitor(cache time.Duration) *monitor {
	mnt := &monitor{}
	var lock sync.Mutex
	mnt.trysync = lazy(cache, func(ctx context.Context) error {
		lock.Lock()
		defer lock.Unlock()
		return mnt.sync(ctx)
	})
	return mnt
}

func (mnt monitor) Stats() Stats {
	return mnt.stats
}

func (mnt *monitor) sync(ctx context.Context) error {
	var memstats runtime.MemStats
	runtime.ReadMemStats(&memstats)
	mnt.stats.MemAlloc = memstats.Alloc
	mnt.stats.MemSystem = memstats.Sys
	for _, p := range memstats.PauseNs {
		mnt.stats.CpuPause += p
	}
	mnt.stats.CpuPause /= 256
	if percents, err := cpu.Percent(10*time.Millisecond, true); err != nil {
		for _, p := range percents {
			mnt.stats.CpuUsage += p
		}
		mnt.stats.CpuUsage /= float64(len(percents))
	}
	return nil
}
