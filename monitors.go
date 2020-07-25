package gohalt

import (
	"context"
	"runtime"
	"sync"
	"time"

	"github.com/shirou/gopsutil/cpu"
)

type Monitor interface {
	Stats(context.Context) (Stats, error)
}

type Stats struct {
	MEMAlloc  uint64
	MEMSystem uint64
	CPUPause  uint64
	CPUUsage  float64
}

type monitor struct {
	memsync Runnable
	stats   Stats
}

func NewMonitor(cache time.Duration) *monitor {
	mnt := &monitor{}
	var lock sync.Mutex
	mnt.memsync = cached(cache, func(ctx context.Context) error {
		lock.Lock()
		defer lock.Unlock()
		return mnt.sync(ctx)
	})
	return mnt
}

func (mnt *monitor) Stats(ctx context.Context) (Stats, error) {
	if err := mnt.memsync(ctx); err != nil {
		return mnt.stats, err
	}
	return mnt.stats, nil
}

func (mnt *monitor) sync(context.Context) error {
	var memstats runtime.MemStats
	runtime.ReadMemStats(&memstats)
	mnt.stats.MEMAlloc = memstats.Alloc
	mnt.stats.MEMSystem = memstats.Sys
	for _, p := range memstats.PauseNs {
		mnt.stats.CPUPause += p
	}
	mnt.stats.CPUPause /= 256
	if percents, err := cpu.Percent(10*time.Millisecond, true); err != nil {
		for _, p := range percents {
			mnt.stats.CPUUsage += p
		}
		mnt.stats.CPUUsage /= float64(len(percents))
	}
	return nil
}
