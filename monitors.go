package gohalt

import (
	"context"
	"runtime"
	"sync"
	"time"

	"github.com/shirou/gopsutil/cpu"
)

// Stats defines typical set of metrics returned by system monitor:
// - MEMAlloc shows how many bytes are allocated by heap objects.
// - MEMSystem shows how many bytes are obtained from the OS.
// - CPUPause shows average GC stop-the-world pause in nanoseconds.
// - CPUUsage shows average CPU utilization in percents.
type Stats struct {
	MEMAlloc  uint64
	MEMSystem uint64
	CPUPause  uint64
	CPUUsage  float64
}

// Monitor defines system monitor interface that returns the system stats.
type Monitor interface {
	// Stats returns system stats or internal error if any happened.
	Stats(context.Context) (Stats, error)
}

// mnts defines inner runnable type that returns stats and possible error.
type mnts func(context.Context) (Stats, error)

type mntsys struct {
	mnts  mnts
	stats Stats
}

// NewMonitorSystem creates system monitor instance
// with cache interval defined by the provided duration
// and time to process CPU utilization.
func NewMonitorSystem(cache time.Duration, tp time.Duration) Monitor {
	mnt := &mntsys{}
	memsync, _ := cached(cache, func(ctx context.Context) error {
		return mnt.sync(ctx, tp)
	})
	var lock sync.Mutex
	mnt.mnts = func(ctx context.Context) (Stats, error) {
		lock.Lock()
		defer lock.Unlock()
		if err := memsync(ctx); err != nil {
			return mnt.stats, err
		}
		return mnt.stats, nil
	}
	return mnt
}

func (mnt *mntsys) Stats(ctx context.Context) (Stats, error) {
	return mnt.mnts(ctx)
}

func (mnt *mntsys) sync(_ context.Context, tp time.Duration) error {
	var memstats runtime.MemStats
	runtime.ReadMemStats(&memstats)
	mnt.stats.MEMAlloc = memstats.Alloc
	mnt.stats.MEMSystem = memstats.Sys
	for _, p := range memstats.PauseNs {
		mnt.stats.CPUPause += p
	}
	mnt.stats.CPUPause /= 256
	if percents, err := cpu.Percent(tp, true); err == nil {
		for _, p := range percents {
			mnt.stats.CPUUsage += p
		}
		mnt.stats.CPUUsage /= float64(len(percents))
	}
	return nil
}

type mntmock struct {
	stats Stats
	err   error
}

func (mnt mntmock) Stats(context.Context) (Stats, error) {
	return mnt.stats, mnt.err
}
