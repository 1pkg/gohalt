package gohalt

import (
	"runtime"
	"time"
)

type Stats interface {
	MEM() (alloc uint64, system uint64)
	CPU() (avgpause uint64, usage float64)
}

type cachedstats struct {
	alloc    uint64
	system   uint64
	avgpause uint64
	usage    float64
}

func NewCachedStats(d time.Duration) *cachedstats {
	s := &cachedstats{}
	s.refresh()
	go func() {
		tick := time.NewTicker(d)
		defer tick.Stop()
		for {
			<-tick.C
			s.refresh()
		}
	}()
	return s
}

func (s cachedstats) MEM() (alloc uint64, system uint64) {
	return s.alloc, s.system
}

func (s cachedstats) CPU() (avgpause uint64, usage float64) {
	return s.avgpause, s.usage
}

func (s *cachedstats) refresh() {
	var stats runtime.MemStats
	runtime.ReadMemStats(&stats)
	for _, p := range stats.PauseNs {
		s.avgpause += p
	}
	s.avgpause /= 256
	s.alloc = stats.Alloc
	s.system = stats.Sys
}
