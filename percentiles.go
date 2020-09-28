package gohalt

import (
	"math"
	"sort"
	"sync"
)

type percentiles struct {
	buf  []uint64
	cap  uint8
	lock sync.Mutex
}

func (p *percentiles) Len() int {
	p.lock.Lock()
	defer p.lock.Unlock()
	return len(p.buf)
}

func (p *percentiles) Push(dim uint64) {
	p.lock.Lock()
	defer p.lock.Unlock()
	if len(p.buf) >= int(p.cap) {
		p.buf = p.buf[1:]
	}
	p.buf = append(p.buf, dim)
}

func (p *percentiles) At(pval float64) uint64 {
	p.lock.Lock()
	defer p.lock.Unlock()
	buf := make([]uint64, len(p.buf))
	_ = copy(buf, p.buf)
	sort.Slice(buf, func(i, j int) bool {
		return buf[i] < buf[j]
	})
	at := int(math.Round(float64(len(buf)-1) * pval))
	return buf[at]
}

func (p *percentiles) Prune() {
	p.lock.Lock()
	defer p.lock.Unlock()
	p.buf = make([]uint64, 0, p.cap)
}
