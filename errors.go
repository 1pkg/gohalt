package gohalt

import (
	"fmt"
	"time"
)

type strbool bool

func (b strbool) String() string {
	return fmt.Sprintf("%t", bool(b))
}

type strpair struct {
	current   uint64
	threshold uint64
}

func (p strpair) String() string {
	return fmt.Sprintf("%d out of %d", p.current, p.threshold)
}

type strpercent float64

func (p strpercent) String() string {
	return fmt.Sprintf("%.4f%%", float64(p)*100)
}

type strdurations struct {
	current   time.Duration
	threshold time.Duration
}

func (d strdurations) String() string {
	return fmt.Sprintf("%s out of %s", d.current, d.threshold)
}

type strstats struct {
	current   Stats
	threshold Stats
}

func (s strstats) String() string {
	return fmt.Sprintf(
		`
			%d out of %d bytes
			%d out of %d bytes
			%d out of %d ns
			%.4f out of %.4f %%
		`,
		s.current.MEMAlloc,
		s.threshold.MEMAlloc,
		s.current.MEMSystem,
		s.threshold.MEMSystem,
		s.current.CPUPause,
		s.threshold.CPUPause,
		s.current.CPUUsage*100,
		s.threshold.CPUUsage*100,
	)
}

// ErrorThreshold defines error type
// that occurs if throttler reaches specified threshold.
type ErrorThreshold struct {
	Throttler string
	Threshold fmt.Stringer
}

func (err ErrorThreshold) Error() string {
	return fmt.Sprintf(
		"throttler %q has reached its threshold: %s",
		err.Throttler,
		err.Threshold,
	)
}

// ErrorInternal defines error type
// that occurs if throttler internal error happens.
type ErrorInternal struct {
	Throttler string
	Message   string
}

func (err ErrorInternal) Error() string {
	return fmt.Sprintf(
		"throttler %q internal error happened: %s",
		err.Throttler,
		err.Message,
	)
}
