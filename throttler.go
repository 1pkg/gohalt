package gohalt

import "context"

// Throttler defines main interfaces
// for all derived throttler, and
// defines main throttling check flow.
type Throttler interface {
	Check(context.Context) error
}
