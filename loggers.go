package gohalt

import stdlog "log"

// Logger defined by typical logger func signature.
type Logger func(string, ...interface{})

// DefaultLogger defines default logger value used for logging.
// By default DefaultLogger is set to use `log.Printf`.
// Loggign can be completely disabled by setting DefaultLogger to nil.
var DefaultLogger Logger = stdlog.Printf

func log(format string, v ...interface{}) {
	if DefaultLogger != nil {
		DefaultLogger(format, v...)
	}
}
