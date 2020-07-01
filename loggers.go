package gohalt

import (
	"bytes"
	"context"
	"fmt"
	"log"
	"strings"
	"time"
)

type Logger func(string, ...interface{})

var DefaultLogger Logger = log.Printf

type logbuffered struct {
	log    Logger
	buffer bytes.Buffer
	level  uint
}

func NewLoggerBuffered(log Logger) *logbuffered {
	return &logbuffered{log: log}
}

func (log *logbuffered) Log(ctx context.Context, thr Throttler) error {
	defer log.buffer.Reset()
	if err := thr.accept(ctx, log); err != nil {
		return err.(error)
	}
	log.log(log.buffer.String())
	return nil
}

func (log *logbuffered) write(kind string, message string) error {
	var err error
	if message != "" {
		_, err = log.buffer.WriteString(
			fmt.Sprintf(
				"%s throttler %s - %s",
				strings.Repeat("-", int(log.level)),
				kind,
				message,
			),
		)
	} else {
		_, err = log.buffer.WriteString(
			fmt.Sprintf(
				"%s throttler %s",
				strings.Repeat("-", int(log.level)),
				kind,
			),
		)
	}
	return err
}

func (log *logbuffered) lnext() func() {
	log.level++
	return func() { log.level-- }
}

func (log *logbuffered) tvisitEcho(ctx context.Context, thr techo) interface{} {
	message := ""
	if thr.err != nil {
		message = thr.err.Error()
	}
	return log.write("echo", message)
}

func (log *logbuffered) tvisitWait(ctx context.Context, thr twait) interface{} {
	return log.write("wait", thr.duration.String())
}

func (log *logbuffered) tvisitPanic(ctx context.Context, thr tpanic) interface{} {
	return log.write("panic", "")
}

func (log *logbuffered) tvisitEach(ctx context.Context, thr teach) interface{} {
	return log.write("each", fmt.Sprintf("%d of %d", thr.current, thr.threshold))
}

func (log *logbuffered) tvisitAfter(ctx context.Context, thr tafter) interface{} {
	return log.write("after", fmt.Sprintf("%d of %d", thr.current, thr.threshold))
}

func (log *logbuffered) tvisitChance(ctx context.Context, thr tchance) interface{} {
	return log.write("chance", fmt.Sprintf("%f", thr.percentage))
}

func (log *logbuffered) tvisitFixed(ctx context.Context, thr tfixed) interface{} {
	return log.write("fixed", fmt.Sprintf("%d of %d", thr.current, thr.limit))
}

func (log *logbuffered) tvisitRunning(ctx context.Context, thr trunning) interface{} {
	return log.write("running", fmt.Sprintf("%d of %d", thr.running, thr.limit))
}

func (log *logbuffered) tvisitBuffered(ctx context.Context, thr tbuffered) interface{} {
	return log.write("buffered", fmt.Sprintf("%d", len(thr.running)))
}

func (log *logbuffered) tvisitPriority(ctx context.Context, thr tpriority) interface{} {
	return log.write("priority", fmt.Sprintf("%d with %d", thr.size, thr.limit))
}

func (log *logbuffered) tvisitTimed(ctx context.Context, thr ttimed) interface{} {
	return log.write("timed", fmt.Sprintf("%d of %d in %s", thr.current, thr.limit, thr.interval))
}

func (log *logbuffered) tvisitMonitor(ctx context.Context, thr tmonitor) interface{} {
	return log.write("monitor", fmt.Sprintf("%+v", thr.limit))
}

func (log *logbuffered) tvisitMetric(ctx context.Context, thr tmetric) interface{} {
	return log.write("metric", "")
}

func (log *logbuffered) tvisitLatency(ctx context.Context, thr tlatency) interface{} {
	return log.write("latency", fmt.Sprintf(
		"%s of %s in %s",
		time.Duration(thr.latency),
		thr.limit,
		thr.retention,
	))
}

func (log *logbuffered) tvisitPercentile(ctx context.Context, thr tpercentile) interface{} {
	return log.write("percentile", fmt.Sprintf(
		"%v of %s with %f in %s",
		thr.latencies,
		thr.limit,
		thr.percentile,
		thr.retention,
	))
}

func (log *logbuffered) tvisitAdaptive(ctx context.Context, thr tadaptive) interface{} {
	if err := log.write("adaptive", fmt.Sprintf(
		"%d of %d in %s with %d",
		thr.current,
		thr.limit,
		thr.interval,
		thr.step,
	)); err != nil {
		return err
	}
	lprev := log.lnext()
	defer lprev()
	return thr.thr.accept(ctx, log)
}

func (log *logbuffered) tvisitContext(ctx context.Context, thr tcontext) interface{} {
	return log.write("context", "")
}

func (log *logbuffered) tvisitEnqueue(ctx context.Context, thr tenqueue) interface{} {
	return log.write("enqueue", "")
}

func (log *logbuffered) tvisitKeyed(ctx context.Context, thr tkeyed) interface{} {
	if err := log.write("keyed", ""); err != nil {
		return err
	}
	lprev := log.lnext()
	defer lprev()
	var err interface{}
	thr.keys.Range(func(key interface{}, val interface{}) bool {
		err = val.(Throttler).accept(ctx, log)
		return err != nil
	})
	return err
}

func (log *logbuffered) tvisitAll(ctx context.Context, thrs tall) interface{} {
	if err := log.write("all", fmt.Sprintf("%d", len(thrs))); err != nil {
		return err
	}
	lprev := log.lnext()
	defer lprev()
	for _, thr := range thrs {
		if err := thr.accept(ctx, log); err != nil {
			return err
		}
	}
	return nil
}

func (log *logbuffered) tvisitAny(ctx context.Context, thrs tany) interface{} {
	if err := log.write("any", fmt.Sprintf("%d", len(thrs))); err != nil {
		return err
	}
	lprev := log.lnext()
	defer lprev()
	for _, thr := range thrs {
		if err := thr.accept(ctx, log); err != nil {
			return err
		}
	}
	return nil
}

func (log *logbuffered) tvisitNot(ctx context.Context, thr tnot) interface{} {
	if err := log.write("not", ""); err != nil {
		return err
	}
	lprev := log.lnext()
	defer lprev()
	return thr.thr.accept(ctx, log)
}
