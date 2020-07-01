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

type logbuffer struct {
	buffer bytes.Buffer
	level  uint
}

func (log Logger) Log(ctx context.Context, thr Throttler) {
	var logb logbuffer
	thr.accept(ctx, &logb)
	log(logb.buffer.String())
}

func (logb *logbuffer) write(kind string, message string) {
	if message != "" {
		_, _ = logb.buffer.WriteString(
			fmt.Sprintf(
				"%s throttler %s - %s",
				strings.Repeat("-", int(logb.level)),
				kind,
				message,
			),
		)
	} else {
		_, _ = logb.buffer.WriteString(
			fmt.Sprintf(
				"%s throttler %s",
				strings.Repeat("-", int(logb.level)),
				kind,
			),
		)
	}
}

func (logb *logbuffer) lnext() func() {
	logb.level++
	return func() { logb.level-- }
}

func (logb *logbuffer) tvisitEcho(ctx context.Context, thr techo) {
	message := ""
	if thr.err != nil {
		message = thr.err.Error()
	}
	logb.write("echo", message)
}

func (logb *logbuffer) tvisitWait(ctx context.Context, thr twait) {
	logb.write("wait", thr.duration.String())
}

func (logb *logbuffer) tvisitPanic(ctx context.Context, thr tpanic) {
	logb.write("panic", "")
}

func (logb *logbuffer) tvisitEach(ctx context.Context, thr teach) {
	logb.write("each", fmt.Sprintf("%d of %d", thr.current, thr.threshold))
}

func (logb *logbuffer) tvisitAfter(ctx context.Context, thr tafter) {
	logb.write("after", fmt.Sprintf("%d of %d", thr.current, thr.threshold))
}

func (logb *logbuffer) tvisitChance(ctx context.Context, thr tchance) {
	logb.write("chance", fmt.Sprintf("%f", thr.percentage))
}

func (logb *logbuffer) tvisitFixed(ctx context.Context, thr tfixed) {
	logb.write("fixed", fmt.Sprintf("%d of %d", thr.current, thr.limit))
}

func (logb *logbuffer) tvisitRunning(ctx context.Context, thr trunning) {
	logb.write("running", fmt.Sprintf("%d of %d", thr.running, thr.limit))
}

func (logb *logbuffer) tvisitBuffered(ctx context.Context, thr tbuffered) {
	logb.write("buffered", fmt.Sprintf("%d", len(thr.running)))
}

func (logb *logbuffer) tvisitPriority(ctx context.Context, thr tpriority) {
	logb.write("priority", fmt.Sprintf("%d with %d", thr.size, thr.limit))
}

func (logb *logbuffer) tvisitTimed(ctx context.Context, thr ttimed) {
	logb.write("timed", fmt.Sprintf("%d of %d in %s", thr.current, thr.limit, thr.interval))
}

func (logb *logbuffer) tvisitMonitor(ctx context.Context, thr tmonitor) {
	logb.write("monitor", fmt.Sprintf("%+v", thr.limit))
}

func (logb *logbuffer) tvisitMetric(ctx context.Context, thr tmetric) {
	logb.write("metric", "")
}

func (logb *logbuffer) tvisitLatency(ctx context.Context, thr tlatency) {
	logb.write("latency", fmt.Sprintf(
		"%s of %s in %s",
		time.Duration(thr.latency),
		thr.limit,
		thr.retention,
	))
}

func (logb *logbuffer) tvisitPercentile(ctx context.Context, thr tpercentile) {
	logb.write("percentile", fmt.Sprintf(
		"%v of %s with %f in %s",
		thr.latencies,
		thr.limit,
		thr.percentile,
		thr.retention,
	))
}

func (logb *logbuffer) tvisitAdaptive(ctx context.Context, thr tadaptive) {
	logb.write("adaptive", fmt.Sprintf(
		"%d of %d in %s with %d",
		thr.current,
		thr.limit,
		thr.interval,
		thr.step,
	))
	lprev := logb.lnext()
	defer lprev()
	thr.thr.accept(ctx, logb)
}

func (logb *logbuffer) tvisitContext(ctx context.Context, thr tcontext) {
	logb.write("context", "")
}

func (logb *logbuffer) tvisitEnqueue(ctx context.Context, thr tenqueue) {
	logb.write("enqueue", "")
}

func (logb *logbuffer) tvisitKeyed(ctx context.Context, thr tkeyed) {
	logb.write("keyed", "")
	lprev := logb.lnext()
	defer lprev()
	thr.keys.Range(func(key interface{}, val interface{}) bool {
		val.(Throttler).accept(ctx, logb)
		return true
	})
}

func (logb *logbuffer) tvisitAll(ctx context.Context, thrs tall) {
	logb.write("all", fmt.Sprintf("%d", len(thrs)))
	lprev := logb.lnext()
	defer lprev()
	for _, thr := range thrs {
		thr.accept(ctx, logb)
	}
}

func (logb *logbuffer) tvisitAny(ctx context.Context, thrs tany) {
	logb.write("any", fmt.Sprintf("%d", len(thrs)))
	lprev := logb.lnext()
	defer lprev()
	for _, thr := range thrs {
		thr.accept(ctx, logb)
	}
}

func (logb *logbuffer) tvisitNot(ctx context.Context, thr tnot) {
	logb.write("not", "")
	lprev := logb.lnext()
	defer lprev()
	thr.thr.accept(ctx, logb)
}
