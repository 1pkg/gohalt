<p align="center">
    <img src="https://raw.githubusercontent.com/1pkg/gohalt/master/gopher.png" alt="gohalt"/>
</p>

# Gohalt 👮‍♀🛑: Fast; Simple; Powerful; Go Throttler library

[![lint](https://github.com/1pkg/gohalt/workflows/lint/badge.svg)](https://github.com/1pkg/gohalt/actions?query=workflow%3Alint+branch%3Amaster+)
[![test](https://github.com/1pkg/gohalt/workflows/test/badge.svg)](https://github.com/1pkg/gohalt/actions?query=workflow%3Atest+branch%3Amaster+)
[![report](https://goreportcard.com/badge/github.com/1pkg/gohalt?cache=1)](https://goreportcard.com/report/github.com/1pkg/gohalt)
[![version](https://img.shields.io/github/go-mod/go-version/1pkg/gohalt?cache=1)](https://github.com/1pkg/gohalt/blob/master/go.mod)
[![license](https://img.shields.io/github/license/1pkg/gohalt?cache=1)](LICENSE)
[![godoc](https://img.shields.io/badge/godoc-godoc-green?cache=1)](https://pkg.go.dev/github.com/1pkg/gohalt?tab=doc)

`go get -u github.com/1pkg/gohalt`

## Introduction

Gohalt is simple and convenient yet powerful and efficient throttling go library. Gohalt provides various throttlers and surronding tools to build throttling pipelines and rate limiters of any complexity adjusted to your specific needs. Gohalt provides an easy way to integrate throttling and rate limiting with your infrastructure through built in middlewares.

## Concepts

Gohalt uses `Throttler` as the core interface for all derived throttlers and surronding tools.
```go
// Throttler defines core gohalt throttler abstraction and exposes pair of counterpart methods: `Acquire` and `Release`.
type Throttler interface {
	// Acquire takes a part of throttling quota or returns error if throttling quota is drained
	// it needs to be called right before shared resource acquire.
	Acquire(context.Context) error
	// Release puts a part of throttling quota back or returns error if this is not possible
	// it needs to be called just after shared resource release.
	Release(context.Context) error
}
```
`Throttler` interface exposes pair of counterpart methods: `Acquire` takes a part of *throttling quota* or returns error if *throttling quota* is drained and needs to be called right before shared resource acquire; `Release` puts a part of *throttling quota* back or returns error if this is not possible and needs to be called just after shared resource release; **Note:** all derived throttler implementations are thread safe, so they could be used concurrently without additional locking. **Note:** all acquired throttlers should be released exatly the same amount of times they have been acquired. **Note:** despite throttler `Release` method has the same signature as `Acquire` has, `Release` implementations should try to handle any internal error gracefully and return error back rarely, nevertheless all errors returned by `Release` should be handeled by client.

In Gohalt throtllers could be easily combined with each other to build complex pipelines. There are multiple composite throttlers (all, any, ring, pattern, not, generator, etc) as well as leaf throttlers (timed, latency, monitor, metric, percentile, etc) to work with in Gohalt. If you don't find in [existing throttlers](#Throttlers) the one that fits your needs you can create custom throttler by implementing `Throttler` interface. Such custom throttler should work with existing Gohalt throttlers and tools out of box.

Gohalt includes multiple supporting surrounding tools to make throttling more sugary.
```go
// Runnable defined by typical abstract async func signature.
// Runnable is used by `Runner` as a subject for execution.
type Runnable func(context.Context) error
// Runner defines abstraction to execute a set of `Runnable`
// and return possible execution error back.
// Runner is designed to simplify work with throttlers
// by managing `Acquire`/`Release` loop.
type Runner interface {
	// Run executes single prodived `Runnable` instance.
	Run(Runnable)
	// Result returns possible execution error back.
	Result() error
}
```
`Runnable` and `Runner` define slim abstraction for executable and executor in Gohalt. `Runner` insterface aims to provide similar interface as [errgroup.Group](https://godoc.org/golang.org/x/sync/errgroup#Group) does. So to run a single executable use `Run` to wait and get result use `Result`.
There are two runners implementations in Gohalt:
- sync `func NewRunnerSync(ctx context.Context, thr Throttler) Runner`
- async `func NewRunnerAsync(ctx context.Context, thr Throttler) Runner`
Both implementation accept throttler and context as input arguments and handle all throttling cycle internaly. This way client donesn't need to call neither `Acquire` nor `Release` manually, all this is done by the runner. This way the only thing that needs to be done to add throttling to existing code wrap existing executable by `Runnable`. The only difference between sync and async runner is that the `async` runner starts each new `Runnable` inside new goroutine and uses locks for its imternal state. **Note:** You can't use sync runner in async fashion with `go syncr.Run(func(context.Context) error{})` this will cause data race, use async runner instead `async.Run(func(context.Context) error{})`.

Last but not least Gohalt uses context heavily inside and there are multiple helpers to provide data via context for throttles, see [throttles list](#Throttlers) to know when to use them.
```go
// WithTimestamp adds the provided timestamp to the provided context
// to determine latency between `Acquire` and `Release`.
// Resulted context is used by: `latency` and `percentile` throtttlers.
func WithTimestamp(ctx context.Context, ts time.Time) context.Context
// WithPriority adds the provided priority to the provided context
// to differ `Acquire` priority levels.
// Resulted context is used by: `priority` throtttler.
func WithPriority(ctx context.Context, priority uint8) context.Context
// WithWeight adds the provided weight to the provided context
// to differ `Acquire` weight levels.
// Resulted context is used by: `semaphore` and `cellrate` throtttlers.
func WithWeight(ctx context.Context, weight int64) context.Context
// WithKey adds the provided key to the provided context
// to add additional call identifier to context.
// Resulted context is used by: `before`, `after`, `timed`, `adaptive`, `semaphore`, `cellrate` and `bucket` throtttlers.
func WithKey(ctx context.Context, key string) context.Context
// WithMessage adds the provided message to the provided context
// to add additional message that need to be used to context.
// Resulted context is used by: `enqueue` throtttler.
func WithMessage(ctx context.Context, message interface{}) context.Context
// WithMarshaler adds the provided marshaler to the provided context
// to add additional message marshaler that need to be used to context.
// Resulted context is used by: `enqueue` throtttler.
// Used in pair with `WithMessage`.
func WithMarshaler(ctx context.Context, mrsh Marshaler) context.Context
// WithParams facade call that respectively calls:
// - `WithTimestamp`
// - `WithPriority`
// - `WithWeight`
// - `WithKey`
// - `WithMessage`
// - `WithMarshaler`
func WithParams(ctx context.Context, ts time.Time, priority uint8, weight int64, key string, message interface{}, marshaler Marshaler) context.Context
```
Also there is yet another throttling sugar `func WithThrottler(ctx context.Context, thr Throttler, freq time.Duration) context.Context` related to context. Which defines context implementation that uses parrent context plus throttler internally. Using it you can keep typical context patterns for cancelation handling and apply and combine it with throttling. 
```go
select {
	case <-ctx.Done():
		return ctx.Error()
	default:
}
```
If internal context throttler is throttling context done channel will be closed respectively. **Note** such behavior is implemented by throttler long pooling with the specified frequency, so efficiently there will be additional throttling user in form of long pooling goroutine.

```go
// complex throttler example
thr := NewThrottlerAll( // throttles only if all children throttle
	NewThrottlerPattern(
		Pattern{ // use throttler only if provided key matches `192.*.*.*` submask
			Pattern: regexp.MustCompile(`192\.[0-9]+\.[0-9]+\.[0-9]+`),
			Throttler: NewThrottlerAny( // throttles if any children throttles
				// throttles only if latency is above 50 millisecond
				NewThrottlerLatency(50*time.Millisecond, 5*time.Second),
				// throttles only if cpu usage is above 70%
				NewThrottlerMonitor(NewMonitorSystem(time.Minute), Stats{CPUUsage: 0.7}),
			),
		},
	),
	// throttles each not 3rd call
	NewThrottlerNot(NewThrottlerEach(3)),
	// enqueues provided message to queue
	NewThrottlerEnqueue(NewEnqueuerRabbit("amqp://user:pass@localhost:5672/vhost", "queue", time.Minute)),
)
```

In gohalt v0.4.0 breaking change is introduced to replace all untyped errors with two major error types:
- `ErrorThreshold` which defines error type that occurs if throttler reaches specified threshold.
- `ErrorInternal` which defines error type that occurs if throttler internal error happens.
You can find list of returning error types for all existing throttlers in throttlers table bellow or in documentation.  
**Note:** not every gohalt throttler must return error; some throttlers might cause different side effects like logging or call to `time.Sleep` instead.

## Throttlers

| Throttler | Definition | Description |
|---|---|---|
| echo | `func NewThrottlerEcho(err error) Throttler` | Always throttles with the specified error back.<br> - could return any specified error; |
| wait | `func NewThrottlerWait(duration time.Duration) Throttler` | Always waits for the specified duration. |
| square | `func NewThrottlerSquare(duration time.Duration, limit time.Duration, reset bool) Throttler` | Always waits for square growing *[1, 4, 9, 16, ...]* multiplier on the specified initial duration, up until the specified duration limit is reached.<br> If reset is set then after throttler riches the specified duration limit next multiplier value will be reseted. |
| jitter | `func NewThrottlerJitter(initial time.Duration, limit time.Duration, reset bool, jitter float64) Throttler` | Waits accordingly to undelying square throttler but also adds the provided jitter delta distribution on top.<br> Jitter value is normalized to [0.0, 1.0] range and defines which part of square delay could be randomized in percents.<br> Implementation uses secure `crypto/rand` as PRNG function. |
| context | `func NewThrottlerContext() Throttler` | Always throttless on *done* context.<br> - could return `ErrorInternal`; |
| panic | `func NewThrottlerPanic() Throttler` | Always panics with `ErrorInternal`. |
| each | `func NewThrottlerEach(threshold uint64) Throttler` | Throttles each periodic *i-th* call defined by the specified threshold.<br> - could return `ErrorThreshold`; |
| before | `func NewThrottlerBefore(threshold uint64) Throttler` | Throttles each call below the *i-th* call defined by the specified threshold.<br>Use `WithWeight` to override context call qunatity, 1 by default.<br> - could return `ErrorThreshold`; |
| after | `func NewThrottlerAfter(threshold uint64) Throttler` | Throttles each call after the *i-th* call defined by the specified threshold.<br>Use `WithWeight` to override context call qunatity, 1 by default.<br> - could return `ErrorThreshold`; |
| past | `func NewThrottlerPast(threshold time.Time) Throttler` | Throttles each call befor timestamp defined by the specified UTC time threshold.<br> - could return `ErrorThreshold`; |
| future | `func NewThrottlerFuture(threshold time.Time) Throttler` | Throttles each call after timestamp defined by the specified UTC time threshold.<br> - could return `ErrorThreshold`; |
| chance | `func NewThrottlerChance(threshold float64) Throttler` | Throttles each call with the chance *p* defined by the specified threshold.<br> Chance value is normalized to *[0.0, 1.0]* range.<br> Implementation uses secure `crypto/rand` as PRNG function.<br> - could return `ErrorThreshold`; |
| running | `func NewThrottlerRunning(threshold uint64) Throttler` | Throttles each call which exeeds the running quota *acquired - release* *q* defined by the specified threshold.<br> - could return `ErrorThreshold`; |
| buffered | `func NewThrottlerBuffered(threshold uint64) Throttler` | Waits on call which exeeds the running quota *acquired - release* *q* defined by the specified threshold until the running quota is available again. |
| priority | `func NewThrottlerPriority(threshold uint64, levels uint8) Throttler` | Waits on call which exeeds the running quota *acquired - release* *q* defined by the specified threshold until the running quota is available again.<br> Running quota is not equally distributed between *n* levels of priority defined by the specified levels.<br> Use `func WithPriority(ctx context.Context, priority uint8) context.Context` to override context call priority, *1* by default. |
| timed | `func NewThrottlerTimed(threshold uint64, interval time.Duration, quantum time.Duration) Throttler` | Throttles each call which exeeds the running quota *acquired - release* *q* defined by the specified threshold in the specified interval.<br> Periodically each specified interval the running quota number is reseted.<br> If quantum is set then quantum will be used instead of interval to provide the running quota delta updates.<br>Use `WithWeight` to override context call qunatity, 1 by default.<br> - could return `ErrorThreshold`; |
| latency | `func NewThrottlerLatency(threshold time.Duration, retention time.Duration) Throttler` | Throttles each call after the call latency *l* defined by the specified threshold was exeeded once.<br> If retention is set then throttler state will be reseted after retention duration.<br> Use `func WithTimestamp(ctx context.Context, ts time.Time) context.Context` to specify running duration between throttler *acquire* and *release*.<br> - could return `ErrorThreshold`; |
| percentile | `func NewThrottlerPercentile(threshold time.Duration, capacity uint8, percentile float64, retention time.Duration) Throttler` | Throttles each call after the call latency *l* defined by the specified threshold was exeeded once considering the specified percentile.<br> Percentile values are kept in bounded buffer with capacity *c* defined by the specified capacity. <br> If retention is set then throttler state will be reseted after retention duration.<br> Use `func WithTimestamp(ctx context.Context, ts time.Time) context.Context` to specify running duration between throttler *acquire* and *release*.<br> - could return `ErrorThreshold`; |
| monitor | `func NewThrottlerMonitor(mnt Monitor, threshold Stats) Throttler` | Throttles call if any of the stats returned by provided monitor exceeds any of the stats defined by the specified threshold or if any internal error occurred.<br> Builtin `Monitor` implementations come with stats caching by default.<br> Use builtin `NewMonitorSystem` to create go system monitor instance.<br> - could return `ErrorInternal`;<br> - could return `ErrorThreshold`; |
| metric | `func NewThrottlerMetric(mtc Metric) Throttler` | Throttles call if boolean metric defined by the specified boolean metric is reached or if any internal error occurred.<br> Builtin `Metric` implementations come with boolean metric caching by default.<br> Use builtin `NewMetricPrometheus` to create Prometheus metric instance.<br> - could return `ErrorInternal`;<br> - could return `ErrorThreshold`; |
| enqueuer | `func NewThrottlerEnqueue(enq Enqueuer) Throttler` | Always enqueues message to the specified queue throttles only if any internal error occurred.<br> Use `func WithMessage(ctx context.Context, message interface{}) context.Context` to specify context message for enqueued message and `func WithMarshaler(ctx context.Context, mrsh Marshaler) context.Context` to specify context message marshaler.<br> Builtin `Enqueuer` implementations come with connection reuse and retries by default.<br> Use builtin `func NewEnqueuerRabbit(url string, queue string, retries uint64) Enqueuer` to create RabbitMQ enqueuer instance or `func NewEnqueuerKafka(net string, url string, topic string, retries uint64) Enqueuer` to create Kafka enqueuer instance.<br> - could return `ErrorInternal`; |
| adaptive | `func NewThrottlerAdaptive(threshold uint64, interval time.Duration, quantum time.Duration, step uint64, thr Throttler) Throttler` | Throttles each call which exeeds the running quota *acquired - release* *q* defined by the specified threshold in the specified interval.<br> Periodically each specified interval the running quota number is reseted.<br> If quantum is set then quantum will be used instead of interval to provide the running quota delta updates.<br> Provided adapted throttler adjusts the running quota of adapter throttler by changing the value by *d* defined by the specified step, it subtracts *d^2* from the running quota if adapted throttler throttles or adds *d* to the running quota if it doesn't.<br>Use `WithWeight` to override context call qunatity, 1 by default.<br> - could return `ErrorThreshold`; |
| pattern | `func NewThrottlerPattern(patterns ...Pattern) Throttler` | Throttles if matching throttler from provided patterns throttles.<br> Use `func WithKey(ctx context.Context, key string) context.Context` to specify key for regexp pattern throttler matching.<br> `Pattern` defines a pair of regexp and related throttler.<br> - could return `ErrorInternal`;<br> - could return any underlying throttler error; |
| ring | `func NewThrottlerRing(thrs ...Throttler) Throttler` | Throttles if the *i-th* call throttler from provided list throttle.<br> - could return `ErrorInternal`;<br> - could return any underlying throttler error; |
| all | `func NewThrottlerAll(thrs ...Throttler) Throttler` | Throttles call if all provided throttlers throttle.<br> - could return `ErrorInternal`; |
| any | `func NewThrottlerAny(thrs ...Throttler) Throttler` | Throttles call if any of provided throttlers throttle.<br> - could return `ErrorInternal`; |
| not | `func NewThrottlerNot(thr Throttler) Throttler` | Throttles call if provided throttler doesn't throttle.<br> - could return `ErrorInternal`; |
| suppress | `func NewThrottlerSuppress(thr Throttler) Throttler` | Suppresses provided throttler to never throttle. |
| retry | `func NewThrottlerRetry(thr Throttler, retries uint64) Throttler` | Retries provided throttler error up until the provided retries threshold.<br> If provided onthreshold flag is set even `ErrorThreshold` errors will be retried.<br> Internally retry uses square throttler with `DefaultRetriedDuration` initial duration.<br> - could return any underlying throttler error; |
| cache | `func NewThrottlerCache(thr Throttler, cache time.Duration) Throttler` | Caches provided throttler calls for the provided cache duration, throttler release resulting resets cache.<br> Only non throttling calls are cached for the provided cache duration.<br> - could return any underlying throttler error; |
| generator | `func NewThrottlerGenerator(gen Generator, capacity uint64, eviction float64) Throttler` | Creates new throttler instance that throttles if found key matching throttler throttles.<br> If no key matching throttler has been found generator used insted to provide new throttler that will be added to existing throttlers map.<br> Generated throttlers are kept in bounded map with capacity *c* defined by the specified capacity and eviction rate *e* defined by specified eviction value is normalized to [0.0, 1.0], where eviction rate affects number of throttlers that will be removed from the map after bounds overflow.<br> Use `WithKey` to specify key for throttler matching and generation.<br> - could return `ErrorInternal`;<br> - could return any underlying throttler error; |
| semaphore | `func NewThrottlerSemaphore(weight int64) Throttler` | Creates new throttler instance that throttles call if underlying semaphore throttles.<br>Use `WithWeight` to override context call weight, 1 by default.<br> - could return `ErrorThreshold`; |
| cellrate | `func NewThrottlerCellRate(threshold uint64, interval time.Duration, monotone bool) Throttler` | Creates new throttler instance that uses generic cell rate algorithm to throttles call within provided interval and threshold.<br>If provided monotone flag is set class to release will have no effect on throttler.<br>Use `WithWeight` to override context call qunatity, 1 by default.<br> - could return `ErrorThreshold`; |
| bucket | `func NewThrottlerBucket(threshold uint64, interval time.Duration, monotone bool) Throttler` | Creates new throttler instance that leaky bucket algorithm to throttles call within provided interval and threshold.<br>If provided monotone flag is set class to release will have no effect on throttler.<br>Use `WithWeight` to override context call qunatity, 1 by default.<br> - could return `ErrorThreshold`; |

## Licence

Gohalt is licensed under the MIT License.  
See [LICENSE](LICENSE) for the full license text.
