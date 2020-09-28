<p align="center">
    <img src="https://raw.githubusercontent.com/1pkg/gohalt/master/gopher.png" alt="gohalt"/>
</p>

# Gohalt 👮‍♀️: Fast; Simple; Powerful; Go Throttler library

[![lint](https://github.com/1pkg/gohalt/workflows/lint/badge.svg)](https://github.com/1pkg/gohalt/actions?query=workflow%3Alint+branch%3Amaster+)
[![build](https://github.com/1pkg/gohalt/workflows/build/badge.svg)](https://github.com/1pkg/gohalt/actions?query=workflow%3Abuild+branch%3Amaster+)
[![test](https://github.com/1pkg/gohalt/workflows/test/badge.svg)](https://github.com/1pkg/gohalt/actions?query=workflow%3Atest+branch%3Amaster+)
[![report](https://goreportcard.com/badge/github.com/1pkg/gohalt)](https://goreportcard.com/report/github.com/1pkg/gohalt)
[![version](https://img.shields.io/github/go-mod/go-version/1pkg/gohalt)](https://github.com/1pkg/gohalt/blob/master/go.mod)
[![license](https://img.shields.io/github/license/1pkg/gohalt)](LICENSE)

## Introduction

Gohalt is simple and convenient yet powerful and efficient throttling library for [go](https://golang.org/). Gohalt provides various throttlers and surronding tools to build throttling pipelines and rate limiters of any complexity adjusted to your specific needs. Gohalt provides easy ways to integrate throttling and rate limiting with your infrastructure through built in middlewares.

## Features

- Blastly fast and efficient, Gohalt has minimal performance overhead, it was design with performance as primary goal.
- Flexible and powerful, Gohalt supports numbers of different throttling strategies and conditions  that could be easily combined and customized to match your needs.
- Easy to integrate, Gohalt provides numbers of built in  middlewares for simple (couple lines of code) integrations with stdlib and other libraries, among which are: io, rpc/grpc, http, sql, gin, etc.
- Metrics awareness, Gohalt could use metrics as a conditions for throttling, currently Gohalt supports prometheus metrics.
- Queueing and delayed processing, Gohalt supports throttling queueing which means you can easily save throttled query to rabbitmq/kafka stream to process it later.

## Concepts

Gohalt uses `Throttler` as core interface for all derived throttlers.
```go
type Throttler interface {
	Acquire(context.Context) error
	Release(context.Context) error
}
```
`Throttler` interface exposes pair of methods that make it usage similar to `sync.Mutex` - you need to acquire throttling quota right before shared resource use and release throttling quota just after shared resource use. **Note:** all derived throttler implementations are thread safe, so they could be used concurrently without additional locking. **Note:** generally all acquired throttlers should be released exatly the same amount of times they have been acquired.

## Usage 

For example we have next function that queries duckduckgo to get topic->url map: 
```go
func duckduckgo(ctx context.Context, q string) (string, error) {
	var answer struct {
		AbstractURL string `json:"AbstractURL"`
	}
	select {
	case <-ctx.Done():
		return "", ctx.Err()
	default:
	}
	resp, err := http.Get(fmt.Sprintf("http://api.duckduckgo.com/?q=%s&format=json", q))
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return "", err
	}
	if err := json.Unmarshal(body, &answer); err != nil {
		return "", err
	}
	return answer.AbstractURL, nil
}
```

And next search function to make range queries to duckduckgo searcher:

```go
type searcher func(ctx context.Context, q string) (string, error)

func search(s searcher, topics []string) (urls []string) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	urls = make([]string, 0, len(topics))
	for _, topic := range topics {
		select {
		case <-ctx.Done():
			return urls
		default:
		}
		topic := topic
		go func() {
			url, err := s(ctx, topic)
			if err != nil {
				cancel()
				return
			}
			urls = append(urls, url)
		}()
	}
	return urls
}
```

Now if we wanna add throttling quota to duckduckgo searcher to limit it to let's say 10 concurent queries at max we can use next code:

```go
func maxsearcher(s searcher, max uint64) searcher {
	t := NewThrottlerRunning(max)
	return func(ctx context.Context, q string) (string, error) {
		if err := t.Acquire(ctx); err != nil {
			return "", err
		}
		defer func() {
			if err := t.Release(ctx); err != nil {
				log.Print(err.Error())
			}
		}()
		return s(ctx, q)
	}
}
```

Even better to use runner to handle all acquire/release code:

```go
func maxrunsearcher(s searcher, max uint64) searcher {
	t := NewThrottlerBuffered(max)
	return func(ctx context.Context, q string) (string, error) {
		var result string
		r := NewRunnerSync(ctx, t)
		r.Run(func(ctx context.Context) (err error) {
			result, err = s(ctx, q)
			return
		})
		return result, r.Result()
	}
}
```

Or you can even add this throttler to context by using:

```go
func maxsearch(ctx context.Context, s searcher, topics []string, max uint64) (urls []string) {
	ctx = WithThrottler(ctx, NewThrottlerRunning(max), time.Millisecond)
	return search(ctx, s, topics)
}
```

## Throttlers

| Throttler | Definition | Description |
|---|---|---|
| echo | `NewThrottlerEcho(err error)` | Always throttles with the specified error back. |
| wait | `NewThrottlerWait(duration time.Duration)` | Always waits for the specified duration. |
| square | `NewThrottlerSquare(duration time.Duration, limit time.Duration, reset bool)` | Always waits for square growing *[1, 4, 9, 16, ...]* multiplier on the specified duration, up until the specified duration limit is riched.<br> If reset is set then after throttler riches the specified duration limit next multiplier value will be reseted. |
| context | `NewThrottlerContext()` | Always throttless on *done* context. |
| panic | `NewThrottlerPanic()` | Always panics. |
| each | `NewThrottlerEach(threshold uint64)` | Throttles each periodic *i-th* call defined by the specified threshold. |
| before | `NewThrottlerBefore(threshold uint64)` | Throttles each call below the *i-th* call defined by the specified threshold. |
| after | `NewThrottlerAfter(threshold uint64)` | Throttles each call after the *i-th* call defined by the specified threshold. |
| chance | `NewThrottlerChance(threshold float64)` | Throttles each call with the chance *p* defined by the specified threshold.<br> Chance value is normalized to *[0.0, 1.0]* range.<br> Implementation uses `math/rand` as PRNG function and expects rand seeding by a client. |
| running | `NewThrottlerRunning(threshold uint64)` | Throttles each call which exeeds the running quota *acquired - release* *q* defined by the specified threshold. |
| buffered | `NewThrottlerBuffered(threshold uint64)` | Waits on call which exeeds the running quota *acquired - release* *q* defined by the specified threshold until until the running quota is available again. |
| priority | `NewThrottlerPriority(threshold uint64, levels uint8)` | Waits on call which exeeds the running quota *acquired - release* *q* defined by the specified threshold until until the running quota is available again.<br> Running quota is not equally distributed between *n* levels of priority defined by the specified levels.<br> Use `WithPriority(ctx context.Context, priority uint8) context.Context` to override context call priority, *1* by default. |
| timed | `NewThrottlerTimed(threshold uint64, interval time.Duration, quantum time.Duration)` | Throttles each call which exeeds the running quota *acquired - release* *q* defined by the specified threshold in the specified interval.<br> Periodically each specified interval the running quota number is reseted.<br> If quantum is set then quantum will be used instead of interval to provide the running quota delta updates. |
| latency | `NewThrottlerLatency(threshold time.Duration, retention time.Duration)` | Throttles each call after the call latency *l* defined by the specified threshold was exeeded once.<br> If retention is set then throttler state will be reseted after retention duration. |
| percentile | `NewThrottlerPercentile(threshold time.Duration, percentile float64, retention time.Duration)` | Throttles each call after the call latency *l* defined by the specified threshold was exeeded once considering the specified percentile.<br> If retention is set then throttler state will be reseted after retention duration. |
| monitor | `NewThrottlerMonitor(mnt Monitor, threshold Stats)` | Throttles call if any of the stats returned by the provided monitor exceeds any of the stats defined by the specified threshold or if any internal error occurred. |
| metric | `NewThrottlerMetric(mtc Metric)` | Throttles call if metric defined by the specified metric is riched or if any internal error occurred. |
| enqueuer | `NewThrottlerEnqueue(enq Enqueuer)` | Always enqueues message to the specified queue throttles only if any internal error occurred.<br> Use `WithData(ctx context.Context, data interface{}) context.Context` to specify context data for enqueued message and `WithMarshaler(ctx context.Context, mrsh Marshaler) context.Context` to specify context data marshaler. |
| adaptive | `NewThrottlerAdaptive(threshold uint64, interval time.Duration, quantum time.Duration, step uint64, thr Throttler)` | Throttles each call which exeeds the running quota *acquired - release* *q* defined by the specified threshold in the specified interval.<br> Periodically each specified interval the running quota number is reseted.<br> If quantum is set then quantum will be used instead of interval to provide the running quota delta updates.<br> Provided adapted throttler adjusts the running quota of adapter throttler by changing the value by *d* defined by the specified step, it substracts *d^2* from the running quota if adapted throttler throttles or adds *d* to the running quota if it doesn't. |
| pattern | `NewThrottlerPattern(patterns ...Pattern)` | Throttles if matching throttler from provided patterns throttle.<br> Use `WithKey(ctx context.Context, key interface{}) context.Context` to specify key for regex pattern throttler matching.|
| ring | `NewThrottlerRing(thrs ...Throttler)` | Throttles if the *i-th* call throttler from the provided list throttle. |
| all | `NewThrottlerAll(thrs ...Throttler)` | Throttles call if all provided throttlers throttle. |
| any | `NewThrottlerAny(thrs ...Throttler)` | Throttles call if any of provided throttlers throttle. |
| not | `NewThrottlerNot(thr Throttler)` | Throttles call if provided throttler doesn't throttle. |
| suppress | `NewThrottlerSuppress(thr Throttler)` | Suppresses provided throttler to never throttle. |

## Licence

Gohalt is licensed under the MIT License.  
See [LICENSE](LICENSE) for the full license text.
