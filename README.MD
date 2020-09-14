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
| echo |  `NewThrottlerEcho(err error)` | Throttles specified error back. |
| wait  | `NewThrottlerWait(duration time.Duration)`  | Waits (blocks) for static specified time duration. |
| square | `NewThrottlerSquare(duration time.Duration, limit time.Duration, reset bool)`  | Waits (blocks) for (n^2) * specified time duration, up until specified time duration limit is riched. <br>If reset was specified after throttler riches specified time duration limit next wait time resets to n=1. |
| panic | `NewThrottlerPanic()` | Just panics. |
| each | `NewThrottlerEach(threshold uint64)` | Throttles each n-th specified threshold.  |
| before | `NewThrottlerBefore(threshold uint64)` | Throttles only while specified threshold wasn't riched. |
| chance | `NewThrottlerChance(threshold float64)` | Throttles with specified threshold chance.<br>Chance has to have value betweeen 0.0 and 1.0. |
| after | `NewThrottlerAfter(threshold uint64)` | Throttles only after specified threshold wasn riched. |
| running | `NewThrottlerRunning(threshold uint64)` | Throttles if current number of acquired - release > specified threshold number. |
| buffered | `NewThrottlerBuffered(threshold uint64)` | Waits (blocks) if current number of acquired - release > specified threshold number. |
| priority | `NewThrottlerPriority(threshold uint64, levels uint8)` | Waits (blocks) if current number of acquired - release > specified threshold number on specific priority level. Use  `WithPriority(ctx context.Context, priority uint8) context.Context` to override acquire default priority. |
| timed | `NewThrottlerTimed(threshold uint64, interval time.Duration, quantum time.Duration)` | Throttles if current number of acquired - release > specified threshold number in specified interval duration. Each specified interval duration current number of acquired will be reset. If quantum>0 then quantum interval will be used instead with quantized delta substraction. |
| monitor | `NewThrottlerMonitor(mnt Monitor, threshold Stats)` | Throttles if Stats returned from Monitor exceed specified threshold Stats. |
| metric | `NewThrottlerMetric(mtc Metric)` | Throttles if specified Metric was riched. |
| latency | `NewThrottlerLatency(threshold time.Duration, retention time.Duration)` | Throttles if specified threshold latency duration was exceed. If specified retention duration > 0 then throttler will be reset after retention duration. |
| percentile | `NewThrottlerPercentile(threshold time.Duration, percentile float64, retention time.Duration)` | Throttles if specified threshold latency duration percentile was exceed. If specified retention duration > 0 then throttler will be reset after retention duration. |
| adaptive | `NewThrottlerAdaptive(threshold uint64, interval time.Duration, quantum time.Duration, step uint64, thr Throttler)` | Adaptive is timed throttler that uses specified step and additional throttler to adjust timed threshold. If adaptee throttler throttles specified timed threshold will go down by step*step otherwise it will go up by step. |
| context | `NewThrottlerContext()` | Throttles on Done context. |
| enqueuer | `NewThrottlerEnqueue(enq Enqueuer)` | Enqueues message to queue. |
| pattern | `NewThrottlerPattern(patterns ...Pattern)` | Uses regex pattern match throttler. |
| ring | `NewThrottlerRing(thrs ...Throttler)` | Uses next throttler from the ring buffer. |
| all | `NewThrottlerAll(thrs ...Throttler)` | Throttles only if child throttlers throttled. |
| any | `NewThrottlerAny(thrs ...Throttler)` | Throttles if any of child throttlers throttled. |
| not | `NewThrottlerNot(thr Throttler)` | Throttles only if child throttler didn't throttle. |
| suppress | `NewThrottlerSuppress(thr Throttler)` | Suppresses any child throttler. |

## Licence

Gohalt is licensed under the MIT License.  
See [LICENSE](LICENSE) for the full license text.
