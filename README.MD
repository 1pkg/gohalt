<p align="center">
    <img src="https://raw.githubusercontent.com/1pkg/gohalt/master/gopher.png" alt="gohalt"/>
</p>

# Gohalt 👮‍♀️: Fast; Simple; Powerful; Go; Throttler library

[![lint](https://github.com/1pkg/gohalt/workflows/lint/badge.svg)](https://github.com/1pkg/gohalt/actions?query=workflow%3Alint+branch%3Amaster+)
[![build](https://github.com/1pkg/gohalt/workflows/build/badge.svg)](https://github.com/1pkg/gohalt/actions?query=workflow%3Abuild+branch%3Amaster+)
[![test](https://github.com/1pkg/gohalt/workflows/test/badge.svg)](https://github.com/1pkg/gohalt/actions?query=workflow%3Atest+branch%3Amaster+)
[![report](https://goreportcard.com/badge/github.com/1pkg/gohalt)](https://goreportcard.com/report/github.com/1pkg/gohalt)
[![version](https://img.shields.io/github/go-mod/go-version/1pkg/gohalt)](https://github.com/1pkg/gohalt/blob/master/go.mod)
[![license](https://img.shields.io/github/license/1pkg/gohalt)](LICENSE)

## Introduction

Gohalt is simple and convenient yet powerful and fast throttling library for go. Gohalt provides number of efficient throttlers and surronding tools to build throttling pipelines and rate limiters of any complexity adjusted for your specific needs, and easy integrate them with your infrastructure through set of builtin middlewares.

## Features

- Blastly fast and efficient, Gohalt has close to zero overhead for your application as it uses well optimized and simple throttling techniques.
- Flexible and powerful, Gohalt supports numbers of different strategies and conditions for throttling that could be easily adjust to your needs, it's easy to build any throttling pipeline or rate limiter that you need with Gohalt.
- Easy to integrate, Gohalt provides numbers of built in middlewares to have one line integrations with go stdlib and other libraries, besides those are: io, rpc/grpc, http, sql, gin, etc.
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


## Contribution

Do you have an idea to improve Gohalt? -> [Create an issue](https://github.com/1pkg/gohalt/issues/new/choose).  
Have you discovered a bug? -> [Create an issue](https://github.com/1pkg/gohalt/issues/new/choose).  
Have you already coded something for Gohalt? -> [Create a pull request](https://github.com/1pkg/gohalt/compare).

## Licence

Gohalt is licensed under the MIT License.  
See [LICENSE](LICENSE) for the full license text.
