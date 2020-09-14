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

```go
type Throttler interface {
	Acquire(context.Context) error
	Release(context.Context) error
}
```

## Contribution

Do you have an idea to improve Gohalt? -> [Create an issue](https://github.com/1pkg/gohalt/issues/new/choose).  
Have you discovered a bug? -> [Create an issue](https://github.com/1pkg/gohalt/issues/new/choose).  
Have you already coded something for Gohalt? -> [Create a pull request](https://github.com/1pkg/gohalt/compare).

## Licence

Gohalt is licensed under the MIT License.  
See [LICENSE](LICENSE) for the full license text.
