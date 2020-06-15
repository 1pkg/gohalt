# Gohalt 🛑: Deadly Fast and Incredebly Powerful Solution for Throttling and Rate Limiting

## Features

- Blastly fast and effective, Gohalt has close to zero overhead for your application as it uses well known dead simple but yet powerful throttling techniques and algorithms, check benchmark section for comparison.
- Flexible and powerful, Gohalt supports numbers of different strategies and conditions on rate limiting and throttling that could be easily adjust to your needs.
- Easily integratable, Gohalt provides numbers of built in middlewares to have one line integrations with http, fasthttp, gin, grpc, etc.
- Full metrics awareness, Gohalt could use metrics as main source of conditions for throttling, it does/will fully support for prometheus metrics.
- Queueing out of the box, Gohalt supports execution queueing out of the box which means you can easily save throttled query to queue to process it later.
- No external dependencies, Gohalt has zero additional external dependencies even for durable setups it doesn't need neither redis nor memchached nor nothing else.
- Optional durable embedded dbadger k/v, Gohalt can be set to use embedded dbadger k/v to provide durability for reads/writes.

## Licence

Gopium is licensed under the MIT License.  
See [LICENSE](LICENSE) for the full license text.
