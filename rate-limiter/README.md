# rate-limiter

An in-process rate limiter in Java, written for the blog post
[Your rate limiter's lock is around the wrong thing](https://0mega28.github.io/blog/concurrency/02-rate-limiter-lock-scope).

- `allow(clientId, api)` with limits per (client, API) pair and a default for unconfigured pairs
- Token bucket and sliding window counter strategies, each guarding its own state with `synchronized`
- `ConcurrentHashMap.compute` updates a pair's last access atomically with the eviction check, so evicting an idle pair can't leave two live strategies for one key
- One `DelayQueue` entry per live pair, re-armed while the pair stays active, so memory stays bounded as clients go idle
- Default config, per-pair config, and idle timeout can all change at runtime

The sliding window is the counter variant: O(1) memory per pair, but it estimates, so the true count can briefly exceed the limit (never reaching 2x).

## Run

Needs Java 25 or later (module imports and an instance `main` method).

```sh
cd rate-limiter
java -ea Main.java
```

`main` runs 23 checks: algorithm tests with hand-driven time (including a `nanoTime` wrap), concurrent tests released through a `CountDownLatch` start gate, eviction and runtime-config tests, and a deliberately unsynchronized bucket that shows the concurrent test can fail.

## License

MIT, see [LICENSE](../LICENSE).
