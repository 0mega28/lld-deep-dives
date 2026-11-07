# mvcc-kv-store

A small in-process multi-version (MVCC) key-value store in Java, written for the blog post
[Three places my MVCC design broke when I wrote the code](https://0mega28.github.io/blog/mvcc-design-breaks).

- `put`, `get`, `delete`, per-key TTL, and an atomic `writeBatch` across keys
- Readers take no locks and never see half of a batch
- Each version points at its transaction; one CAS on the transaction's status makes all its versions visible at once
- Commit timestamps are allocated and published under a short lock, so they become visible in order
- Cleanup removes a version only when a later version in scan order also committed later

## Run

Needs Java 25 or later (module imports and an instance `main` method).

```sh
cd mvcc-kv-store
java Main.java
```

`main` runs a demo: basic reads and writes, TTL expiry, an aborted batch, and a concurrency check where three readers verify that no batch is ever seen half-applied.

## License

MIT, see [LICENSE](../LICENSE).
