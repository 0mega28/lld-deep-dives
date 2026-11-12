// Build an in-process rate-limiting library that services embed to protect their APIs.
//
// A caller asks allow(clientId, api) and gets back whether the request may proceed.
// Limits are configured per (client, API) pair, with a default for pairs that have no explicit config.
// A config includes the algorithm and its parameters.
// For example, client A gets 100 requests per minute on /search using one algorithm,
// and client B gets 10 per second on /upload using another.
// Support at least two algorithms: token bucket and sliding window (log or counter, your choice).
// Adding a third should not require touching existing algorithm code.
// Many threads call allow concurrently for the same and different keys.
// No request may be admitted beyond the configured limit.
// Limits can be updated at runtime for a pair.
// Memory should not grow without bound as new clients appear and go idle.
// Write tests that show the limits hold, including under concurrency.

import module java.base;

interface RateLimitConfig {}

record TokenBucketConfig(
    long tokenPerMinute,
    int maxToken
) implements RateLimitConfig {}

record SlidingWindowConfig(
    int windowMinute,
    int maxToken
) implements RateLimitConfig {}

interface RateLimitStrategy {
    boolean allow(long timeStamp);
}

static class TokenBucketStrategy implements RateLimitStrategy {

    // Refill is computed in nanoseconds per token, so any configured rate is honoured exactly.
    // lastUpdateAccess advances only by the time actually converted into whole tokens;
    // the remainder carries over to the next call instead of being thrown away.
    static final double NANOS_IN_MIN = 60 * 1e9;
    final TokenBucketConfig config;
    final double tokensPerNs;
    long lastUpdateAccess;
    long leftToken;

    public TokenBucketStrategy(TokenBucketConfig config, long createTimeStamp) {
        this.config = config;
        this.tokensPerNs = (double) config.tokenPerMinute() / NANOS_IN_MIN;
        this.lastUpdateAccess = createTimeStamp;
        this.leftToken = config.maxToken(); // a new bucket starts full
    }

    @Override
    public synchronized boolean allow(long timeStamp) {
        // TODO can be lock-free: pack lastUpdateAccess and leftToken into one CAS-able state
        long elapsedTimeNs = timeStamp - lastUpdateAccess;

        long newTokens = (long) Math.floor(elapsedTimeNs * tokensPerNs);
        if (newTokens >= 1) {
            leftToken += newTokens;
            lastUpdateAccess += (long) (newTokens / tokensPerNs);
        }
        if (leftToken > config.maxToken()) leftToken = config.maxToken();

        if (leftToken <= 0) return false;
        --leftToken;
        return true;
    }
}

static class SlidingWindowStrategy implements RateLimitStrategy {

    // Sliding window counter. Estimates the count in (now - window, now] as
    //   currCount + prevCount * (fraction of the previous window still inside the sliding window).
    // Assumes the previous window's requests were spread evenly. A burst at the end of the
    // previous window can let the true count exceed maxToken (bounded below 2x).
    // Tradeoff taken for O(1) memory; use a sliding log if the limit must be exact.
    final SlidingWindowConfig config;
    final long windowNs;
    long currWindowStart;
    long currCount;
    long prevCount;

    public SlidingWindowStrategy(
        SlidingWindowConfig config,
        long createTimeStamp
    ) {
        this.config = config;
        this.windowNs = TimeUnit.MINUTES.toNanos(config.windowMinute());
        this.currWindowStart = createTimeStamp;
    }

    @Override
    public synchronized boolean allow(long timeStamp) {
        long elapsed = timeStamp - currWindowStart;
        if (elapsed >= windowNs) {
            long windowsPassed = elapsed / windowNs;
            // Only the window immediately before the current one counts; older windows are gone.
            prevCount = windowsPassed == 1 ? currCount : 0;
            currCount = 0;
            currWindowStart += windowsPassed * windowNs; // stay aligned to window boundaries
            elapsed -= windowsPassed * windowNs;
        }
        double prevWeight = (double) (windowNs - elapsed) / windowNs;
        double estimate = prevCount * prevWeight + currCount;
        if (estimate + 1 > config.maxToken()) return false;
        currCount++;
        return true;
    }
}

static interface RateLimitStrategyFactory {
    static RateLimitStrategy create(
        RateLimitConfig config,
        long createTimeStamp
    ) {
        return switch (config) {
            case TokenBucketConfig tokenBucketConfig -> new TokenBucketStrategy(
                tokenBucketConfig,
                createTimeStamp
            );
            case SlidingWindowConfig slidingWindowConfig -> new SlidingWindowStrategy(
                slidingWindowConfig,
                createTimeStamp
            );
            case null -> throw new NullPointerException();
            default -> throw new NoSuchElementException(
                "Strategy doesn't exist"
            );
        };
    }
}

record ClientAndAPI(long clientId, String api) {}

record IdleClientAndAPI(
    ClientAndAPI clientAndAPI,
    long deadline
) implements Delayed {
    @Override
    public long getDelay(TimeUnit timeUnit) {
        return timeUnit.convert(
            deadline() - System.nanoTime(),
            TimeUnit.NANOSECONDS
        );
    }

    @Override
    public int compareTo(Delayed other) {
        // difference, not Long.compare: nanoTime values may wrap
        long diff = this.deadline() - ((IdleClientAndAPI) other).deadline();
        return diff < 0 ? -1 : diff > 0 ? 1 : 0;
    }
}

static class RateLimiter implements AutoCloseable {

    // Eviction bookkeeping lives here, not in the algorithms, so a new algorithm
    // only has to implement allow(). lastAccess is written and read only inside
    // CHM bin-locked lambdas, so the bin lock orders those accesses.
    static final class Entry {

        final RateLimitStrategy strategy;
        long lastAccess;

        Entry(RateLimitStrategy strategy, long lastAccess) {
            this.strategy = strategy;
            this.lastAccess = lastAccess;
        }
    }

    // volatile: an admin thread's write happens-before any later read that sees it
    // (allow threads, cleanup thread). Config records have only final fields.
    volatile long idleTimeout;
    volatile RateLimitConfig defaultConfig;
    // CHM publication rule: a put happens-before any get that returns that value.
    final ConcurrentHashMap<ClientAndAPI, RateLimitConfig> clientAndAPIConfig =
        new ConcurrentHashMap<>();
    final ConcurrentHashMap<ClientAndAPI, Entry> strategyMap =
        new ConcurrentHashMap<>();
    // At most one entry per live pair: enqueued on creation, re-armed by cleanup.
    final DelayQueue<IdleClientAndAPI> cleanUpQueue = new DelayQueue<>();
    final Thread cleanUpThread;

    public RateLimiter(RateLimitConfig defaultConfig, long idleTimeout) {
        setIdleTimeout(idleTimeout);
        setDefaultConfig(defaultConfig);

        cleanUpThread = new Thread(this::cleanUp);
        cleanUpThread.setDaemon(true);
        cleanUpThread.start();
    }

    boolean allow(long clientId, String api) {
        var clientAndAPI = new ClientAndAPI(clientId, api);
        long timeStamp = System.nanoTime();

        // compute (not computeIfAbsent): the lastAccess touch and cleanup's eviction
        // serialize on the same bin lock, so cleanup can't evict a strategy a caller
        // is about to use and let a second, fresh strategy admit alongside it.
        // Assumption: a caller isn't paused (GC, preemption) for longer than idleTimeout
        // between compute returning and strategy.allow running; if it is, one extra
        // request can be admitted.
        var entry = strategyMap.compute(clientAndAPI, (key, existing) -> {
            if (existing == null) {
                var config = clientAndAPIConfig.getOrDefault(
                    key,
                    defaultConfig
                );
                existing = new Entry(
                    RateLimitStrategyFactory.create(config, timeStamp),
                    timeStamp
                );
                cleanUpQueue.add(
                    new IdleClientAndAPI(key, timeStamp + idleTimeout)
                );
            } else {
                existing.lastAccess = timeStamp;
            }
            return existing;
        });
        return entry.strategy.allow(System.nanoTime());
    }

    // Switching a pair's config resets its state: the new strategy starts fresh
    // (a token bucket starts full, a sliding window starts empty). A client can
    // observe a sudden new allowance right at the switch.
    public void setStrategyForClientAndAPI(
        long clientId,
        String api,
        RateLimitConfig config
    ) {
        var clientAndAPI = new ClientAndAPI(clientId, api);
        // Config first: if the pair is evicted and recreated in between, it picks up the new config.
        clientAndAPIConfig.put(clientAndAPI, config);
        strategyMap.compute(clientAndAPI, (key, existing) -> {
            long timeStamp = System.nanoTime();
            if (existing == null) {
                cleanUpQueue.add(
                    new IdleClientAndAPI(key, timeStamp + idleTimeout)
                );
            }
            return new Entry(
                RateLimitStrategyFactory.create(config, timeStamp),
                timeStamp
            );
        });
    }

    // Pairs already running on the old default keep it until they are evicted;
    // new or re-created pairs pick up the new default.
    public void setDefaultConfig(RateLimitConfig defaultConfig) {
        this.defaultConfig = Objects.requireNonNull(defaultConfig);
    }

    public void setIdleTimeout(long idleTimeout) {
        this.idleTimeout = idleTimeout;
    }

    public void close() {
        cleanUpThread.interrupt();
    }

    void cleanUp() {
        while (!Thread.currentThread().isInterrupted()) {
            try {
                var idle = cleanUpQueue.take();
                long now = System.nanoTime();
                long timeout = idleTimeout;

                strategyMap.computeIfPresent(
                    idle.clientAndAPI(),
                    (key, entry) -> {
                        if (now - entry.lastAccess >= timeout) {
                            return null; // idle: evict; explicit config stays in clientAndAPIConfig
                        }
                        // still active: re-arm for its real deadline
                        cleanUpQueue.add(
                            new IdleClientAndAPI(
                                key,
                                entry.lastAccess + timeout
                            )
                        );
                        return entry;
                    }
                );
            } catch (InterruptedException _) {
                return;
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Tests. Run with: java -ea Main.java  (Java 25)
//
// Time control: strategies take the timestamp as a parameter, so single-strategy
// tests drive time by hand and are fully deterministic. RateLimiter reads
// System.nanoTime itself, so its tests pick rates where no refill can happen
// during the test (1 token/min) and real sleeps only for eviction.
// ---------------------------------------------------------------------------

static final long SEC = 1_000_000_000L;

static final long MIN = 60 * SEC;

static int failures = 0;

static void check(String name, long actual, long expected) {
    if (actual == expected) {
        IO.println("PASS " + name + " = " + actual);
    } else {
        failures++;
        IO.println(
            "FAIL " + name + ": expected " + expected + ", got " + actual
        );
    }
}

static int admit(RateLimitStrategy s, long at, int n) {
    int admitted = 0;
    for (int i = 0; i < n; i++) if (s.allow(at)) admitted++;
    return admitted;
}

// ---- Token bucket, deterministic time ----

static void tokenBucketTests() {
    // Fresh bucket starts full.
    var s = new TokenBucketStrategy(new TokenBucketConfig(60, 5), 0);
    // expected: 5 (capacity 5, no time passed)
    check("tb fresh burst", admit(s, 0, 7), 5);

    // Zero tokens: the next request at the same instant is denied.
    // expected: 0
    check("tb zero tokens", admit(s, 0, 1), 0);

    // Burst after 1 s: exactly one token accrued (60/min = 1/s).
    // expected: 1
    check("tb burst after 1s", admit(s, SEC, 10), 1);

    // Request rate equals configured rate: 1/s against 60/min, starting full, for 10 min.
    s = new TokenBucketStrategy(new TokenBucketConfig(60, 5), 0);
    int admitted = 0;
    for (int i = 1; i <= 600; i++) if (s.allow(i * SEC)) admitted++;
    // expected: 600 (each request finds the token earned in the previous second)
    check("tb 1/s vs 60/min", admitted, 600);

    // Sub-unit rate: 1/min, drained, a request every 30 s for 10 min (20 requests).
    s = new TokenBucketStrategy(new TokenBucketConfig(1, 5), 0);
    admit(s, 0, 5);
    admitted = 0;
    for (int i = 1; i <= 20; i++) if (s.allow(i * 30 * SEC)) admitted++;
    // expected: 10 (one token per minute, 10 minutes)
    check("tb 1/min every 30s", admitted, 10);

    // Half a token is not a token.
    s = new TokenBucketStrategy(new TokenBucketConfig(1, 5), 0);
    admit(s, 0, 5);
    // expected: 0 (only 0.5 tokens accrued at 30 s)
    check("tb half token", admit(s, 30 * SEC, 1), 0);

    // Long idle is capped at capacity.
    s = new TokenBucketStrategy(new TokenBucketConfig(1, 5), 0);
    admit(s, 0, 5);
    // expected: 5 (60 tokens accrued, capped at 5)
    check("tb long idle cap", admit(s, 60 * MIN, 100), 5);

    // nanoTime wraps: start just below Long.MAX_VALUE, cross the wrap.
    long t0 = Long.MAX_VALUE - SEC / 2;
    s = new TokenBucketStrategy(new TokenBucketConfig(60, 5), t0);
    admit(s, t0, 5);
    // expected: 1 (1 s elapsed across the wrap -> 1 token)
    check("tb across nanoTime wrap", admit(s, t0 + SEC, 10), 1);
}

// ---- Sliding window counter, deterministic time ----

static void slidingWindowTests() {
    // Fresh window, burst at 6 s.
    var s = new SlidingWindowStrategy(new SlidingWindowConfig(1, 10), 0);
    // expected: 10 (prevCount 0, currCount fills to 10)
    check("sw fresh burst", admit(s, 6 * SEC, 100), 10);

    // Boundary approximation: 10 at 59.9 s, then a burst at 90 s.
    s = new SlidingWindowStrategy(new SlidingWindowConfig(1, 10), 0);
    admit(s, 59_900_000_000L, 10);
    // At 90 s: prev window weight = (60 - 30) / 60 = 0.5, estimate = 10 * 0.5 = 5.
    // expected: 5. Note the true count in (30 s, 90 s] becomes 15 > 10:
    // this is the counter's documented approximation, not a bug.
    check("sw boundary approximation", admit(s, 90 * SEC, 100), 5);

    // Two full windows later the previous window no longer counts.
    s = new SlidingWindowStrategy(new SlidingWindowConfig(1, 10), 0);
    admit(s, 0, 10);
    // expected: 10 (windowsPassed = 2 -> prevCount = 0)
    check("sw after two windows", admit(s, 2 * MIN, 100), 10);

    // Exactly one window later, at the start of it, the previous window weighs 1.
    s = new SlidingWindowStrategy(new SlidingWindowConfig(1, 10), 0);
    admit(s, 0, 10);
    // expected: 0 (estimate = 10 * 1.0 = 10, full)
    check("sw exactly one window later", admit(s, MIN, 100), 0);
}

// ---- Concurrency ----

// Many threads released at the same instant by a latch; returns total admits.
static int hammer(
    int threads,
    int perThread,
    IntFunction<BooleanSupplier> callFor
) throws Exception {
    var ready = new CountDownLatch(threads);
    var start = new CountDownLatch(1);
    var admitted = new AtomicInteger();
    try (var pool = Executors.newFixedThreadPool(threads)) {
        var futures = new ArrayList<Future<?>>();
        try {
            for (int t = 0; t < threads; t++) {
                var call = callFor.apply(t);
                futures.add(
                    pool.submit(() -> {
                        ready.countDown();
                        start.await();
                        for (int i = 0; i < perThread; i++) 
                            if (call.getAsBoolean()) 
                                admitted.incrementAndGet();
                        return null;
                    })
                );
            }
            ready.await();
        } finally {
            start.countDown();
        }
        for (var f : futures) f.get();
    }
    return admitted.get();
}

// Deliberately broken copy: same logic as TokenBucketStrategy, without synchronized.
// Exists only to show the concurrent test can fail.
static class UnsynchronizedTokenBucket implements RateLimitStrategy {

    long leftToken;

    UnsynchronizedTokenBucket(int max) {
        leftToken = max;
    }

    public boolean allow(long ts) {
        if (leftToken <= 0) return false;
        --leftToken;
        return true;
    }
}

static void concurrencyTests() throws Exception {
    // Same pair, token bucket default. 1 token/min so nothing refills during the test.
    try (var rl = new RateLimiter(new TokenBucketConfig(1, 100), 10 * MIN)) {
        // expected: 100 (capacity), however the 16 x 1000 calls interleave
        check(
            "concurrent same pair, token bucket",
            hammer(16, 1000, t -> () -> rl.allow(1, "/search")),
            100
        );
    }

    // Same pair, sliding window default.
    try (var rl = new RateLimiter(new SlidingWindowConfig(1, 100), 10 * MIN)) {
        // expected: 100 (fresh window, no previous window)
        check(
            "concurrent same pair, sliding window",
            hammer(16, 1000, t -> () -> rl.allow(1, "/search")),
            100
        );
    }

    // Different pairs don't share limits: 16 threads, 2 threads per client.
    try (var rl = new RateLimiter(new TokenBucketConfig(1, 100), 10 * MIN)) {
        // expected: 8 clients * 100 = 800
        check(
            "concurrent 8 pairs",
            hammer(16, 1000, t -> () -> rl.allow(t / 2, "/search")),
            800
        );
    }

    // The broken copy, to show the test catches a missing lock. Run several rounds;
    // a single round may happen to pass.
    int worst = 0;
    for (int round = 0; round < 20; round++) {
        var broken = new UnsynchronizedTokenBucket(100);
        worst = Math.max(worst, hammer(16, 1000, t -> () -> broken.allow(0)));
    }
    IO.println(
        (worst > 100 ? "SEEN TO FAIL" : "DID NOT FAIL this run") +
            ": unsynchronized bucket admitted up to " +
            worst +
            " (limit 100)"
    );
}

// ---- Eviction and runtime config ----

static void evictionTests() throws Exception {
    // Idle pairs are evicted.
    try (var rl = new RateLimiter(new TokenBucketConfig(1, 100), 50_000_000L)) {
        rl.allow(1, "/a");
        rl.allow(2, "/b");
        Thread.sleep(300);
        // expected: 0 strategies and 0 queue entries after idling past 50 ms
        check("evicted strategies", rl.strategyMap.size(), 0);
        check("evicted queue entries", rl.cleanUpQueue.size(), 0);
    }

    // Queue holds one entry per pair, not one per request.
    try (var rl = new RateLimiter(new TokenBucketConfig(1, 100), 60 * MIN)) {
        for (int i = 0; i < 10_000; i++) rl.allow(1, "/a");
        // expected: 1
        check("queue entries after 10k requests", rl.cleanUpQueue.size(), 1);
    }

    // An explicit config survives eviction of its strategy.
    try (var rl = new RateLimiter(new TokenBucketConfig(1, 100), 50_000_000L)) {
        rl.setStrategyForClientAndAPI(
            7,
            "/upload",
            new SlidingWindowConfig(1, 2)
        );
        Thread.sleep(300);
        int admitted = 0;
        for (int i = 0; i < 5; i++) if (rl.allow(7, "/upload")) admitted++;
        // expected: 2 (sliding window limit), not 5 (which the default would give)
        check("explicit config after eviction", admitted, 2);
    }

    // Switching config resets state: a drained pair gets a fresh allowance.
    try (var rl = new RateLimiter(new TokenBucketConfig(1, 3), 60 * MIN)) {
        int before = 0,
            after = 0;
        for (int i = 0; i < 5; i++) if (rl.allow(1, "/a")) before++;
        rl.setStrategyForClientAndAPI(1, "/a", new TokenBucketConfig(1, 3));
        for (int i = 0; i < 5; i++) if (rl.allow(1, "/a")) after++;
        // expected: 3 before the switch (drained), 3 after (new bucket starts full)
        check("admits before switch", before, 3);
        check("admits after switch", after, 3);
    }

    // A new default applies to new pairs; a pair already running keeps the old one.
    try (var rl = new RateLimiter(new TokenBucketConfig(1, 3), 60 * MIN)) {
        rl.allow(1, "/a");
        rl.setDefaultConfig(new TokenBucketConfig(1, 10));
        int oldPair = 0,
            newPair = 0;
        for (int i = 0; i < 20; i++) if (rl.allow(1, "/a")) oldPair++;
        for (int i = 0; i < 20; i++) if (rl.allow(2, "/a")) newPair++;
        // expected: 2 (old default, capacity 3, 1 already used), 10 (new default)
        check("old pair keeps old default", oldPair, 2);
        check("new pair gets new default", newPair, 10);
    }
}

void main() throws Exception {
    tokenBucketTests();
    slidingWindowTests();
    concurrencyTests();
    evictionTests();
    IO.println(failures == 0 ? "ALL PASSED" : failures + " FAILED");
}
