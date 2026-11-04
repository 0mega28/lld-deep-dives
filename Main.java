import module java.base;

/*
 * MVCC key-value store.
 *
 * - Every write appends a new immutable Version; nothing is updated in place.
 * - Each Version points at the Txn that wrote it. A Txn flips PENDING -> COMMITTED
 *   in one atomic step, and that single flip makes all of its versions visible at
 *   once. This is the "transaction complete" marker; it lives on the Txn object
 *   instead of in a global completed-set.
 * - Readers take no locks. Writers only contend with each other on the tiny commit
 *   step and on per-key chain installs.
 */
class MvccStore implements AutoCloseable {

    static final long PENDING = 0;
    static final long ABORTED = -1;

    static final class Txn {
        final long id;
        // One field holds both "is it done" and "in what order": PENDING, ABORTED,
        // or the commit timestamp (> 0). Commit and abort both CAS from PENDING, so
        // exactly one wins (the in-process version of fencing).
        final AtomicLong status = new AtomicLong(PENDING);

        Txn(long id) { this.id = id; }

        long commitTs() { return status.get(); }
        boolean committed() { return status.get() > 0; }
        boolean aborted() { return status.get() == ABORTED; }
    }

    // Immutable. value == null is a tombstone; deadlineNs == null means no TTL.
    record Version(Txn txn, String value, Long deadlineNs) {
        boolean expired(long nowNs) {
            // nanoTime values are compared by subtraction (they can wrap).
            // Same boundary (>= 0) as Expiry.getDelay, so reads and cleanup agree.
            return deadlineNs != null && nowNs - deadlineNs >= 0;
        }
    }

    record Expiry(String key, long deadlineNs) implements Delayed {
        public long getDelay(TimeUnit unit) {
            return unit.convert(deadlineNs - System.nanoTime(), TimeUnit.NANOSECONDS);
        }
        public int compareTo(Delayed other) {
            return Long.signum(deadlineNs - ((Expiry) other).deadlineNs);
        }
    }

    // key -> version chain, ordered by txn id (the order readers scan in).
    private final ConcurrentHashMap<String, ConcurrentSkipListMap<Long, Version>> store = new ConcurrentHashMap<>();
    private final DelayQueue<Expiry> expiryQueue = new DelayQueue<>();
    private final AtomicLong txnIds = new AtomicLong();

    // Guards lastCommitTs. Held only to allocate a timestamp and flip one CAS, so
    // it is a few instructions long. Its job: commit timestamps become visible in
    // increasing order, so no reader can see ts 11 committed while ts 10 is still
    // about to become committed.
    private final ReentrantLock commitLock = new ReentrantLock();
    private long lastCommitTs = 0;

    private final Thread cleaner;

    MvccStore() {
        cleaner = new Thread(this::expiryLoop, "mvcc-expiry");
        cleaner.setDaemon(true);
        cleaner.start();
    }

    // ---------------------------------------------------------------- API

    void put(String key, String value) {
        write(Map.of(key, value), Set.of(), null);
    }

    void put(String key, String value, long ttlMs) {
        if (ttlMs <= 0) throw new IllegalArgumentException("ttlMs must be > 0, got " + ttlMs);
        write(Map.of(key, value), Set.of(), TimeUnit.MILLISECONDS.toNanos(ttlMs));
    }

    // Returns whether the key was visible just before the delete. The check and the
    // delete are not one atomic step; a concurrent writer can slip in between.
    boolean delete(String key) {
        boolean existed = get(key).isPresent();
        write(Map.of(), Set.of(key), null);
        return existed;
    }

    // Decisions: batch-written keys get no TTL; a key in both puts and deletes is rejected.
    void writeBatch(Map<String, String> puts, Set<String> deletes) {
        puts = puts == null ? Map.of() : puts;
        deletes = deletes == null ? Set.of() : deletes;
        for (var key : deletes) {
            if (puts.containsKey(key)) throw new IllegalArgumentException("key in both puts and deletes: " + key);
        }
        write(puts, deletes, null);
    }

    Optional<String> get(String key) {
        var chain = store.get(key);
        if (chain == null) return Optional.empty();

        // Upward scan (ascending txn id): pick the committed version with the
        // highest commit timestamp. Pending (0) and aborted (-1) never beat bestTs.
        // The iterator is weakly consistent: missing a version that commits mid-scan
        // is fine, the get linearizes just before that commit.
        Version best = null;
        long bestTs = 0;
        for (var v : chain.values()) {
            long ts = v.txn().commitTs();
            if (ts > bestTs) { best = v; bestTs = ts; }
        }

        // A tombstone or an expired newest version means the key is absent. We must
        // NOT fall back to an older version, or an expired key would "resurrect".
        if (best == null || best.value() == null || best.expired(System.nanoTime())) {
            return Optional.empty();
        }
        return Optional.of(best.value());
    }

    public void close() {
        cleaner.interrupt();
    }

    // ---------------------------------------------------------------- write path

    private void write(Map<String, String> puts, Set<String> deletes, Long ttlNs) {
        var txn = new Txn(txnIds.incrementAndGet());
        Long deadline = ttlNs == null ? null : System.nanoTime() + ttlNs;
        var touched = new ArrayList<String>();

        try {
            // Install phase: versions are in the chains but invisible (txn PENDING).
            puts.forEach((key, value) -> {
                touched.add(key);   // before install, so a failure mid-install is still cleaned up
                install(key, new Version(txn, value, deadline));
            });
            for (var key : deletes) {
                touched.add(key);
                install(key, new Version(txn, null, null));
            }
            commit(txn);
        } finally {
            // Abort on any failure. Marking ABORTED first makes every version of this
            // txn invisible at once; prune() below then removes them physically.
            if (!txn.committed()) txn.status.compareAndSet(PENDING, ABORTED);
            touched.forEach(this::prune);
        }

        if (deadline != null) {
            for (var key : puts.keySet()) expiryQueue.offer(new Expiry(key, deadline));
        }
    }

    private void install(String key, Version version) {
        // Inside compute so a concurrent prune() can't drop the chain from the map
        // between us fetching it and inserting into it (a write into an orphaned
        // chain would be silently lost).
        store.compute(key, (k, chain) -> {
            if (chain == null) chain = new ConcurrentSkipListMap<>();
            chain.put(version.txn().id, version);
            return chain;
        });
    }

    private void commit(Txn txn) {
        commitLock.lock();
        try {
            long ts = ++lastCommitTs;
            // Happens-before: the chain inserts above happen-before this CAS (a
            // volatile write). A reader that reads status and sees ts therefore
            // sees every version this txn installed.
            if (!txn.status.compareAndSet(PENDING, ts)) {
                throw new IllegalStateException("txn " + txn.id + " was aborted before commit");
            }
        } finally {
            commitLock.unlock();
        }
    }

    // ---------------------------------------------------------------- GC

    // Removes garbage from one key's chain. Called by the writer that created the
    // garbage and by the expiry thread, so there's no full-store scan anywhere.
    private void prune(String key) {
        store.computeIfPresent(key, (k, chain) -> {
            // Walk from the highest txn id down. A committed version can be dropped
            // only if some version to its RIGHT (higher id, so later in a reader's
            // scan) is committed with a HIGHER commit ts. Higher ts alone is not
            // enough: ids and commit order can disagree, and an in-flight reader
            // that already passed the newer version would find nothing.
            long maxTsToRight = 0;
            var it = chain.descendingMap().values().iterator();
            while (it.hasNext()) {
                var v = it.next();
                long ts = v.txn().commitTs();
                if (ts == ABORTED) { it.remove(); continue; }   // never visible, pure garbage
                if (ts == PENDING) continue;                    // still in flight, keep
                if (ts < maxTsToRight) it.remove();             // dominated, no reader can need it
                else maxTsToRight = ts;
            }

            // The loop never removes the newest committed version, even a tombstone or an
            // expired one: dropping it would make an older version visible again. Once it
            // is the only version left (nothing older to resurface, nothing pending), the
            // key is simply absent, so drop the whole chain.
            if (chain.size() == 1) {
                var only = chain.firstEntry().getValue();
                if (only.txn().committed() && (only.value() == null || only.expired(System.nanoTime()))) {
                    return null;
                }
            }
            return chain.isEmpty() ? null : chain;
        });
    }

    private void expiryLoop() {
        while (!Thread.currentThread().isInterrupted()) {
            try {
                // Stale entries (key rewritten since) are harmless: prune finds
                // nothing to drop. Lazy deletion, same as the single-version store.
                prune(expiryQueue.take().key());
            } catch (InterruptedException ex) {
                return;   // close() was called
            }
        }
    }

    int chainLength(String key) {   // for the demo only
        var chain = store.get(key);
        return chain == null ? 0 : chain.size();
    }
}

void sleep(long ms) {
    try {
        Thread.sleep(ms);
    } catch (InterruptedException ex) {
        Thread.currentThread().interrupt();
    }
}

void main() throws Exception {
    try (var store = new MvccStore()) {
        // --- basics
        store.put("k", "v1");
        IO.println(store.get("k"));                 // Optional[v1]
        store.put("k", "v2");
        IO.println(store.get("k"));                 // Optional[v2]
        IO.println(store.chainLength("k"));         // 1  (v1 pruned by the writer)
        IO.println(store.delete("k"));              // true
        IO.println(store.get("k"));                 // Optional.empty
        IO.println(store.chainLength("k"));         // 0  (tombstone alone -> chain dropped)
        IO.println(store.delete("k"));              // false

        // --- TTL: well clear of the boundary in both directions
        store.put("t", "temp", 200);
        IO.println(store.get("t"));                 // Optional[temp]
        sleep(400);
        IO.println(store.get("t"));                 // Optional.empty
        IO.println(store.chainLength("t"));         // 0  (expiry thread pruned it)

        // --- TTL key overwritten by a plain put must outlive the old deadline
        store.put("p", "short", 100);
        store.put("p", "forever");
        sleep(300);
        IO.println(store.get("p"));                 // Optional[forever]

        // --- validation
        try { store.put("x", "y", 0); } catch (IllegalArgumentException e) { IO.println("rejected ttl 0"); }
        try { store.writeBatch(Map.of("a", "1"), Set.of("a")); } catch (IllegalArgumentException e) { IO.println("rejected overlap"); }

        // --- aborted batch: fails after installing X, so X must never become visible
        var poisoned = new LinkedHashMap<String, String>() {
            @Override
            public void forEach(BiConsumer<? super String, ? super String> action) {
                action.accept("X", "partial");
                throw new RuntimeException("boom");
            }
        };
        try { store.writeBatch(poisoned, null); } catch (RuntimeException e) { IO.println("batch failed: " + e.getMessage()); }
        IO.println(store.get("X"));                 // Optional.empty
        IO.println(store.chainLength("X"));         // 0  (aborted version removed)

        // --- atomic batches under concurrency
        // One writer: batch k writes A = 2k-1, B = 2k. A reader reads A, then B.
        // B comes from the same or a later batch, so B > A always holds if batches
        // are atomic. LinkedHashMap pins A to be written first, which is the order
        // this check can catch if atomicity were broken.
        int batches = 200_000, readers = 3;
        var done = new AtomicBoolean(false);
        var checks = new LongAdder();
        // try-with-resources: if a reader fails, close() still shuts the pool down.
        // A bare pool would leave non-daemon threads alive and hang the JVM.
        try (var pool = Executors.newFixedThreadPool(readers + 1)) {
            var writer = pool.submit(() -> {
                for (int k = 1; k <= batches; ++k) {
                    var puts = new LinkedHashMap<String, String>();
                    puts.put("A", Integer.toString(2 * k - 1));
                    puts.put("B", Integer.toString(2 * k));
                    store.writeBatch(puts, null);
                }
                done.set(true);
            });

            var readerTasks = new ArrayList<Future<?>>();
            for (int r = 0; r < readers; ++r) {
                readerTasks.add(pool.submit(() -> {
                    while (!done.get()) {
                        var a = store.get("A");
                        var b = store.get("B");
                        if (a.isEmpty() || b.isEmpty()) continue;   // before the first batch
                        if (Integer.parseInt(b.get()) <= Integer.parseInt(a.get())) {
                            throw new IllegalStateException("torn batch: A=" + a.get() + " B=" + b.get());
                        }
                        checks.increment();
                    }
                }));
            }

            writer.get();
            for (var f : readerTasks) f.get();   // rethrows a reader's failure
        }

        IO.println("checks: " + checks.sum());                                       // > 0
        IO.println(store.get("A") + " " + store.get("B"));                            // Optional[399999] Optional[400000]
        IO.println("chain lengths: " + store.chainLength("A") + ", " + store.chainLength("B"));  // 1, 1
    }
}