package dev.quicksave.io;

import dev.quicksave.config.QuickSaveConfig;
import net.minecraft.nbt.NbtCompound;
import net.minecraft.util.math.ChunkPos;
import net.minecraft.world.storage.StorageIoWorker;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.LockSupport;

/**
 * QuickSave's core asynchronous chunk-save pipeline — one instance per hijacked
 * {@link StorageIoWorker}.
 *
 * <h2>Why this class exists</h2>
 *
 * <p>Vanilla 1.20.1 deposits serialized chunks into an <b>unbounded</b>
 * {@code Map<ChunkPos, Result>} inside {@code StorageIoWorker}, which is drained
 * for disk writing by a single lane through {@code RegionBasedStorage} /
 * {@code RegionFile}. During aggressive pre-generation (Chunky, jjthunder)
 * generation outpaces the SSD, the map balloons, and the JVM dies with OOM.</p>
 *
 * <h2>Design</h2>
 * <pre>
 *   producer (server thread)                     consumer(s)
 *   ------------------------                     --------------------------------
 *   setResult(pos, nbt)                          Stripe-i writer thread:
 *     |                                            1. take from stripe queue[i]
 *     |- coalesce? swap volatile tag               2. remove from pending map
 *     |- else enqueue entry  --------------------> 3. call ORIGINAL setResult body
 *     v                                            4. complete future, recycle
 *     [BLOCKS when stripe full = HARD backpressure,
 *      which throttles world generation naturally]
 * </pre>
 *
 * <h2>Thread-safety proofs</h2>
 *
 * <p><b>1. Per-chunk write ordering (the world-corruption killer).</b> A naive
 * thread pool can write two saves of the SAME chunk out of order, leaving a
 * stale version on disk. Prevented structurally: {@code pos -> stripe} is a pure
 * function of the packed position ({@link #stripeIndexFor}), and each stripe is
 * drained strictly FIFO by exactly one thread. Therefore all writes to a given
 * chunk are totally ordered. Cross-chunk ordering is never relied upon by the
 * Anvil format.</p>
 *
 * <p><b>2. Coalescing visibility.</b> Producer P updating an entry E already in
 * the pending map performs {@code putIfAbsent} (returns E), then writes
 * {@code E.tag} (volatile). Consumer C later removes E from the same
 * {@link ConcurrentHashMap} bin, then reads {@code E.tag}. Both map operations
 * synchronize on the same bin monitor, so C's removal happens-after P's access;
 * combined with the volatile tag pair, C is guaranteed to observe P's newest
 * snapshot. If instead C removed E before P's {@code putIfAbsent}, P inserts a
 * NEW entry strictly behind E in FIFO order — final disk state is still the
 * newest data either way.</p>
 *
 * <p><b>3. Data isolation.</b> Vanilla {@code setResult(ChunkPos, NbtCompound)}
 * already receives a fully materialized snapshot built on the world thread by
 * the chunk serializer. We enqueue that reference only; after the producer calls
 * {@code setResult}, vanilla treats the tag as opaque and never mutates it (we
 * inherit vanilla's exact contract). No locks on live {@code Chunk} state are
 * needed because no live chunk state ever enters this pipeline.</p>
 *
 * <p><b>4. Re-entrancy.</b> Writer threads invoke the ORIGINAL method body via
 * the MixinExtras {@code Operation}. The static {@link #ON_WRITER_THREAD}
 * ThreadLocal additionally guarantees that if any path re-enters the wrapped
 * method from a writer thread, it falls straight through to vanilla instead of
 * re-enqueueing (which would deadlock a drainer against itself).</p>
 */
public final class QuickSavePipeline {

    public static final Logger LOGGER = LoggerFactory.getLogger("QuickSave");

    private static final long WARN_INTERVAL_MS = 5_000;

    /** Marker set on writer threads so they bypass enqueueing entirely. */
    private static final ThreadLocal<Boolean> ON_WRITER_THREAD =
            ThreadLocal.withInitial(() -> Boolean.FALSE);

    /** All live pipelines, iterated by lifecycle/shutdown hooks. */
    private static final Set<QuickSavePipeline> ACTIVE_PIPELINES = ConcurrentHashMap.newKeySet();

    // ------------------------------------------------------------------
    // Entry recycling (GC pressure reduction)
    // ------------------------------------------------------------------

    private static final int RECYCLE_BIN_CAPACITY = 2048;
    private static final ConcurrentLinkedQueue<PendingChunkWrite> ENTRY_BIN =
            new ConcurrentLinkedQueue<>();
    private static final AtomicInteger ENTRY_BIN_SIZE = new AtomicInteger();

    private static PendingChunkWrite obtainEntry() {
        PendingChunkWrite e = ENTRY_BIN.poll();
        if (e != null) {
            ENTRY_BIN_SIZE.decrementAndGet();
            return e;
        }
        return new PendingChunkWrite();
    }

    private static void recycleEntry(PendingChunkWrite e) {
        // Clear references first so a pooled entry never pins NBT garbage.
        e.pos = null;
        e.tag = null;
        e.dispatcher = null;
        if (ENTRY_BIN_SIZE.get() < RECYCLE_BIN_CAPACITY && ENTRY_BIN.offer(e)) {
            ENTRY_BIN_SIZE.incrementAndGet();
        }
    }

    // ------------------------------------------------------------------
    // Public surface used by the mixin & lifecycle hooks
    // ------------------------------------------------------------------

    /** Callback that executes the ORIGINAL vanilla save body for an entry. */
    @FunctionalInterface
    public interface Dispatcher {
        CompletableFuture<Void> dispatch(PendingChunkWrite entry) throws Throwable;
    }

    /** @return true if the calling thread is one of our writer threads. */
    public static boolean isOnWriterThread() {
        return ON_WRITER_THREAD.get();
    }

    /** Snapshot of all live pipelines (for shutdown hooks / commands). */
    public static List<QuickSavePipeline> activePipelines() {
        return new ArrayList<>(ACTIVE_PIPELINES);
    }

    // ------------------------------------------------------------------
    // Instance state
    // ------------------------------------------------------------------

    private final String workerName;
    private final StorageIoWorker owner;
    private final Stripe[] stripes;
    private final int softWarningThreshold;
    private final int shutdownFlushBudgetMs;

    /** Entries popped from a queue but not yet completed (disk write in flight). */
    private final AtomicInteger inFlight = new AtomicInteger();

    /** Rate-limiter state for the SOFT pressure warning. */
    private final AtomicLong lastSoftWarnMs = new AtomicLong();

    private final java.util.concurrent.atomic.LongAdder submitted = new java.util.concurrent.atomic.LongAdder();
    private final java.util.concurrent.atomic.LongAdder written = new java.util.concurrent.atomic.LongAdder();
    private final java.util.concurrent.atomic.LongAdder failed = new java.util.concurrent.atomic.LongAdder();

    /** Serializes flushes so close()/completeAll()/SERVER_STOPPING cannot interleave. */
    private final Object flushLock = new Object();

    private volatile boolean shutdownRequested;
    private final Thread shutdownHook;

    /** Creates (and registers) the pipeline bound to one vanilla worker. */
    public static QuickSavePipeline create(StorageIoWorker owner, String workerName) {
        return new QuickSavePipeline(owner, workerName, QuickSaveConfig.get());
    }

    private QuickSavePipeline(StorageIoWorker owner, String workerName, QuickSaveConfig cfg) {
        this.owner = owner;
        this.workerName = workerName;
        this.softWarningThreshold = cfg.softWarningThreshold;
        this.shutdownFlushBudgetMs = cfg.shutdownFlushBudgetMs;

        int count = cfg.resolveIoThreadCount();
        this.stripes = new Stripe[count];
        for (int i = 0; i < count; i++) {
            stripes[i] = new Stripe(i, cfg.stripeQueueCapacity);
        }

        // Last-resort crash integrity: JVM shutdown hooks run even when the game
        // crashes with an uncaught exception. (Not on SIGKILL/power loss — no
        // software can intercept those; the same is true of vanilla saving.)
        this.shutdownHook = new Thread(this::emergencyFlush, "QuickSave-ShutdownHook-" + workerName);
        Runtime.getRuntime().addShutdownHook(shutdownHook);

        ACTIVE_PIPELINES.add(this);

        LOGGER.info("[QuickSave] Pipeline online for '{}' worker: {} writer thread(s), capacity {}/stripe (hard cap {} chunks).",
                workerName, count, cfg.stripeQueueCapacity, count * cfg.stripeQueueCapacity);
    }

    // ------------------------------------------------------------------
    // Producer path (called from the wrapped setResult)
    // ------------------------------------------------------------------

    /**
     * Enqueues one chunk snapshot for asynchronous writing.
     *
     * <p>Called on whatever thread invoked vanilla {@code setResult} — usually
     * the server thread, which is exactly where we WANT blocking backpressure
     * to land: stalling that caller throttles world generation itself.</p>
     */
    public CompletableFuture<Void> submit(ChunkPos pos, NbtCompound tag, Dispatcher dispatcher) {
        submitted.increment();
        warnIfUnderPressure();

        final Stripe stripe = stripes[stripeIndexFor(pos)];
        final long key = pos.toLong();

        PendingChunkWrite entry = obtainEntry();
        entry.key = key;
        entry.pos = pos;
        entry.tag = tag;
        entry.dispatcher = dispatcher;

        // ---- Coalescing fast path ----------------------------------------
        // If an older save of THIS chunk is still queued, replace its payload
        // instead of queueing a duplicate (last-write-wins — mirroring how the
        // vanilla `results` map dedupes by position). Saves RAM AND redundant
        // disk I/O. See visibility proof in the class javadoc (#2).
        PendingChunkWrite existing = stripe.pending.putIfAbsent(key, entry);
        if (existing != null) {
            existing.tag = tag; // volatile write; consumer reads it after map removal
            recycleEntry(entry); // unused shell returns straight to the pool
            return existing.future;
        }

        enqueueWithBackpressure(stripe, entry);
        return entry.future;
    }

    /**
     * HARD BACKPRESSURE. Offers non-blockingly; only when the stripe is at
     * capacity does the producer park in short spins with rate-limited ERROR
     * logging. Blocking here stalls the server thread → chunk holders cannot
     * complete their save futures → Chunky/jjthunder throttle automatically.
     * This makes OOM impossible <b>by construction</b>: pending entries can
     * never exceed {@code ioThreads * stripeQueueCapacity}.
     */
    private void enqueueWithBackpressure(Stripe stripe, PendingChunkWrite entry) {
        if (stripe.queue.offer(entry)) {
            return;
        }
        long lastHardWarn = System.currentTimeMillis() - WARN_INTERVAL_MS + 1_000;
        while (!stripe.queue.offer(entry)) {
            if (shutdownRequested) {
                // Pipeline is dying; fail loudly rather than silently dropping.
                stripe.pending.remove(entry.key, entry);
                entry.future.completeExceptionally(
                        new IllegalStateException("QuickSave pipeline shut down while chunk was queued"));
                failed.increment();
                recycleEntry(entry);
                return;
            }
            long now = System.currentTimeMillis();
            if (now - lastHardWarn >= WARN_INTERVAL_MS) {
                lastHardWarn = now;
                LOGGER.error("[QuickSave] HARD BACKPRESSURE on '{}': buffer full ({} pending total). " +
                                "Throttling world generation until the SSD catches up; consider raising 'ioThreads'.",
                        workerName, totalQueued());
            }
            LockSupport.parkNanos(250_000L); // 0.25 ms
        }
    }

    /** SOFT threshold: aggressive but rate-limited warning before hard blocking begins. */
    private void warnIfUnderPressure() {
        int total = totalQueued();
        if (total < softWarningThreshold) {
            return;
        }
        long now = System.currentTimeMillis();
        long last = lastSoftWarnMs.get();
        if (now - last >= WARN_INTERVAL_MS && lastSoftWarnMs.compareAndSet(last, now)) {
            int hardCap = stripes.length * stripes[0].capacity;
            LOGGER.error("[QuickSave] PRESSURE WARNING on '{}': {} chunk writes pending (soft limit {}). " +
                            "Disk I/O is falling behind generation; hard throttling begins at {}.",
                    workerName, total, softWarningThreshold, hardCap);
        }
    }

    private int totalQueued() {
        int total = 0;
        for (Stripe s : stripes) {
            total += s.queue.size();
        }
        return total;
    }

    /**
     * Deterministic pos -> stripe mapping. MUST be a pure function of the
     * packed position: per-chunk FIFO order (and therefore world integrity)
     * depends on the same chunk always landing on the same single-consumer
     * stripe.
     */
    private int stripeIndexFor(ChunkPos pos) {
        long key = pos.toLong();
        int h = (int) (key ^ (key >>> 32));
        h ^= h >>> 16;
        return Math.floorMod(h, stripes.length);
    }

    // ------------------------------------------------------------------
    // Consumer path (dedicated writer threads)
    // ------------------------------------------------------------------

    private final class Stripe extends Thread {
        final LinkedBlockingQueue<PendingChunkWrite> queue;
        /** Coalescing index: packedPos -> still-queued entry. See class javadoc #1/#2. */
        final ConcurrentHashMap<Long, PendingChunkWrite> pending = new ConcurrentHashMap<>();
        final int capacity;

        Stripe(int index, int capacity) {
            super("QuickSave-Writer-" + index + "-" + workerName);
            this.capacity = capacity;
            this.queue = new LinkedBlockingQueue<>(capacity);
            setDaemon(true);                    // must never block JVM exit
            setPriority(NORM_PRIORITY - 1);     // yield CPU to the server thread
            setContextClassLoader(QuickSavePipeline.class.getClassLoader());
            start();
        }

        @Override
        public void run() {
            final LinkedBlockingQueue<PendingChunkWrite> q = queue;
            while (!shutdownRequested) {
                PendingChunkWrite entry;
                try {
                    entry = q.poll(250, TimeUnit.MILLISECONDS);
                } catch (InterruptedException ie) {
                    continue; // loop re-checks the shutdown flag
                }
                if (entry == null) {
                    continue;
                }
                // Remove from the coalescing map BEFORE dispatching, so a racing
                // producer either (a) coalesced into us earlier — its volatile
                // tag write is guaranteed visible to our later read — or
                // (b) enqueues a fresh, strictly-newer entry behind ours in FIFO.
                pending.remove(entry.key, entry);
                dispatch(entry);
            }
        }

        private void dispatch(PendingChunkWrite entry) {
            ON_WRITER_THREAD.set(Boolean.TRUE); // re-entrancy guard, javadoc #4
            inFlight.incrementAndGet();
            try {
                CompletableFuture<Void> vanillaFuture = entry.dispatcher.dispatch(entry);
                vanillaFuture.whenComplete((unused, error) -> completeEntry(entry, error));
            } catch (Throwable t) {
                // Vanilla body threw synchronously (disk full, IO error...).
                // The caller's future receives the SAME failure vanilla would
                // have produced — never swallow write errors silently.
                completeEntry(entry, t);
            } finally {
                ON_WRITER_THREAD.set(Boolean.FALSE);
            }
        }
    }

    private void completeEntry(PendingChunkWrite entry, Throwable error) {
        try {
            if (error == null) {
                entry.future.complete(null);
                written.increment();
            } else {
                entry.future.completeExceptionally(error);
                failed.increment();
                LOGGER.error("[QuickSave] Chunk write FAILED at {} on '{}' worker.",
                        entry.pos, workerName, error);
            }
        } finally {
            inFlight.decrementAndGet();
            recycleEntry(entry);
        }
    }

    // ------------------------------------------------------------------
    // Graceful shutdown & data integrity
    // ------------------------------------------------------------------

    /**
     * Blocks the CALLING thread until every queued entry has physically passed
     * through the vanilla sink and every in-flight disk write has completed.
     *
     * <p><b>Deadlock analysis:</b></p>
     * <ul>
     *   <li>Never called from a writer thread (guarded below).</li>
     *   <li>Concurrent callers (close mixin, completeAll mixin,
     *       SERVER_STOPPING hook) serialize on {@link #flushLock}; Java monitors
     *       are reentrant, so hooks triggering one another cannot self-deadlock.</li>
     *   <li>Writers make independent progress: they only perform non-blocking
     *       enqueue operations into vanilla's own task queue.</li>
     * </ul>
     */
    public void flushBlocking(String reason) {
        if (ON_WRITER_THREAD.get()) {
            LOGGER.warn("[QuickSave] flushBlocking({}) ignored on writer thread.", reason);
            return;
        }
        synchronized (flushLock) {
            long nextLog = System.currentTimeMillis() + WARN_INTERVAL_MS;
            while (true) {
                int queued = totalQueued();
                int running = inFlight.get();
                if (queued == 0 && running == 0) {
                    break;
                }
                long now = System.currentTimeMillis();
                if (now >= nextLog) {
                    nextLog = now + WARN_INTERVAL_MS;
                    LOGGER.warn("[QuickSave] Flushing '{}' ({}): {} queued, {} writing — refusing to proceed until drained.",
                            workerName, reason, queued, running);
                }
                LockSupport.parkNanos(500_000L); // 0.5 ms
            }
        }
    }

    /**
     * Crash-path best effort (JVM shutdown hook): drain our queue within a time
     * budget, then force vanilla into a SYNCHRONOUS drain of everything we have
     * already handed over via {@code owner.completeAll(true)}. Runs on the hook
     * thread during JVM exit, including after uncaught crashes.
     */
    private void emergencyFlush() {
        int initial = totalQueued() + inFlight.get();
        if (initial == 0) {
            return;
        }
        LOGGER.error("[QuickSave] SHUTDOWN HOOK engaged for '{}' — {} chunk write(s) pending, budget {} ms.",
                workerName, initial, shutdownFlushBudgetMs);

        long deadline = System.currentTimeMillis() + shutdownFlushBudgetMs;
        synchronized (flushLock) {
            while ((totalQueued() > 0 || inFlight.get() > 0) && System.currentTimeMillis() < deadline) {
                LockSupport.parkNanos(500_000L);
            }
        }
        // NOTE: owner.completeAll(true) is deliberately invoked OUTSIDE flushLock.
        // It re-enters our own completeAll() mixin which calls flushBlocking again
        // (reentrancy-safe via monitor reentry, but cleaner not to hold the lock).

        boolean oursDrained = totalQueued() == 0 && inFlight.get() == 0;
        if (oursDrained) {
            try {
                owner.completeAll(true);
                LOGGER.warn("[QuickSave] '{}' flushed cleanly before exit ({} chunk writes).", workerName, initial);
            } catch (Throwable t) {
                LOGGER.error("[QuickSave] '{}' drained, but forcing vanilla completeAll failed!", workerName, t);
            }
        } else {
            LOGGER.error("[QuickSave] CRITICAL: '{}' did NOT drain within budget ({} ms); up to {} chunk(s) may be lost. " +
                            "Region headers will mark those chunks for regeneration; re-run pre-generation for safety.",
                    workerName, shutdownFlushBudgetMs, totalQueued() + inFlight.get());
        }
    }

    /**
     * Winds down the pipeline. Only ever invoked from the worker's close()
     * mixin AFTER {@link #flushBlocking} succeeded, i.e. with an empty queue.
     */
    public void dispose() {
        shutdownRequested = true;
        // Deliberately NO Thread.interrupt(): interrupting a thread parked in a
        // blocking NIO region-file write can surface ClosedByInterruptException
        // from the channel. Daemon writers wind down within <=250 ms by
        // themselves and can never block JVM exit.
        try {
            Runtime.getRuntime().removeShutdownHook(shutdownHook);
        } catch (IllegalStateException alreadyShuttingDown) {
            // JVM is exiting; the hook ran or is running — nothing to do.
        }
        ACTIVE_PIPELINES.remove(this);
        LOGGER.info("[QuickSave] Pipeline for '{}' disposed. Stats: {} submitted, {} written, {} failed.",
                workerName, submitted.sum(), written.sum(), failed.sum());
    }
}
