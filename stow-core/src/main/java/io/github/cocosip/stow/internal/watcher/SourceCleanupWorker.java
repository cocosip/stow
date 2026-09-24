package io.github.cocosip.stow.internal.watcher;

import io.github.cocosip.stow.api.FileWatcherManager;
import io.github.cocosip.stow.api.FileWatcherOptionsManager;
import io.github.cocosip.stow.config.SourceCleanupConfiguration;
import io.github.cocosip.stow.config.SqliteConfiguration;
import io.github.cocosip.stow.exception.StowInterruptedException;
import java.io.IOException;
import java.nio.file.Path;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class SourceCleanupWorker implements AutoCloseable {

    record CycleResult(
            int claimed,
            int completed,
            int retried,
            int quarantined,
            int discardedMismatch,
            int terminalFailed,
            int staleReleased,
            int pruned,
            int optimized) {

        static CycleResult empty() {
            return new CycleResult(0, 0, 0, 0, 0, 0, 0, 0, 0);
        }
    }

    private static final Logger LOG = LoggerFactory.getLogger(SourceCleanupWorker.class);
    private static final Duration CLAIM_LEASE = Duration.ofMinutes(5);

    private final SourceCleanupConfiguration configuration;
    private final SourceCleanupStore store;
    private final SourceCleanupGate gate;
    private final SourceFileRelocator relocator;
    private final Clock clock;
    private final Executor actionExecutor;
    private final ScheduledExecutorService scheduler;

    private Instant lastOptimizationAt;
    private ScheduledFuture<?> future;
    private boolean closing;
    private Thread runningThread;

    SourceCleanupWorker(
            SourceCleanupConfiguration configuration,
            SourceCleanupStore store,
            SourceCleanupGate gate,
            SourceFileRelocator relocator,
            Clock clock,
            Executor actionExecutor,
            ScheduledExecutorService scheduler) {
        this.configuration = Objects.requireNonNull(configuration, "configuration");
        this.store = Objects.requireNonNull(store, "store");
        this.gate = Objects.requireNonNull(gate, "gate");
        this.relocator = Objects.requireNonNull(relocator, "relocator");
        this.clock = Objects.requireNonNull(clock, "clock");
        this.actionExecutor = Objects.requireNonNull(actionExecutor, "actionExecutor");
        this.scheduler = scheduler;
        lastOptimizationAt = clock.instant();
    }

    public SourceCleanupWorker(
            SourceCleanupConfiguration configuration,
            SqliteConfiguration sqlite,
            FileWatcherOptionsManager options,
            FileWatcherManager watchers,
            Clock clock,
            Executor actionExecutor,
            ScheduledExecutorService scheduler) {
        this(
                configuration,
                new SourceCleanupStore(configuration, sqlite, clock),
                options,
                watchers,
                clock,
                actionExecutor,
                scheduler);
    }

    public SourceCleanupWorker(
            SourceCleanupConfiguration configuration,
            SourceCleanupStore store,
            FileWatcherOptionsManager options,
            FileWatcherManager watchers,
            Clock clock,
            Executor actionExecutor,
            ScheduledExecutorService scheduler) {
        this(
                configuration,
                store,
                new SourceCleanupGate(configuration, options, watchers),
                new SourceFileRelocator(),
                clock,
                actionExecutor,
                scheduler);
    }

    public synchronized void start() {
        if (future != null) return;
        if (scheduler == null) throw new IllegalStateException("A scheduler is required to start source cleanup");
        closing = false;
        future = scheduler.scheduleWithFixedDelay(
                this::runScheduledCycle, 0, configuration.pollInterval().toMillis(), TimeUnit.MILLISECONDS);
    }

    CycleResult runCycle() {
        if (!gate.enabled()) return CycleResult.empty();

        Instant now = clock.instant();
        int staleReleased = store.releaseStaleImports(
                now.minus(configuration.importReservationTimeout()), configuration.terminalPruneBatchSize());
        int pruned = store.pruneTerminal(
                now.minus(configuration.terminalRetention()), configuration.terminalPruneBatchSize());
        int optimized = optimizeIfDue(now);

        Counters counters = new Counters();
        while (!isClosing()) {
            List<SourceCleanupJob> jobs =
                    store.claimDue(clock.instant(), configuration.maxConcurrentActions(), CLAIM_LEASE);
            if (jobs.isEmpty()) break;
            counters.claimed.addAndGet(jobs.size());
            List<CompletableFuture<Void>> actions = new ArrayList<>(jobs.size());
            for (SourceCleanupJob job : jobs) {
                actions.add(CompletableFuture.runAsync(() -> process(job, counters), actionExecutor));
            }
            CompletableFuture.allOf(actions.toArray(CompletableFuture[]::new)).join();
        }
        return counters.result(staleReleased, pruned, optimized);
    }

    @Override
    public synchronized void close() {
        closing = true;
        if (future != null) future.cancel(false);
        while (runningThread != null && runningThread != Thread.currentThread()) {
            try {
                wait();
            } catch (InterruptedException exception) {
                Thread.currentThread().interrupt();
                throw new StowInterruptedException("Interrupted while stopping source cleanup", exception);
            }
        }
        future = null;
    }

    private void runScheduledCycle() {
        synchronized (this) {
            if (closing) return;
            runningThread = Thread.currentThread();
        }
        try {
            runCycle();
        } catch (RuntimeException failure) {
            LOG.error("Source cleanup cycle failed; continuing", failure);
        } finally {
            synchronized (this) {
                runningThread = null;
                notifyAll();
            }
        }
    }

    private void process(SourceCleanupJob job, Counters counters) {
        if (job.state() == SourceCleanupState.MOVE_PENDING) {
            quarantine(job, counters, "Retrying source quarantine");
            return;
        }
        try {
            SourceFileRelocator.Status status =
                    switch (job.action()) {
                        case KEEP -> SourceFileRelocator.Status.COMPLETED;
                        case DELETE -> relocator.deleteIfMatching(job.sourcePath(), job.fingerprint());
                        case MOVE ->
                            relocator
                                    .moveIfMatching(job.sourcePath(), job.moveTargetPath(), job.fingerprint())
                                    .status();
                    };
            finish(job, status, counters);
        } catch (IOException | RuntimeException failure) {
            recordFailure(job, failure, counters);
        }
    }

    private void finish(SourceCleanupJob job, SourceFileRelocator.Status status, Counters counters) {
        store.remove(job.id());
        if (status == SourceFileRelocator.Status.FINGERPRINT_MISMATCH) {
            counters.discardedMismatch.incrementAndGet();
        } else {
            counters.completed.incrementAndGet();
        }
    }

    private void recordFailure(SourceCleanupJob job, Exception failure, Counters counters) {
        int attempts = job.attemptCount() + 1;
        Instant now = clock.instant();
        if (attempts < job.maxAttempts()) {
            SourceCleanupJob retry = job.transition(
                    job.fileKey(),
                    attempts,
                    SourceCleanupState.RETRYING,
                    now.plus(retryDelay(job, attempts)),
                    error(failure),
                    now,
                    job.moveTargetPath());
            store.update(retry);
            counters.retried.incrementAndGet();
            return;
        }
        if (job.failureDirectory() != null) {
            quarantine(
                    job.transition(
                            job.fileKey(),
                            attempts,
                            SourceCleanupState.MOVE_PENDING,
                            null,
                            error(failure),
                            now,
                            job.moveTargetPath()),
                    counters,
                    error(failure));
            return;
        }
        terminalFailure(job, attempts, error(failure), counters);
    }

    private void quarantine(SourceCleanupJob job, Counters counters, String previousError) {
        if (job.failureDirectory() == null) {
            terminalFailure(job, Math.max(1, job.attemptCount()), previousError, counters);
            return;
        }
        Path target = job.failureDirectory()
                .resolve(job.watcherId())
                .resolve(job.sourcePath().getFileName());
        try {
            SourceFileRelocator.Status status = relocator
                    .moveIfMatching(job.sourcePath(), target, job.fingerprint())
                    .status();
            store.remove(job.id());
            if (status == SourceFileRelocator.Status.FINGERPRINT_MISMATCH) {
                counters.discardedMismatch.incrementAndGet();
            } else if (status == SourceFileRelocator.Status.COMPLETED) {
                counters.quarantined.incrementAndGet();
            } else {
                counters.completed.incrementAndGet();
            }
        } catch (IOException | RuntimeException failure) {
            terminalFailure(job, Math.max(1, job.attemptCount()), error(failure), counters);
        }
    }

    private void terminalFailure(SourceCleanupJob job, int attempts, String error, Counters counters) {
        Instant now = clock.instant();
        store.update(job.transition(
                job.fileKey(), attempts, SourceCleanupState.TERMINAL_FAILED, null, error, now, job.moveTargetPath()));
        counters.terminalFailed.incrementAndGet();
    }

    private int optimizeIfDue(Instant now) {
        if (!configuration.databaseOptimizationEnabled()
                || now.isBefore(lastOptimizationAt.plus(configuration.databaseOptimizationInterval()))) {
            return 0;
        }
        store.optimize();
        lastOptimizationAt = now;
        return 1;
    }

    private synchronized boolean isClosing() {
        return closing;
    }

    private static Duration retryDelay(SourceCleanupJob job, int attempts) {
        Duration delay = job.retryInitialDelay();
        int doublings = Math.min(Math.max(0, attempts - 1), 30);
        for (int index = 0; index < doublings && delay.compareTo(job.retryMaxDelay()) < 0; index++) {
            try {
                delay = delay.multipliedBy(2);
            } catch (ArithmeticException ignored) {
                return job.retryMaxDelay();
            }
        }
        return delay.compareTo(job.retryMaxDelay()) > 0 ? job.retryMaxDelay() : delay;
    }

    private static String error(Exception failure) {
        String value = failure.getMessage();
        if (value == null || value.isBlank()) value = failure.getClass().getSimpleName();
        return value.length() <= 2048 ? value : value.substring(0, 2048);
    }

    private static final class Counters {
        private final AtomicInteger claimed = new AtomicInteger();
        private final AtomicInteger completed = new AtomicInteger();
        private final AtomicInteger retried = new AtomicInteger();
        private final AtomicInteger quarantined = new AtomicInteger();
        private final AtomicInteger discardedMismatch = new AtomicInteger();
        private final AtomicInteger terminalFailed = new AtomicInteger();

        private CycleResult result(int staleReleased, int pruned, int optimized) {
            return new CycleResult(
                    claimed.get(),
                    completed.get(),
                    retried.get(),
                    quarantined.get(),
                    discardedMismatch.get(),
                    terminalFailed.get(),
                    staleReleased,
                    pruned,
                    optimized);
        }
    }
}
