package io.github.cocosip.stow.internal.journal;

import io.github.cocosip.stow.config.JournalAckMode;
import io.github.cocosip.stow.config.JournalConfiguration;
import io.github.cocosip.stow.exception.DatabaseRecoveryException;
import io.github.cocosip.stow.exception.StowInterruptedException;
import io.github.cocosip.stow.model.QueueEventRecord;
import io.github.cocosip.stow.spi.JournalCodec;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

public final class TenantJournalWriter implements AutoCloseable {

    private final String tenantId;
    private final Path log;
    private final JournalCodec codec;
    private final JournalConfiguration configuration;
    private final JournalStateStore stateStore;
    private final ArrayBlockingQueue<Pending> queue;
    private final Consumer<WriteResult> resultConsumer;
    private final Runnable stateFlusher;
    private final FileChannel channel;
    private final AtomicBoolean accepting = new AtomicBoolean(true);
    private final AtomicInteger queuedRecords = new AtomicInteger();
    private final Thread worker;
    private volatile Throwable failure;
    private final java.util.concurrent.atomic.LongAdder appendBatchCount = new java.util.concurrent.atomic.LongAdder();
    private final java.util.concurrent.atomic.LongAdder singleRecordAppendBatches =
            new java.util.concurrent.atomic.LongAdder();
    private final java.util.concurrent.atomic.LongAdder multiRecordAppendBatches =
            new java.util.concurrent.atomic.LongAdder();
    private final java.util.concurrent.atomic.LongAdder appendedRecordCount =
            new java.util.concurrent.atomic.LongAdder();
    private final java.util.concurrent.atomic.LongAdder appendedBytes = new java.util.concurrent.atomic.LongAdder();
    private final java.util.concurrent.atomic.LongAdder appendNanos = new java.util.concurrent.atomic.LongAdder();
    private final java.util.concurrent.atomic.LongAdder flushCount = new java.util.concurrent.atomic.LongAdder();
    private final java.util.concurrent.atomic.LongAdder flushNanos = new java.util.concurrent.atomic.LongAdder();
    private long nextOffset;
    private long lastForceNanos;
    private long lastStateFlushNanos = System.nanoTime();
    private boolean stateDirty;

    public TenantJournalWriter(
            String tenantId,
            Path tenantDirectory,
            JournalCodec codec,
            JournalConfiguration configuration,
            long initialOffset,
            Consumer<WriteResult> resultConsumer) {
        this(tenantId, tenantDirectory, codec, configuration, initialOffset, resultConsumer, () -> {});
    }

    public TenantJournalWriter(
            String tenantId,
            Path tenantDirectory,
            JournalCodec codec,
            JournalConfiguration configuration,
            long initialOffset,
            Consumer<WriteResult> resultConsumer,
            Runnable stateFlusher) {
        this.tenantId = tenantId;
        this.log = tenantDirectory.resolve("queue.log");
        this.codec = codec;
        this.configuration = configuration;
        this.stateStore = new JournalStateStore(tenantDirectory);
        this.queue = new ArrayBlockingQueue<>(configuration.asyncQueueCapacityPerTenant());
        this.resultConsumer = resultConsumer;
        this.stateFlusher = stateFlusher;
        this.nextOffset = initialOffset;
        try {
            this.channel = FileChannel.open(
                    log, StandardOpenOption.CREATE, StandardOpenOption.WRITE, StandardOpenOption.APPEND);
        } catch (IOException exception) {
            throw new DatabaseRecoveryException("Unable to open journal for tenant " + tenantId, exception);
        }
        this.worker = new Thread(this::run, "stow-journal-" + tenantId);
        this.worker.setDaemon(true);
        this.worker.start();
    }

    public void append(QueueEventRecord event) {
        append(List.of(event));
    }

    public void append(List<QueueEventRecord> events) {
        await(enqueue(events));
    }

    public CompletableFuture<Void> enqueue(List<QueueEventRecord> events) {
        if (events.isEmpty()) {
            return CompletableFuture.completedFuture(null);
        }
        if (!accepting.get()) {
            throw new IllegalStateException("Journal writer is closed");
        }
        if (failure != null && !worker.isAlive()) {
            // The worker thread is gone; nothing will drain the queue anymore.
            throw new DatabaseRecoveryException("Journal writer failed for tenant " + tenantId, failure);
        }
        int count = events.size();
        while (true) {
            int current = queuedRecords.get();
            if (current > configuration.asyncQueueCapacityPerTenant() - count
                    || !queuedRecords.compareAndSet(current, current + count)) {
                if (current > configuration.asyncQueueCapacityPerTenant() - count) {
                    throw new IllegalStateException("Journal queue is full for tenant " + tenantId);
                }
                continue;
            }
            break;
        }
        if (queue.remainingCapacity() == 0) {
            queuedRecords.addAndGet(-count);
            throw new IllegalStateException("Journal queue is full for tenant " + tenantId);
        }
        Pending pending = new Pending(List.copyOf(events));
        if (!queue.offer(pending)) {
            queuedRecords.addAndGet(-count);
            throw new IllegalStateException("Journal queue is full for tenant " + tenantId);
        }
        return pending.completion;
    }

    public void flush() {
        Pending barrier = new Pending(List.of());
        while (!queue.offer(barrier)) {
            if (failure != null) {
                throw new DatabaseRecoveryException("Journal writer failed for tenant " + tenantId, failure);
            }
            Thread.yield();
        }
        await(barrier.completion);
    }

    private void run() {
        try {
            while (accepting.get() || !queue.isEmpty()) {
                Pending first = queue.poll(100, TimeUnit.MILLISECONDS);
                if (first == null) {
                    flushStateIfDue();
                    continue;
                }
                queuedRecords.addAndGet(-first.events.size());
                write(collect(first));
                flushStateIfDue();
            }
        } catch (InterruptedException exception) {
            if (accepting.get()) {
                failure = exception;
            }
            Thread.currentThread().interrupt();
        } catch (Throwable throwable) {
            failure = throwable;
            queue.forEach(item -> item.completion.completeExceptionally(throwable));
            queue.clear();
        }
    }

    /** Coalesces queued records up to the configured record and byte bounds, lingering for stragglers. */
    private Batch collect(Pending first) throws InterruptedException {
        List<Pending> batch = new ArrayList<>();
        List<byte[]> frames = new ArrayList<>();
        long bytes = encodeInto(first, frames);
        batch.add(first);
        long lingerMillis = configuration.linger().toMillis();
        while (batch.size() < configuration.maxBatchRecords() && bytes < configuration.maxBatchBytes()) {
            Pending next = queue.poll();
            if (next == null && lingerMillis > 0) {
                next = queue.poll(lingerMillis, TimeUnit.MILLISECONDS);
            }
            if (next == null) break;
            queuedRecords.addAndGet(-next.events.size());
            batch.add(next);
            bytes += encodeInto(next, frames);
        }
        return new Batch(batch, frames);
    }

    private long encodeInto(Pending pending, List<byte[]> frames) {
        long bytes = 0;
        for (QueueEventRecord event : pending.events) {
            byte[] frame = codec.encode(event);
            frames.add(frame);
            bytes += frame.length;
        }
        return bytes;
    }

    private void write(Batch collected) {
        List<Pending> batch = collected.pendings();
        long writeStarted = System.nanoTime();
        try {
            boolean wrote = false;
            for (byte[] frame : collected.frames()) {
                writeFully(ByteBuffer.wrap(frame));
                nextOffset += frame.length;
                wrote = true;
            }
            if (wrote) {
                forceAccordingToAckMode();
                resultConsumer.accept(new WriteResult(nextOffset, lastSequenceOf(batch)));
                stateDirty = true;
                appendBatchCount.increment();
                if (batch.size() == 1 && batch.get(0).events.size() == 1) {
                    singleRecordAppendBatches.increment();
                } else {
                    multiRecordAppendBatches.increment();
                }
                appendedRecordCount.add(collected.frames().size());
                appendedBytes.add(collected.frames().stream()
                        .mapToLong(frame -> frame.length)
                        .sum());
            }
            appendNanos.add(System.nanoTime() - writeStarted);
            // A successful batch clears the previous failure: one bad append must not
            // permanently take the tenant journal down.
            failure = null;
            for (Pending pending : batch) {
                pending.completion.complete(null);
            }
        } catch (Throwable throwable) {
            failure = throwable;
            for (Pending pending : batch) {
                pending.completion.completeExceptionally(throwable);
            }
        }
    }

    private static long lastSequenceOf(List<Pending> batch) {
        long last = 0;
        for (Pending pending : batch) {
            for (QueueEventRecord event : pending.events) {
                last = event.sequenceNumber();
            }
        }
        return last;
    }

    private void forceAccordingToAckMode() throws IOException {
        if (configuration.ackMode() == JournalAckMode.DURABLE) {
            long flushStarted = System.nanoTime();
            channel.force(true);
            flushCount.increment();
            flushNanos.add(System.nanoTime() - flushStarted);
            lastForceNanos = System.nanoTime();
        } else if (configuration.ackMode() == JournalAckMode.BALANCED) {
            // fsync immediately with no backlog and at least once per flush window
            // under load, bounding the durability gap acknowledged events can span.
            long window = configuration.balancedFlushWindow().toNanos();
            if (queue.isEmpty() || System.nanoTime() - lastForceNanos >= window) {
                long flushStarted = System.nanoTime();
                channel.force(true);
                flushCount.increment();
                flushNanos.add(System.nanoTime() - flushStarted);
                lastForceNanos = System.nanoTime();
            }
        }
    }

    /** Aggregates this writer's observed write-path counters into the given builder. */
    void collectWritePathStatistics(QueueJournalWritePathStatistics.Builder builder) {
        builder.appendBatchCount(appendBatchCount.sum())
                .singleRecordAppendBatches(singleRecordAppendBatches.sum())
                .multiRecordAppendBatches(multiRecordAppendBatches.sum())
                .appendedRecordCount(appendedRecordCount.sum())
                .appendedBytes(appendedBytes.sum())
                .appendNanos(appendNanos.sum())
                .flushCount(flushCount.sum())
                .flushNanos(flushNanos.sum());
    }

    private void flushStateIfDue() {
        if (!stateDirty) return;
        if (System.nanoTime() - lastStateFlushNanos
                < configuration.stateFlushDebounce().toNanos()) return;
        flushStateNow();
    }

    private void flushStateNow() {
        stateDirty = false;
        lastStateFlushNanos = System.nanoTime();
        try {
            stateFlusher.run();
        } catch (RuntimeException ignored) {
            // The state file is a rebuildable hint; the journal remains the source of truth.
        }
    }

    private void writeFully(ByteBuffer bytes) throws IOException {
        while (bytes.hasRemaining()) {
            if (channel.write(bytes) <= 0) {
                throw new IOException("Journal channel made no progress");
            }
        }
    }

    private static void await(CompletableFuture<Void> completion) {
        try {
            completion.join();
        } catch (java.util.concurrent.CompletionException exception) {
            Throwable cause = exception.getCause();
            if (cause instanceof RuntimeException runtime) {
                throw runtime;
            }
            throw new DatabaseRecoveryException("Journal write failed", cause);
        }
    }

    @Override
    public void close() {
        if (!accepting.get()) {
            return;
        }
        // Put a barrier through the live worker before asking it to stop.
        flush();
        flushStateNow();
        if (!accepting.getAndSet(false)) {
            return;
        }
        try {
            worker.join(configuration.writerIdleTimeout().toMillis());
            if (worker.isAlive()) {
                worker.interrupt();
            }
            channel.force(true);
            channel.close();
        } catch (InterruptedException exception) {
            Thread.currentThread().interrupt();
            throw new StowInterruptedException("Interrupted while closing journal writer", exception);
        } catch (IOException exception) {
            throw new DatabaseRecoveryException("Unable to close journal writer", exception);
        }
    }

    public record WriteResult(long tailOffset, long lastSequenceNumber) {}

    private record Batch(List<Pending> pendings, List<byte[]> frames) {}

    private static final class Pending {
        private final List<QueueEventRecord> events;
        private final CompletableFuture<Void> completion = new CompletableFuture<>();

        private Pending(List<QueueEventRecord> events) {
            this.events = events;
        }
    }
}
