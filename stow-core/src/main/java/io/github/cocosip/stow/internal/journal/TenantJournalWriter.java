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
    private final FileChannel channel;
    private final AtomicBoolean accepting = new AtomicBoolean(true);
    private final AtomicInteger queuedRecords = new AtomicInteger();
    private final Thread worker;
    private volatile Throwable failure;
    private long nextOffset;

    public TenantJournalWriter(
            String tenantId,
            Path tenantDirectory,
            JournalCodec codec,
            JournalConfiguration configuration,
            long initialOffset,
            Consumer<WriteResult> resultConsumer) {
        this.tenantId = tenantId;
        this.log = tenantDirectory.resolve("queue.log");
        this.codec = codec;
        this.configuration = configuration;
        this.stateStore = new JournalStateStore(tenantDirectory);
        this.queue = new ArrayBlockingQueue<>(configuration.asyncQueueCapacityPerTenant());
        this.resultConsumer = resultConsumer;
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
        if (failure != null) {
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
                    continue;
                }
                queuedRecords.addAndGet(-first.events.size());
                List<Pending> batch = new ArrayList<>();
                batch.add(first);
                while (batch.size() < configuration.maxBatchRecords()) {
                    Pending next = queue.poll();
                    if (next == null) {
                        break;
                    }
                    queuedRecords.addAndGet(-next.events.size());
                    batch.add(next);
                }
                write(batch);
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

    private void write(List<Pending> batch) {
        try {
            boolean wrote = false;
            long lastSequence = 0;
            for (Pending pending : batch) {
                for (QueueEventRecord event : pending.events) {
                    byte[] bytes = codec.encode(event);
                    writeFully(ByteBuffer.wrap(bytes));
                    nextOffset += bytes.length;
                    lastSequence = event.sequenceNumber();
                    wrote = true;
                }
            }
            if (configuration.ackMode() == JournalAckMode.DURABLE && wrote) {
                channel.force(true);
            }
            if (wrote) {
                resultConsumer.accept(new WriteResult(nextOffset, lastSequence));
            }
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

    private static final class Pending {
        private final List<QueueEventRecord> events;
        private final CompletableFuture<Void> completion = new CompletableFuture<>();

        private Pending(List<QueueEventRecord> events) {
            this.events = events;
        }
    }
}
