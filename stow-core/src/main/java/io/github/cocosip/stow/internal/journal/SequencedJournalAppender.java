package io.github.cocosip.stow.internal.journal;

import io.github.cocosip.stow.model.QueueEventRecord;
import io.github.cocosip.stow.spi.QueueEventJournal;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/** Assigns and admits journal sequences through one runtime-scoped per-tenant critical section. */
public final class SequencedJournalAppender {

    private static final int SEQUENCE_SCAN_BATCH_SIZE = 1_024;

    private final QueueEventJournal journal;
    private final Map<String, Object> tenantLocks = new ConcurrentHashMap<>();
    private final Map<String, AtomicLong> sequences = new ConcurrentHashMap<>();

    public SequencedJournalAppender(QueueEventJournal journal) {
        this.journal = Objects.requireNonNull(journal, "journal");
    }

    public QueueEventRecord append(QueueEventRecord event) {
        Objects.requireNonNull(event, "event");
        Object lock = tenantLocks.computeIfAbsent(event.tenantId(), ignored -> new Object());
        synchronized (lock) {
            AtomicLong sequence = sequence(event.tenantId());
            QueueEventRecord sequenced = withSequence(event, sequence.incrementAndGet());
            try {
                journal.append(sequenced);
                return sequenced;
            } catch (RuntimeException failure) {
                sequence.decrementAndGet();
                throw failure;
            }
        }
    }

    public List<QueueEventRecord> appendBatch(List<QueueEventRecord> events) {
        Objects.requireNonNull(events, "events");
        if (events.isEmpty()) return List.of();
        String tenantId = events.get(0).tenantId();
        if (events.stream().anyMatch(event -> !tenantId.equals(event.tenantId()))) {
            throw new IllegalArgumentException("appendBatch must contain one tenant");
        }
        Object lock = tenantLocks.computeIfAbsent(tenantId, ignored -> new Object());
        synchronized (lock) {
            AtomicLong sequence = sequence(tenantId);
            List<QueueEventRecord> sequenced = new ArrayList<>(events.size());
            for (QueueEventRecord event : events) {
                sequenced.add(withSequence(event, sequence.incrementAndGet()));
            }
            try {
                journal.appendBatch(sequenced);
                return List.copyOf(sequenced);
            } catch (RuntimeException failure) {
                sequence.addAndGet(-sequenced.size());
                throw failure;
            }
        }
    }

    private AtomicLong sequence(String tenantId) {
        return sequences.computeIfAbsent(tenantId, ignored -> new AtomicLong(lastSequence(tenantId)));
    }

    private long lastSequence(String tenantId) {
        long cursor = journal.baseOffset(tenantId);
        long tail = journal.tailOffset(tenantId);
        long last = journal.readBatch(tenantId, cursor, 1).lastSequenceNumber();
        while (cursor < tail) {
            JournalReadBatch batch = journal.readBatch(tenantId, cursor, SEQUENCE_SCAN_BATCH_SIZE);
            last = Math.max(last, batch.lastSequenceNumber());
            if (batch.events().isEmpty() || batch.nextOffset() <= cursor) break;
            cursor = batch.nextOffset();
        }
        return last;
    }

    private static QueueEventRecord withSequence(QueueEventRecord event, long sequence) {
        return new QueueEventRecord(
                event.schemaVersion(),
                event.eventId(),
                event.tenantId(),
                event.fileKey(),
                event.eventType(),
                event.occurredAt(),
                sequence,
                event.volumeId(),
                event.physicalPath(),
                event.logicalDirectory(),
                event.fileSize(),
                event.status(),
                event.leaseId(),
                event.processingStartedAt(),
                event.retryCount(),
                event.availableAt(),
                event.errorMessage(),
                event.originalFileName(),
                event.fileExtension());
    }
}
