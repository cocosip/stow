package io.github.cocosip.stow.internal.cleanup;

import io.github.cocosip.stow.model.QueueEventRecord;
import io.github.cocosip.stow.spi.QueueEventJournal;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;

/** Serializes maintenance events per tenant and assigns the next journal sequence. */
public final class MaintenanceEventAppender {

    private final QueueEventJournal journal;
    private final Map<String, Object> tenantLocks = new ConcurrentHashMap<>();

    public MaintenanceEventAppender(QueueEventJournal journal) {
        this.journal = Objects.requireNonNull(journal, "journal");
    }

    public QueueEventRecord append(QueueEventRecord event) {
        Objects.requireNonNull(event, "event");
        Object lock = tenantLocks.computeIfAbsent(event.tenantId(), ignored -> new Object());
        synchronized (lock) {
            long nextSequence = lastSequence(event.tenantId()) + 1;
            QueueEventRecord sequenced = withSequence(event, nextSequence);
            journal.append(sequenced);
            return sequenced;
        }
    }

    private long lastSequence(String tenantId) {
        long offset = journal.baseOffset(tenantId);
        long tail = journal.tailOffset(tenantId);
        if (offset >= tail) {
            return journal.readBatch(tenantId, offset, 1).lastSequenceNumber();
        }
        long last = 0;
        long cursor = offset;
        while (cursor < tail) {
            var batch = journal.readBatch(tenantId, cursor, Integer.MAX_VALUE);
            if (batch.events().isEmpty() || batch.nextOffset() <= cursor) {
                return Math.max(last, batch.lastSequenceNumber());
            }
            last = batch.lastSequenceNumber();
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
