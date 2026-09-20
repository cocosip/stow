package io.github.cocosip.stow.internal.scheduler;

import io.github.cocosip.stow.model.ProcessingLease;
import io.github.cocosip.stow.model.QueueEventRecord;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/** Runtime index rebuilt by journal replay and updated immediately after durable terminal admission. */
public final class TerminalLeaseIndex {

    private final Map<String, Outcome> outcomes = new ConcurrentHashMap<>();

    public void record(QueueEventRecord event) {
        if (event.leaseId() == null) return;
        Outcome outcome =
                switch (event.eventType()) {
                    case PROCESSING_COMPLETED -> Outcome.COMPLETED;
                    case PROCESSING_FAILED -> Outcome.FAILED;
                    case PROCESSING_TIMED_OUT -> Outcome.TIMED_OUT;
                    default -> null;
                };
        if (outcome != null) outcomes.put(key(event.tenantId(), event.fileKey(), event.leaseId()), outcome);
    }

    public Outcome find(ProcessingLease lease) {
        return outcomes.get(key(lease.tenantId(), lease.fileKey(), lease.leaseId()));
    }

    private static String key(String tenantId, String fileKey, java.util.UUID leaseId) {
        return tenantId + '\u0000' + fileKey + '\u0000' + leaseId;
    }

    public enum Outcome {
        COMPLETED,
        FAILED,
        TIMED_OUT
    }
}
