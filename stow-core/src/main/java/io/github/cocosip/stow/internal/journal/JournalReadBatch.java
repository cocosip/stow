package io.github.cocosip.stow.internal.journal;

import io.github.cocosip.stow.model.QueueEventRecord;
import java.util.List;

public record JournalReadBatch(
        String tenantId, long startOffset, long nextOffset, long lastSequenceNumber, List<QueueEventRecord> events) {

    public JournalReadBatch {
        if (startOffset < 0 || nextOffset < startOffset || lastSequenceNumber < 0) {
            throw new IllegalArgumentException("invalid journal batch offsets");
        }
        events = List.copyOf(events);
    }

    public boolean hasMore() {
        return !events.isEmpty() && nextOffset > startOffset;
    }
}
