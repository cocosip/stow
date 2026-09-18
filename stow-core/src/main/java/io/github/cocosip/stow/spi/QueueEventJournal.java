package io.github.cocosip.stow.spi;

import io.github.cocosip.stow.internal.journal.JournalReadBatch;
import io.github.cocosip.stow.model.QueueEventRecord;
import java.util.List;
import java.util.Set;

@ExperimentalApi
public interface QueueEventJournal extends AutoCloseable {

    long append(QueueEventRecord event);

    default long appendBatch(List<QueueEventRecord> events) {
        long last = 0;
        for (QueueEventRecord event : events) {
            last = append(event);
        }
        return last;
    }

    JournalReadBatch readBatch(String tenantId, long offset, int maxRecords);

    long tailOffset(String tenantId);

    long baseOffset(String tenantId);

    Set<String> tenantIds();

    void compact(String tenantId, long throughOffset);

    void flush();

    @Override
    void close();
}
