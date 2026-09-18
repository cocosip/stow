package io.github.cocosip.stow.internal.projection;

import io.github.cocosip.stow.internal.journal.JournalReadBatch;
import io.github.cocosip.stow.model.QueueEventRecord;
import io.github.cocosip.stow.spi.QueueEventJournal;
import java.time.Clock;

public final class QueueProjectionService {

    private final QueueEventJournal journal;
    private final QueueEventReducer reducer;
    private final ProjectionCursorStore cursors;
    private final Clock clock;
    private final ActiveFileCache activeCache;

    public QueueProjectionService(QueueEventJournal journal, QueueEventReducer reducer, ProjectionCursorStore cursors) {
        this(journal, reducer, cursors, Clock.systemUTC(), null);
    }

    public QueueProjectionService(
            QueueEventJournal journal,
            QueueEventReducer reducer,
            ProjectionCursorStore cursors,
            Clock clock,
            ActiveFileCache activeCache) {
        this.journal = journal;
        this.reducer = reducer;
        this.cursors = cursors;
        this.clock = clock;
        this.activeCache = activeCache;
    }

    public boolean projectTenant(String tenantId, int maxRecords) {
        if (maxRecords <= 0) throw new IllegalArgumentException("maxRecords must be positive");
        ProjectionCursorStore.Cursor cursor = cursors.load(tenantId);
        JournalReadBatch batch = journal.readBatch(tenantId, cursor.nextOffset(), maxRecords);
        if (batch.events().isEmpty()) return false;
        QueueEventRecord last = null;
        for (QueueEventRecord event : batch.events()) {
            reducer.apply(event);
            last = event;
        }
        cursors.save(new ProjectionCursorStore.Cursor(
                tenantId, batch.nextOffset(), last.sequenceNumber(), last.eventId(), clock.instant()));
        if (activeCache != null) activeCache.invalidate(tenantId);
        return true;
    }

    public int projectTenantUntilCaughtUp(String tenantId, int maxRecords) {
        int batches = 0;
        while (projectTenant(tenantId, maxRecords)) batches++;
        return batches;
    }
}
