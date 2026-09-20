package io.github.cocosip.stow.internal.projection;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import io.github.cocosip.stow.internal.journal.JournalReadBatch;
import io.github.cocosip.stow.model.QueueEventRecord;
import io.github.cocosip.stow.spi.QueueEventJournal;
import java.time.Clock;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;

@SuppressFBWarnings(
        value = "EI_EXPOSE_REP2",
        justification = "The journal, reducer, cursor store, and cache are owned runtime services.")
public final class QueueProjectionService {

    private final QueueEventJournal journal;
    private final QueueEventReducer reducer;
    private final ProjectionCursorStore cursors;
    private final Clock clock;
    private final ActiveFileCache activeCache;
    private final Consumer<RuntimeException> failureObserver;
    private final Consumer<QueueEventRecord> projectedObserver;
    private final Map<String, Object> tenantLocks = new ConcurrentHashMap<>();

    public QueueProjectionService(QueueEventJournal journal, QueueEventReducer reducer, ProjectionCursorStore cursors) {
        this(journal, reducer, cursors, Clock.systemUTC(), null, ignored -> {}, ignored -> {});
    }

    public QueueProjectionService(
            QueueEventJournal journal,
            QueueEventReducer reducer,
            ProjectionCursorStore cursors,
            Clock clock,
            ActiveFileCache activeCache) {
        this(journal, reducer, cursors, clock, activeCache, ignored -> {}, ignored -> {});
    }

    public QueueProjectionService(
            QueueEventJournal journal,
            QueueEventReducer reducer,
            ProjectionCursorStore cursors,
            Clock clock,
            ActiveFileCache activeCache,
            Consumer<RuntimeException> failureObserver) {
        this(journal, reducer, cursors, clock, activeCache, failureObserver, ignored -> {});
    }

    public QueueProjectionService(
            QueueEventJournal journal,
            QueueEventReducer reducer,
            ProjectionCursorStore cursors,
            Clock clock,
            ActiveFileCache activeCache,
            Consumer<RuntimeException> failureObserver,
            Consumer<QueueEventRecord> projectedObserver) {
        this.journal = journal;
        this.reducer = reducer;
        this.cursors = cursors;
        this.clock = clock;
        this.activeCache = activeCache;
        this.failureObserver = failureObserver;
        this.projectedObserver = projectedObserver;
    }

    public boolean projectTenant(String tenantId, int maxRecords) {
        if (maxRecords <= 0) throw new IllegalArgumentException("maxRecords must be positive");
        Object lock = tenantLocks.computeIfAbsent(tenantId, ignored -> new Object());
        try {
            synchronized (lock) {
                return projectTenantLocked(tenantId, maxRecords);
            }
        } catch (RuntimeException failure) {
            notifyFailure(failure);
            throw failure;
        }
    }

    private boolean projectTenantLocked(String tenantId, int maxRecords) {
        ProjectionCursorStore.Cursor cursor = cursors.load(tenantId);
        JournalReadBatch batch = journal.readBatch(tenantId, cursor.nextOffset(), maxRecords);
        if (batch.events().isEmpty()) return false;
        QueueEventRecord last = null;
        for (QueueEventRecord event : batch.events()) {
            reducer.apply(event);
            projectedObserver.accept(event);
            last = event;
        }
        cursors.save(new ProjectionCursorStore.Cursor(
                tenantId, batch.nextOffset(), last.sequenceNumber(), last.eventId(), clock.instant()));
        if (activeCache != null) activeCache.invalidate(tenantId);
        return true;
    }

    public int projectTenantUntilCaughtUp(String tenantId, int maxRecords) {
        if (maxRecords <= 0) throw new IllegalArgumentException("maxRecords must be positive");
        Object lock = tenantLocks.computeIfAbsent(tenantId, ignored -> new Object());
        try {
            synchronized (lock) {
                int batches = 0;
                while (projectTenantLocked(tenantId, maxRecords)) batches++;
                return batches;
            }
        } catch (RuntimeException failure) {
            notifyFailure(failure);
            throw failure;
        }
    }

    private void notifyFailure(RuntimeException failure) {
        try {
            failureObserver.accept(failure);
        } catch (RuntimeException observerFailure) {
            failure.addSuppressed(observerFailure);
        }
    }
}
