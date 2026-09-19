package io.github.cocosip.stow.internal.projection;

import io.github.cocosip.stow.api.QueueProjectionMaintenance;
import io.github.cocosip.stow.model.HealthStatus;
import io.github.cocosip.stow.model.ProjectionTenantState;
import io.github.cocosip.stow.spi.QueueEventJournal;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

public final class ProjectionMaintenanceService implements QueueProjectionMaintenance {

    private final QueueEventJournal journal;
    private final QueueProjectionService projection;
    private final ProjectionCursorStore cursors;
    private final ProjectionSnapshotStore snapshots;
    private final QueueEventReducer reducer;

    public ProjectionMaintenanceService(
            QueueEventJournal journal,
            QueueProjectionService projection,
            ProjectionCursorStore cursors,
            ProjectionSnapshotStore snapshots) {
        this(journal, projection, cursors, snapshots, null);
    }

    public ProjectionMaintenanceService(
            QueueEventJournal journal,
            QueueProjectionService projection,
            ProjectionCursorStore cursors,
            ProjectionSnapshotStore snapshots,
            QueueEventReducer reducer) {
        this.journal = java.util.Objects.requireNonNull(journal, "journal");
        this.projection = projection;
        this.cursors = java.util.Objects.requireNonNull(cursors, "cursors");
        this.snapshots = java.util.Objects.requireNonNull(snapshots, "snapshots");
        this.reducer = reducer;
    }

    @Override
    public ProjectionTenantState state(String tenantId) {
        long base = journal.baseOffset(tenantId);
        long tail = journal.tailOffset(tenantId);
        ProjectionCursorStore.Cursor cursor = cursors.load(tenantId);
        long projected = Math.max(base, Math.min(cursor.nextOffset(), tail));
        return new ProjectionTenantState(
                tenantId,
                base,
                tail,
                projected,
                cursor.lastSequenceNumber(),
                snapshots.load(tenantId) == null ? Instant.EPOCH : Instant.now(),
                projected == tail ? HealthStatus.UP : HealthStatus.DEGRADED);
    }

    @Override
    public ProjectionTenantState replay(String tenantId) {
        if (projection == null) throw new IllegalStateException("projection service is not configured");
        projection.projectTenantUntilCaughtUp(tenantId, 256);
        return state(tenantId);
    }

    @Override
    public synchronized ProjectionTenantState snapshot(String tenantId) {
        long base = journal.baseOffset(tenantId);
        long tail = journal.tailOffset(tenantId);
        ProjectionCursorStore.Cursor cursor = cursors.load(tenantId);
        if (cursor.nextOffset() < base || cursor.nextOffset() > tail) {
            throw new IllegalStateException("snapshot offset is outside journal bounds");
        }
        ProjectionTenantState state = state(tenantId);
        List<io.github.cocosip.stow.model.QueueEventRecord> events =
                readThrough(tenantId, state.baseOffset(), state.projectedOffset());
        snapshots.save(new ProjectionSnapshotStore.Snapshot(
                tenantId, state.baseOffset(), state.projectedOffset(), state.lastSequence(), events));
        return state;
    }

    public synchronized void compact(String tenantId) {
        ProjectionTenantState state = state(tenantId);
        if (state.projectedOffset() != state.tailOffset())
            throw new IllegalStateException("projector must be caught up before compaction");
        ProjectionSnapshotStore.Snapshot snapshot = snapshots.load(tenantId);
        if (snapshot == null || snapshot.nextOffset() != state.tailOffset()) snapshot(tenantId);
        journal.compact(tenantId, state.tailOffset());
    }

    @Override
    public synchronized ProjectionTenantState rebuild(String tenantId) {
        if (reducer == null) throw new IllegalStateException("reducer is not configured");
        reducer.resetMetadata(tenantId);
        ProjectionSnapshotStore.Snapshot snapshot = snapshots.load(tenantId);
        long base = journal.baseOffset(tenantId);
        long tail = journal.tailOffset(tenantId);
        List<io.github.cocosip.stow.model.QueueEventRecord> events = new ArrayList<>();
        long offset = base;
        if (snapshot != null) {
            if (snapshot.nextOffset() < base || snapshot.nextOffset() > tail)
                throw new IllegalStateException("snapshot offset is outside journal bounds");
            events.addAll(snapshot.events());
            offset = snapshot.nextOffset();
        }
        events.addAll(readThrough(tenantId, offset, tail));
        for (var event : events) reducer.applyMetadataOnly(event);
        ProjectionCursorStore.Cursor cursor = events.isEmpty()
                ? ProjectionCursorStore.Cursor.initial(tenantId)
                : new ProjectionCursorStore.Cursor(
                        tenantId,
                        tail,
                        events.get(events.size() - 1).sequenceNumber(),
                        events.get(events.size() - 1).eventId(),
                        Instant.now());
        cursors.save(cursor);
        return state(tenantId);
    }

    private List<io.github.cocosip.stow.model.QueueEventRecord> readThrough(String tenantId, long offset, long end) {
        List<io.github.cocosip.stow.model.QueueEventRecord> events = new ArrayList<>();
        long cursor = offset;
        while (cursor < end) {
            var batch = journal.readBatch(tenantId, cursor, 256);
            if (batch.events().isEmpty() || batch.nextOffset() <= cursor) break;
            events.addAll(batch.events());
            cursor = batch.nextOffset();
        }
        return List.copyOf(events);
    }
}
