package io.github.cocosip.stow.internal.projection;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import io.github.cocosip.stow.api.QueueProjectionMaintenance;
import io.github.cocosip.stow.model.HealthStatus;
import io.github.cocosip.stow.model.ProjectionTenantState;
import io.github.cocosip.stow.spi.QueueEventJournal;
import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

@SuppressFBWarnings(
        value = "EI_EXPOSE_REP2",
        justification = "The maintenance service intentionally shares the runtime-owned projection and quota stores.")
public final class ProjectionMaintenanceService implements QueueProjectionMaintenance {

    private static final int CATCH_UP_BATCH = 4_096;

    private final QueueEventJournal journal;
    private final QueueProjectionService projection;
    private final ProjectionCursorStore cursors;
    private final ProjectionSnapshotStore snapshots;
    private final QueueEventReducer reducer;
    private final SqliteMetadataProjectionStore metadata;
    private final io.github.cocosip.stow.internal.quota.SqliteQuotaRepository quota;

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
        this(journal, projection, cursors, snapshots, reducer, null, null);
    }

    public ProjectionMaintenanceService(
            QueueEventJournal journal,
            QueueProjectionService projection,
            ProjectionCursorStore cursors,
            ProjectionSnapshotStore snapshots,
            QueueEventReducer reducer,
            SqliteMetadataProjectionStore metadata,
            io.github.cocosip.stow.internal.quota.SqliteQuotaRepository quota) {
        this.journal = java.util.Objects.requireNonNull(journal, "journal");
        this.projection = projection;
        this.cursors = java.util.Objects.requireNonNull(cursors, "cursors");
        this.snapshots = java.util.Objects.requireNonNull(snapshots, "snapshots");
        this.reducer = reducer;
        this.metadata = metadata;
        this.quota = quota;
    }

    @Override
    public ProjectionTenantState state(String tenantId) {
        long base = journal.baseOffset(tenantId);
        long tail = journal.tailOffset(tenantId);
        ProjectionCursorStore.Cursor cursor = cursors.load(tenantId);
        long projected = Math.max(base, Math.min(cursor.nextOffset(), tail));
        ProjectionSnapshotStore.Snapshot snapshot = snapshots.load(tenantId);
        Instant snapshotAt = snapshot == null || snapshot.createdAt() == null ? Instant.EPOCH : snapshot.createdAt();
        return new ProjectionTenantState(
                tenantId,
                base,
                tail,
                projected,
                cursor.lastSequenceNumber(),
                snapshotAt,
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
        // The contract only allows snapshots from a caught-up projector; try to catch
        // up first, then refuse a cursor outside the journal bounds as before.
        if (projection != null) {
            try {
                projection.projectTenantUntilCaughtUp(tenantId, CATCH_UP_BATCH);
            } catch (RuntimeException ignored) {
                // A transient projection failure must not make snapshots unavailable;
                // the bounds check below still enforces consistency.
            }
        }
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
                tenantId,
                state.baseOffset(),
                state.projectedOffset(),
                state.lastSequence(),
                events,
                java.time.Clock.systemUTC().instant(),
                activeFiles(tenantId),
                quotaState(tenantId)));
        return state;
    }

    public synchronized void compact(String tenantId) {
        ProjectionTenantState state = state(tenantId);
        if (state.projectedOffset() != state.tailOffset())
            throw new IllegalStateException("projector must be caught up before compaction");
        ProjectionSnapshotStore.Snapshot snapshot = snapshots.load(tenantId);
        if (snapshot != null && snapshot.baseOffset() != state.baseOffset()) {
            throw new IllegalStateException("snapshot base does not match journal base");
        }
        if (snapshot == null || snapshot.nextOffset() != state.tailOffset()) snapshot(tenantId);
        journal.compact(tenantId, state.tailOffset());
        // With a snapshot plus compaction covering the sequence, the applied-event
        // ledgers for compacted events are dead weight (space only, never correctness).
        if (snapshot != null) {
            long throughSequence = snapshot.lastSequenceNumber();
            if (metadata != null) metadata.pruneAppliedEvents(tenantId, throughSequence);
            if (quota != null) quota.pruneAppliedQuotaEvents(tenantId, throughSequence);
        }
    }

    @Override
    public synchronized ProjectionTenantState rebuild(String tenantId) {
        if (reducer == null) throw new IllegalStateException("reducer is not configured");
        if (metadata == null || quota == null) {
            return rebuildLocked(tenantId);
        }
        // The replay must not interleave with concurrent projection batches (Locus
        // BeginDatabaseRebuildAsync holds the exclusive tenant lock for the rebuild).
        return metadata.exclusively(tenantId, () -> quota.exclusively(tenantId, () -> rebuildLocked(tenantId)));
    }

    private ProjectionTenantState rebuildLocked(String tenantId) {
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
        // A metadata-only replay skips quota side effects by design, so the counts are
        // recomputed from the restored active set once the replay converges.
        if (quota != null && metadata != null) {
            quota.rebuildFromMetadata(tenantId, metadata.activeFiles(tenantId));
        }
        return state(tenantId);
    }

    private List<io.github.cocosip.stow.internal.projection.SqliteMetadataProjectionStore.FileRow> activeFiles(
            String tenantId) {
        return metadata == null ? null : metadata.activeFiles(tenantId);
    }

    private ProjectionSnapshotStore.QuotaState quotaState(String tenantId) {
        if (quota == null || metadata == null) return null;
        List<io.github.cocosip.stow.internal.projection.SqliteMetadataProjectionStore.FileRow> files =
                metadata.activeFiles(tenantId);
        Map<String, Long> counts = new LinkedHashMap<>();
        for (var file : files) counts.merge(file.logicalDirectory(), 1L, Long::sum);
        List<ProjectionSnapshotStore.DirectoryQuotaEntry> directories = new ArrayList<>();
        for (var entry : counts.entrySet()) {
            long limit = quota.directoryLimit(tenantId, entry.getKey());
            directories.add(new ProjectionSnapshotStore.DirectoryQuotaEntry(entry.getKey(), entry.getValue(), limit));
        }
        return new ProjectionSnapshotStore.QuotaState(files.size(), quota.tenantLimit(tenantId), directories);
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
