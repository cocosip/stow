package io.github.cocosip.stow.internal.projection;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import io.github.cocosip.stow.exception.ProjectionException;
import io.github.cocosip.stow.internal.quota.QuotaReservation;
import io.github.cocosip.stow.internal.quota.SqliteQuotaRepository;
import io.github.cocosip.stow.internal.statistics.NoopStatisticsRecorder;
import io.github.cocosip.stow.internal.statistics.StatisticsRecorder;
import io.github.cocosip.stow.model.FileProcessingStatus;
import io.github.cocosip.stow.model.QueueEventRecord;
import io.github.cocosip.stow.model.QueueEventType;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Optional;

@SuppressFBWarnings(
        value = "EI_EXPOSE_REP2",
        justification = "Projection stores are owned runtime services and are intentionally shared by the reducer.")
public final class QueueEventReducer {

    private final SqliteMetadataProjectionStore metadata;
    private final SqliteQuotaRepository quota;
    private final StatisticsRecorder statistics;

    public QueueEventReducer(SqliteMetadataProjectionStore metadata, SqliteQuotaRepository quota) {
        this(metadata, quota, new NoopStatisticsRecorder(java.time.Clock.systemUTC()));
    }

    public QueueEventReducer(
            SqliteMetadataProjectionStore metadata, SqliteQuotaRepository quota, StatisticsRecorder statistics) {
        this.metadata = metadata;
        this.quota = quota;
        this.statistics = statistics;
    }

    public void apply(QueueEventRecord event) {
        apply(event, true);
    }

    public void applyMetadataOnly(QueueEventRecord event) {
        apply(event, false);
    }

    public void resetMetadata(String tenantId) {
        metadata.clear(tenantId);
    }

    private void apply(QueueEventRecord event, boolean applyQuota) {
        metadata.write(event.tenantId(), connection -> {
            boolean fresh = SqliteMetadataProjectionStore.markApplied(connection, event, metadata.nowMillis());
            if (fresh) {
                reduce(connection, event);
            }
            return fresh;
        });
        // The two SQLite databases cannot share a transaction. Re-running the quota side effect
        // after a metadata duplicate is intentional and completes a previously interrupted batch.
        if (!applyQuota) return;
        if (event.eventType() == QueueEventType.ACCEPTED) {
            Optional<QuotaReservation> reservation = quota.reservation(event.tenantId(), event.fileKey());
            quota.consume(
                    event.tenantId(),
                    event.eventId().toString(),
                    event.sequenceNumber(),
                    reservation.map(QuotaReservation::reservationId).orElse("already-applied-or-missing"));
        } else if (event.eventType() == QueueEventType.DELETE_SUCCEEDED
                || event.eventType() == QueueEventType.DEAD_LETTERED) {
            quota.release(
                    event.tenantId(),
                    event.eventId().toString(),
                    event.sequenceNumber(),
                    event.fileKey(),
                    event.logicalDirectory());
        }
        statistics.recordSqlitePersistence(event.tenantId());
    }

    private void reduce(Connection connection, QueueEventRecord event) throws SQLException {
        Optional<SqliteMetadataProjectionStore.FileRow> existing = find(connection, event.fileKey());
        switch (event.eventType()) {
            case ACCEPTED -> accepted(connection, event, existing);
            case PROCESSING_STARTED -> processingStarted(connection, event, existing);
            case PROCESSING_FAILED -> processingFailed(connection, event, existing);
            case PROCESSING_TIMED_OUT -> timedOut(connection, event, existing);
            case PROCESSING_COMPLETED -> completed(connection, event, existing);
            case DELETE_REQUESTED -> deleteRequested(connection, event, existing);
            case DELETE_SUCCEEDED -> deleteSucceeded(connection, event, existing);
            case DEAD_LETTERED -> deadLettered(connection, event, existing);
        }
    }

    private static void accepted(Connection c, QueueEventRecord e, Optional<SqliteMetadataProjectionStore.FileRow> row)
            throws SQLException {
        if (row.isPresent()) throw conflict("ACCEPTED requires no predecessor");
        if (e.status() != FileProcessingStatus.PENDING) throw conflict("ACCEPTED must result in PENDING");
        try (PreparedStatement s = c.prepareStatement(
                """
                INSERT INTO files(file_key, tenant_id, volume_id, physical_path, logical_directory, file_size,
                    created_at_ms, status, retry_count, lease_id, processing_started_at_ms, original_file_name,
                    file_extension, import_operation_id, last_event_sequence, row_version)
                VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 0)
                """)) {
            s.setString(1, e.fileKey());
            s.setString(2, e.tenantId());
            s.setString(3, e.volumeId());
            s.setString(4, e.physicalPath().toString());
            s.setString(5, e.logicalDirectory());
            s.setLong(6, e.fileSize());
            s.setLong(7, e.occurredAt().toEpochMilli());
            s.setInt(8, FileProcessingStatus.PENDING.ordinal());
            s.setInt(9, 0);
            s.setObject(10, null);
            s.setObject(11, null);
            s.setString(12, e.originalFileName());
            s.setString(13, e.fileExtension());
            s.setString(14, e.importOperationId());
            s.setLong(15, e.sequenceNumber());
            s.executeUpdate();
        }
    }

    private static void processingStarted(
            Connection c, QueueEventRecord e, Optional<SqliteMetadataProjectionStore.FileRow> row) throws SQLException {
        SqliteMetadataProjectionStore.FileRow current = require(row, "PROCESSING_STARTED");
        if (e.status() != FileProcessingStatus.PROCESSING || e.leaseId() == null)
            throw conflict("PROCESSING_STARTED requires processing status and leaseId");
        // A scheduler conditionally claims the row before appending the journal event. Replay of
        // that event must acknowledge the already-applied lease transition rather than applying it twice.
        if (current.status() == FileProcessingStatus.PROCESSING
                && e.leaseId().toString().equals(current.leaseId())) {
            update(
                    c,
                    e,
                    "UPDATE files SET last_event_sequence=?, row_version=row_version+1 WHERE file_key=?",
                    e.sequenceNumber(),
                    e.fileKey());
            return;
        }
        requireStatus(current, FileProcessingStatus.PENDING, FileProcessingStatus.FAILED);
        update(
                c,
                e,
                """
                UPDATE files SET status=?, lease_id=?, processing_started_at_ms=?, available_at_ms=NULL,
                    last_event_sequence=?, row_version=row_version+1 WHERE file_key=?
                """,
                FileProcessingStatus.PROCESSING.ordinal(),
                e.leaseId().toString(),
                millis(e.processingStartedAt(), e.occurredAt()),
                e.sequenceNumber(),
                e.fileKey());
    }

    private static void processingFailed(
            Connection c, QueueEventRecord e, Optional<SqliteMetadataProjectionStore.FileRow> row) throws SQLException {
        SqliteMetadataProjectionStore.FileRow current = require(row, "PROCESSING_FAILED");
        requireStatus(current, FileProcessingStatus.PROCESSING);
        requireLease(current, e);
        if (e.status() != FileProcessingStatus.FAILED && e.status() != FileProcessingStatus.PERMANENTLY_FAILED)
            throw conflict("invalid failed result");
        update(
                c,
                e,
                """
                UPDATE files SET status=?, retry_count=?, last_failed_at_ms=?, last_error=?, lease_id=NULL,
                    processing_started_at_ms=NULL, available_at_ms=?, last_event_sequence=?, row_version=row_version+1 WHERE file_key=?
                """,
                e.status().ordinal(),
                Math.max(current.retryCount() + 1, e.retryCount()),
                e.occurredAt().toEpochMilli(),
                e.errorMessage(),
                millis(e.availableAt(), e.occurredAt()),
                e.sequenceNumber(),
                e.fileKey());
    }

    private static void timedOut(Connection c, QueueEventRecord e, Optional<SqliteMetadataProjectionStore.FileRow> row)
            throws SQLException {
        SqliteMetadataProjectionStore.FileRow current = require(row, "PROCESSING_TIMED_OUT");
        requireStatus(current, FileProcessingStatus.PROCESSING);
        requireLease(current, e);
        if (e.status() != FileProcessingStatus.PENDING) throw conflict("PROCESSING_TIMED_OUT must result in PENDING");
        update(
                c,
                e,
                "UPDATE files SET status=?, lease_id=NULL, processing_started_at_ms=NULL, last_event_sequence=?, row_version=row_version+1 WHERE file_key=?",
                FileProcessingStatus.PENDING.ordinal(),
                e.sequenceNumber(),
                e.fileKey());
    }

    private static void completed(Connection c, QueueEventRecord e, Optional<SqliteMetadataProjectionStore.FileRow> row)
            throws SQLException {
        SqliteMetadataProjectionStore.FileRow current = require(row, "PROCESSING_COMPLETED");
        if (e.status() != FileProcessingStatus.COMPLETED || e.leaseId() == null)
            throw conflict("PROCESSING_COMPLETED requires completed status and leaseId");
        if (current.status() == FileProcessingStatus.PROCESSING) {
            requireLease(current, e);
        } else {
            requireStatus(current, FileProcessingStatus.PENDING, FileProcessingStatus.FAILED);
            if (current.leaseId() != null) throw conflict("Released lease is still active");
        }
        update(
                c,
                e,
                "UPDATE files SET status=?, lease_id=NULL, processing_started_at_ms=NULL, completed_at_ms=?, last_event_sequence=?, row_version=row_version+1 WHERE file_key=?",
                FileProcessingStatus.COMPLETED.ordinal(),
                e.occurredAt().toEpochMilli(),
                e.sequenceNumber(),
                e.fileKey());
    }

    private static void deleteRequested(
            Connection c, QueueEventRecord e, Optional<SqliteMetadataProjectionStore.FileRow> row) throws SQLException {
        SqliteMetadataProjectionStore.FileRow current = require(row, "DELETE_REQUESTED");
        requireStatus(current, FileProcessingStatus.COMPLETED);
        if (e.status() != FileProcessingStatus.DELETE_REQUESTED)
            throw conflict("DELETE_REQUESTED must result in DELETE_REQUESTED");
        update(
                c,
                e,
                "UPDATE files SET status=?, last_event_sequence=?, row_version=row_version+1 WHERE file_key=?",
                FileProcessingStatus.DELETE_REQUESTED.ordinal(),
                e.sequenceNumber(),
                e.fileKey());
    }

    private static void deleteSucceeded(
            Connection c, QueueEventRecord e, Optional<SqliteMetadataProjectionStore.FileRow> row) throws SQLException {
        SqliteMetadataProjectionStore.FileRow current = require(row, "DELETE_SUCCEEDED");
        requireStatus(current, FileProcessingStatus.DELETE_REQUESTED);
        if (e.status() != FileProcessingStatus.DELETE_SUCCEEDED)
            throw conflict("DELETE_SUCCEEDED must result in DELETE_SUCCEEDED");
        try (PreparedStatement s = c.prepareStatement("DELETE FROM files WHERE file_key=?")) {
            s.setString(1, e.fileKey());
            s.executeUpdate();
        }
    }

    private static void deadLettered(
            Connection c, QueueEventRecord e, Optional<SqliteMetadataProjectionStore.FileRow> row) throws SQLException {
        SqliteMetadataProjectionStore.FileRow current = require(row, "DEAD_LETTERED");
        requireStatus(current, FileProcessingStatus.PERMANENTLY_FAILED);
        if (e.status() != FileProcessingStatus.DEAD_LETTERED)
            throw conflict("DEAD_LETTERED must result in DEAD_LETTERED");
        try (PreparedStatement s = c.prepareStatement("DELETE FROM files WHERE file_key=?")) {
            s.setString(1, e.fileKey());
            s.executeUpdate();
        }
    }

    private static Optional<SqliteMetadataProjectionStore.FileRow> find(Connection c, String key) throws SQLException {
        try (PreparedStatement s = c.prepareStatement("SELECT * FROM files WHERE file_key=?")) {
            s.setString(1, key);
            try (var r = s.executeQuery()) {
                return r.next() ? Optional.of(SqliteMetadataProjectionStore.readRow(r)) : Optional.empty();
            }
        }
    }

    private static void update(Connection c, QueueEventRecord e, String sql, Object... args) throws SQLException {
        try (PreparedStatement s = c.prepareStatement(sql)) {
            int index = 1;
            for (Object arg : args) {
                if (arg == null) s.setObject(index++, null);
                else if (arg instanceof Integer i) s.setInt(index++, i);
                else if (arg instanceof Long l) s.setLong(index++, l);
                else s.setString(index++, arg.toString());
            }
            s.executeUpdate();
        }
    }

    private static SqliteMetadataProjectionStore.FileRow require(
            Optional<SqliteMetadataProjectionStore.FileRow> row, String event) {
        return row.orElseThrow(() -> conflict(event + " requires existing file"));
    }

    private static void requireStatus(SqliteMetadataProjectionStore.FileRow row, FileProcessingStatus... allowed) {
        for (FileProcessingStatus status : allowed) if (row.status() == status) return;
        throw conflict("Illegal predecessor status " + row.status());
    }

    private static void requireLease(SqliteMetadataProjectionStore.FileRow row, QueueEventRecord e) {
        if (e.leaseId() == null || !e.leaseId().toString().equals(row.leaseId()))
            throw conflict("Lease does not match active lease");
    }

    private static ProjectionException conflict(String message) {
        return new ProjectionException(message);
    }

    private static Long millis(java.time.Instant value, java.time.Instant fallback) {
        return (value == null ? fallback : value).toEpochMilli();
    }
}
