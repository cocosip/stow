package io.github.cocosip.stow.internal.projection;

import io.github.cocosip.stow.config.SqliteConfiguration;
import io.github.cocosip.stow.exception.DatabaseRecoveryException;
import io.github.cocosip.stow.exception.ProjectionException;
import io.github.cocosip.stow.internal.sqlite.SqliteConnectionFactory;
import io.github.cocosip.stow.internal.sqlite.SqliteSchemaManager;
import io.github.cocosip.stow.model.FileProcessingStatus;
import io.github.cocosip.stow.model.QueueEventRecord;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Clock;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.locks.ReentrantLock;

public final class SqliteMetadataProjectionStore {

    private static final int LOCK_STRIPES = 256;
    private static final ReentrantLock[] LOCKS = createLocks();
    private static final List<String> CREATE_STATEMENTS = List.of(
            """
            CREATE TABLE files (
                file_key TEXT PRIMARY KEY NOT NULL,
                tenant_id TEXT NOT NULL,
                volume_id TEXT NOT NULL,
                physical_path TEXT NOT NULL,
                logical_directory TEXT NOT NULL,
                file_size INTEGER NOT NULL CHECK(file_size >= 0),
                created_at_ms INTEGER NOT NULL,
                status INTEGER NOT NULL,
                retry_count INTEGER NOT NULL DEFAULT 0 CHECK(retry_count >= 0),
                last_failed_at_ms INTEGER,
                last_error TEXT,
                lease_id TEXT,
                processing_started_at_ms INTEGER,
                completed_at_ms INTEGER,
                delete_succeeded_at_ms INTEGER,
                dead_lettered_at_ms INTEGER,
                available_at_ms INTEGER,
                original_file_name TEXT,
                file_extension TEXT,
                metadata_json TEXT,
                last_event_sequence INTEGER NOT NULL,
                row_version INTEGER NOT NULL DEFAULT 0
            )
            """,
            """
            CREATE TABLE applied_events (
                event_id TEXT PRIMARY KEY NOT NULL,
                sequence_number INTEGER NOT NULL UNIQUE,
                applied_at_ms INTEGER NOT NULL
            )
            """,
            "CREATE INDEX idx_files_status_available ON files(status, available_at_ms, created_at_ms, file_key)",
            "CREATE INDEX idx_files_status_completed ON files(status, completed_at_ms, file_key)",
            "CREATE INDEX idx_files_status_failed ON files(status, last_failed_at_ms, file_key)",
            "CREATE UNIQUE INDEX idx_files_physical_path ON files(physical_path)");

    private final SqliteConnectionFactory connections;
    private final SqliteSchemaManager schema = new SqliteSchemaManager(1);
    private final Clock clock;

    public SqliteMetadataProjectionStore(Path metadataDirectory, SqliteConfiguration configuration, Clock clock) {
        connections = new SqliteConnectionFactory(metadataDirectory, "metadata.db", configuration);
        this.clock = clock;
    }

    public SqliteMetadataProjectionStore(Path metadataDirectory) {
        this(metadataDirectory, SqliteConnectionFactory.defaults(), Clock.systemUTC());
    }

    @FunctionalInterface
    interface Transaction<T> {
        T run(Connection connection) throws SQLException;
    }

    <T> T write(String tenantId, Transaction<T> operation) {
        ReentrantLock lock = lockFor(tenantId);
        lock.lock();
        try (Connection connection = connections.open(tenantId)) {
            schema.ensureSchema(connection, CREATE_STATEMENTS);
            connection.setAutoCommit(false);
            try {
                T result = operation.run(connection);
                connection.commit();
                return result;
            } catch (SQLException | RuntimeException failure) {
                try {
                    connection.rollback();
                } catch (SQLException rollbackFailure) {
                    failure.addSuppressed(rollbackFailure);
                }
                throw failure;
            }
        } catch (SQLException exception) {
            throw new DatabaseRecoveryException("Unable to access metadata database for tenant " + tenantId, exception);
        } finally {
            lock.unlock();
        }
    }

    <T> T read(String tenantId, Transaction<T> operation) {
        ReentrantLock lock = lockFor(tenantId);
        lock.lock();
        try (Connection connection = connections.open(tenantId)) {
            schema.ensureSchema(connection, CREATE_STATEMENTS);
            return operation.run(connection);
        } catch (SQLException exception) {
            throw new DatabaseRecoveryException("Unable to access metadata database for tenant " + tenantId, exception);
        } finally {
            lock.unlock();
        }
    }

    public Optional<FileRow> find(String tenantId, String fileKey) {
        return read(tenantId, connection -> {
            try (PreparedStatement statement = connection.prepareStatement("SELECT * FROM files WHERE file_key=?")) {
                statement.setString(1, fileKey);
                try (ResultSet result = statement.executeQuery()) {
                    return result.next() ? Optional.of(readRow(result)) : Optional.empty();
                }
            }
        });
    }

    /** Atomically claims currently available rows. The returned previous state enables compensation. */
    public List<ClaimedRow> claimAvailable(String tenantId, int limit, Instant now) {
        if (limit <= 0) throw new IllegalArgumentException("limit must be positive");
        if (now == null) throw new IllegalArgumentException("now must not be null");
        long nowMillis = now.toEpochMilli();
        return write(tenantId, connection -> {
            List<ClaimedRow> claimed = new ArrayList<>();
            try (PreparedStatement select = connection.prepareStatement(
                    """
                    SELECT * FROM files
                    WHERE tenant_id=?
                      AND status IN (?, ?)
                      AND (available_at_ms IS NULL OR available_at_ms <= ?)
                    ORDER BY created_at_ms, file_key
                    LIMIT ?
                    """)) {
                select.setString(1, tenantId);
                select.setInt(2, FileProcessingStatus.PENDING.ordinal());
                select.setInt(3, FileProcessingStatus.FAILED.ordinal());
                select.setLong(4, nowMillis);
                select.setInt(5, limit);
                try (ResultSet result = select.executeQuery()) {
                    while (result.next()) {
                        FileRow before = readRow(result);
                        FileProcessingStatus previousStatus = before.status();
                        Long previousAvailableAt = before.availableAtMillis();
                        UUID leaseId = UUID.randomUUID();
                        try (PreparedStatement update = connection.prepareStatement(
                                """
                                UPDATE files
                                SET status=?, lease_id=?, processing_started_at_ms=?, available_at_ms=NULL,
                                    row_version=row_version+1
                                WHERE tenant_id=? AND file_key=?
                                  AND status IN (?, ?)
                                  AND (available_at_ms IS NULL OR available_at_ms <= ?)
                                """)) {
                            update.setInt(1, FileProcessingStatus.PROCESSING.ordinal());
                            update.setString(2, leaseId.toString());
                            update.setLong(3, nowMillis);
                            update.setString(4, tenantId);
                            update.setString(5, before.fileKey());
                            update.setInt(6, FileProcessingStatus.PENDING.ordinal());
                            update.setInt(7, FileProcessingStatus.FAILED.ordinal());
                            update.setLong(8, nowMillis);
                            if (update.executeUpdate() != 1) continue;
                        }
                        claimed.add(new ClaimedRow(
                                find(connection, before.fileKey()).orElseThrow(), previousStatus, previousAvailableAt));
                    }
                }
            }
            return List.copyOf(claimed);
        });
    }

    public List<FileRow> processingBefore(String tenantId, Instant cutoff, int limit) {
        if (cutoff == null) throw new IllegalArgumentException("cutoff must not be null");
        if (limit <= 0) throw new IllegalArgumentException("limit must be positive");
        return read(tenantId, connection -> {
            List<FileRow> rows = new ArrayList<>();
            try (PreparedStatement statement = connection.prepareStatement(
                    """
                    SELECT * FROM files
                    WHERE tenant_id=? AND status=? AND processing_started_at_ms IS NOT NULL
                      AND processing_started_at_ms <= ?
                    ORDER BY processing_started_at_ms, file_key
                    LIMIT ?
                    """)) {
                statement.setString(1, tenantId);
                statement.setInt(2, FileProcessingStatus.PROCESSING.ordinal());
                statement.setLong(3, cutoff.toEpochMilli());
                statement.setInt(4, limit);
                try (ResultSet result = statement.executeQuery()) {
                    while (result.next()) rows.add(readRow(result));
                }
            }
            return List.copyOf(rows);
        });
    }

    /** Compensates a pre-committed claim when its journal event cannot be admitted. */
    public boolean rollbackClaim(
            String tenantId,
            String fileKey,
            UUID leaseId,
            FileProcessingStatus previousStatus,
            Long previousAvailableAtMillis) {
        return write(tenantId, connection -> {
            try (PreparedStatement statement = connection.prepareStatement(
                    """
                    UPDATE files
                    SET status=?, lease_id=NULL, processing_started_at_ms=NULL, available_at_ms=?,
                        row_version=row_version+1
                    WHERE tenant_id=? AND file_key=? AND status=? AND lease_id=?
                    """)) {
                statement.setInt(1, previousStatus.ordinal());
                if (previousAvailableAtMillis == null) statement.setObject(2, null);
                else statement.setLong(2, previousAvailableAtMillis);
                statement.setString(3, tenantId);
                statement.setString(4, fileKey);
                statement.setInt(5, FileProcessingStatus.PROCESSING.ordinal());
                statement.setString(6, leaseId.toString());
                return statement.executeUpdate() == 1;
            }
        });
    }

    public List<FileRow> activeFiles(String tenantId) {
        return read(tenantId, connection -> {
            List<FileRow> rows = new ArrayList<>();
            try (PreparedStatement statement =
                    connection.prepareStatement("SELECT * FROM files WHERE tenant_id=? ORDER BY file_key")) {
                statement.setString(1, tenantId);
                try (ResultSet result = statement.executeQuery()) {
                    while (result.next()) {
                        rows.add(readRow(result));
                    }
                }
            }
            return List.copyOf(rows);
        });
    }

    public long appliedEventCount(String tenantId) {
        return read(tenantId, connection -> {
            try (PreparedStatement statement = connection.prepareStatement("SELECT COUNT(*) FROM applied_events");
                    ResultSet result = statement.executeQuery()) {
                result.next();
                return result.getLong(1);
            }
        });
    }

    long nowMillis() {
        return clock.instant().toEpochMilli();
    }

    static boolean markApplied(Connection connection, QueueEventRecord event, long nowMillis) throws SQLException {
        try (PreparedStatement statement =
                connection.prepareStatement("SELECT sequence_number FROM applied_events WHERE event_id=?")) {
            statement.setString(1, event.eventId().toString());
            try (ResultSet result = statement.executeQuery()) {
                if (result.next()) {
                    if (result.getLong(1) != event.sequenceNumber()) {
                        throw new ProjectionException("Event ID was already applied with a different sequence");
                    }
                    return false;
                }
            }
        }
        try (PreparedStatement statement =
                connection.prepareStatement("SELECT event_id FROM applied_events WHERE sequence_number=?")) {
            statement.setLong(1, event.sequenceNumber());
            try (ResultSet result = statement.executeQuery()) {
                if (result.next()) {
                    throw new ProjectionException("Sequence was already applied by a different event");
                }
            }
        }
        try (PreparedStatement statement = connection.prepareStatement(
                "INSERT INTO applied_events(event_id, sequence_number, applied_at_ms) VALUES(?, ?, ?)")) {
            statement.setString(1, event.eventId().toString());
            statement.setLong(2, event.sequenceNumber());
            statement.setLong(3, nowMillis);
            statement.executeUpdate();
        }
        return true;
    }

    public record FileRow(
            String fileKey,
            String tenantId,
            String volumeId,
            String physicalPath,
            String logicalDirectory,
            long fileSize,
            long createdAtMillis,
            FileProcessingStatus status,
            int retryCount,
            Long lastFailedAtMillis,
            String lastError,
            String leaseId,
            Long processingStartedAtMillis,
            Long completedAtMillis,
            Long deleteSucceededAtMillis,
            Long deadLetteredAtMillis,
            Long availableAtMillis,
            String originalFileName,
            String fileExtension,
            String metadataJson,
            long lastEventSequence,
            long rowVersion) {}

    public record ClaimedRow(FileRow row, FileProcessingStatus previousStatus, Long previousAvailableAtMillis) {}

    static FileRow readRow(ResultSet result) throws SQLException {
        return new FileRow(
                result.getString("file_key"),
                result.getString("tenant_id"),
                result.getString("volume_id"),
                result.getString("physical_path"),
                result.getString("logical_directory"),
                result.getLong("file_size"),
                result.getLong("created_at_ms"),
                FileProcessingStatus.values()[result.getInt("status")],
                result.getInt("retry_count"),
                nullable(result, "last_failed_at_ms"),
                result.getString("last_error"),
                result.getString("lease_id"),
                nullable(result, "processing_started_at_ms"),
                nullable(result, "completed_at_ms"),
                nullable(result, "delete_succeeded_at_ms"),
                nullable(result, "dead_lettered_at_ms"),
                nullable(result, "available_at_ms"),
                result.getString("original_file_name"),
                result.getString("file_extension"),
                result.getString("metadata_json"),
                result.getLong("last_event_sequence"),
                result.getLong("row_version"));
    }

    private static Long nullable(ResultSet result, String name) throws SQLException {
        long value = result.getLong(name);
        return result.wasNull() ? null : value;
    }

    private static Optional<FileRow> find(Connection connection, String fileKey) throws SQLException {
        try (PreparedStatement statement = connection.prepareStatement("SELECT * FROM files WHERE file_key=?")) {
            statement.setString(1, fileKey);
            try (ResultSet result = statement.executeQuery()) {
                return result.next() ? Optional.of(readRow(result)) : Optional.empty();
            }
        }
    }

    private static ReentrantLock[] createLocks() {
        ReentrantLock[] locks = new ReentrantLock[LOCK_STRIPES];
        for (int index = 0; index < locks.length; index++) locks[index] = new ReentrantLock();
        return locks;
    }

    private static ReentrantLock lockFor(String tenantId) {
        return LOCKS[Math.floorMod(tenantId.hashCode(), LOCK_STRIPES)];
    }
}
