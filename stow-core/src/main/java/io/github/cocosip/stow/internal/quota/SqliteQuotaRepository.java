package io.github.cocosip.stow.internal.quota;

import io.github.cocosip.stow.config.SqliteConfiguration;
import io.github.cocosip.stow.exception.DatabaseRecoveryException;
import io.github.cocosip.stow.exception.DirectoryQuotaExceededException;
import io.github.cocosip.stow.exception.ProjectionException;
import io.github.cocosip.stow.exception.StowInterruptedException;
import io.github.cocosip.stow.exception.TenantQuotaExceededException;
import io.github.cocosip.stow.internal.sqlite.SqliteConnectionFactory;
import io.github.cocosip.stow.internal.sqlite.SqliteSchemaManager;
import io.github.cocosip.stow.model.DirectoryQuota;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Clock;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.ToLongFunction;
import java.util.regex.Pattern;

public final class SqliteQuotaRepository {

    private static final int SCHEMA_VERSION = 1;
    private static final Pattern FILE_KEY = Pattern.compile("[0-9a-f]{32}");
    private static final ConcurrentHashMap<Path, ReentrantLock> LOCKS = new ConcurrentHashMap<>();
    private static final List<String> CREATE_STATEMENTS = List.of(
            """
            CREATE TABLE tenant_quota (
                singleton_id                INTEGER PRIMARY KEY CHECK(singleton_id = 1),
                current_count               INTEGER NOT NULL DEFAULT 0 CHECK(current_count >= 0),
                max_count                   INTEGER NOT NULL DEFAULT 0 CHECK(max_count >= 0),
                updated_at_ms               INTEGER NOT NULL,
                row_version                 INTEGER NOT NULL DEFAULT 0
            )
            """,
            """
            CREATE TABLE directory_quotas (
                logical_directory           TEXT PRIMARY KEY NOT NULL,
                current_count               INTEGER NOT NULL DEFAULT 0 CHECK(current_count >= 0),
                max_count                   INTEGER NOT NULL DEFAULT 0 CHECK(max_count >= 0),
                enabled                     INTEGER NOT NULL DEFAULT 1 CHECK(enabled IN (0,1)),
                created_at_ms               INTEGER NOT NULL,
                updated_at_ms               INTEGER NOT NULL,
                row_version                 INTEGER NOT NULL DEFAULT 0
            )
            """,
            """
            CREATE TABLE quota_reservations (
                reservation_id              TEXT PRIMARY KEY NOT NULL,
                file_key                    TEXT NOT NULL UNIQUE,
                logical_directory           TEXT NOT NULL,
                created_at_ms               INTEGER NOT NULL
            )
            """,
            """
            CREATE TABLE applied_quota_events (
                event_id                    TEXT PRIMARY KEY NOT NULL,
                sequence_number             INTEGER NOT NULL UNIQUE,
                applied_at_ms               INTEGER NOT NULL
            )
            """);

    private final SqliteConnectionFactory connections;
    private final SqliteSchemaManager schema = new SqliteSchemaManager(SCHEMA_VERSION);
    private final Clock clock;
    private final ToLongFunction<String> initialTenantLimit;

    public SqliteQuotaRepository(
            Path quotaDirectory,
            SqliteConfiguration sqliteConfiguration,
            Clock clock,
            ToLongFunction<String> initialTenantLimit) {
        connections = new SqliteConnectionFactory(quotaDirectory, "quotas.db", sqliteConfiguration);
        this.clock = java.util.Objects.requireNonNull(clock, "clock");
        this.initialTenantLimit = java.util.Objects.requireNonNull(initialTenantLimit, "initialTenantLimit");
    }

    public long tenantCurrentCount(String tenantId) {
        return read(tenantId, connection -> tenantRow(connection).currentCount());
    }

    public long tenantLimit(String tenantId) {
        return read(tenantId, connection -> tenantRow(connection).maxCount());
    }

    public void setTenantLimit(String tenantId, long maxFiles) {
        requireNonNegative("maxFiles", maxFiles);
        write(tenantId, connection -> {
            TenantRow current = tenantRow(connection);
            try (PreparedStatement statement = connection.prepareStatement(
                    """
                    UPDATE tenant_quota
                    SET max_count=?, updated_at_ms=?, row_version=row_version+1
                    WHERE singleton_id=1 AND row_version=?
                    """)) {
                statement.setLong(1, maxFiles);
                statement.setLong(2, nowMillis());
                statement.setLong(3, current.rowVersion());
                requireSingleOptimisticUpdate(statement.executeUpdate(), "tenant quota limit");
            }
            return null;
        });
    }

    public DirectoryQuota directoryQuota(String tenantId, String logicalDirectory) {
        String normalized = normalizeDirectory(tenantId, logicalDirectory);
        return write(tenantId, connection -> {
            ensureDirectory(connection, normalized);
            DirectoryRow row = directoryRow(connection, normalized);
            return new DirectoryQuota(tenantId, normalized, row.currentCount(), row.maxCount(), row.enabled());
        });
    }

    public void setDirectoryLimit(String tenantId, String logicalDirectory, long maxFiles) {
        requireNonNegative("maxFiles", maxFiles);
        String normalized = normalizeDirectory(tenantId, logicalDirectory);
        write(tenantId, connection -> {
            ensureDirectory(connection, normalized);
            DirectoryRow current = directoryRow(connection, normalized);
            try (PreparedStatement statement = connection.prepareStatement(
                    """
                    UPDATE directory_quotas
                    SET max_count=?, updated_at_ms=?, row_version=row_version+1
                    WHERE logical_directory=? AND row_version=?
                    """)) {
                statement.setLong(1, maxFiles);
                statement.setLong(2, nowMillis());
                statement.setString(3, normalized);
                statement.setLong(4, current.rowVersion());
                requireSingleOptimisticUpdate(statement.executeUpdate(), "directory quota limit");
            }
            return null;
        });
    }

    public QuotaReservation reserve(String tenantId, String fileKey, String logicalDirectory) {
        requireFileKey(fileKey);
        String normalized = normalizeDirectory(tenantId, logicalDirectory);
        return write(tenantId, connection -> {
            ensureDirectory(connection, normalized);
            TenantRow tenant = tenantRow(connection);
            DirectoryRow directory = directoryRow(connection, normalized);
            if (tenant.maxCount() != 0 && tenant.currentCount() >= tenant.maxCount()) {
                throw new TenantQuotaExceededException("Tenant quota exceeded: " + tenantId);
            }
            if (directory.enabled() && directory.maxCount() != 0 && directory.currentCount() >= directory.maxCount()) {
                throw new DirectoryQuotaExceededException("Directory quota exceeded: " + normalized);
            }

            incrementTenant(connection, tenant);
            incrementDirectory(connection, normalized, directory);
            QuotaReservation reservation =
                    new QuotaReservation(UUID.randomUUID().toString(), fileKey, normalized);
            try (PreparedStatement statement = connection.prepareStatement(
                    """
                    INSERT INTO quota_reservations(reservation_id, file_key, logical_directory, created_at_ms)
                    VALUES(?, ?, ?, ?)
                    """)) {
                statement.setString(1, reservation.reservationId());
                statement.setString(2, fileKey);
                statement.setString(3, normalized);
                statement.setLong(4, nowMillis());
                statement.executeUpdate();
            }
            return reservation;
        });
    }

    public Optional<QuotaReservation> reservation(String tenantId, String fileKey) {
        requireFileKey(fileKey);
        return read(tenantId, connection -> {
            try (PreparedStatement statement = connection.prepareStatement(
                    """
                    SELECT reservation_id, file_key, logical_directory
                    FROM quota_reservations WHERE file_key=?
                    """)) {
                statement.setString(1, fileKey);
                try (ResultSet result = statement.executeQuery()) {
                    if (!result.next()) {
                        return Optional.empty();
                    }
                    return Optional.of(new QuotaReservation(
                            result.getString("reservation_id"),
                            result.getString("file_key"),
                            result.getString("logical_directory")));
                }
            }
        });
    }

    public void consume(String tenantId, String eventId, long sequenceNumber, String reservationId) {
        requireText("reservationId", reservationId);
        write(tenantId, connection -> {
            if (!markEvent(connection, eventId, sequenceNumber)) {
                return null;
            }
            try (PreparedStatement statement =
                    connection.prepareStatement("DELETE FROM quota_reservations WHERE reservation_id=?")) {
                statement.setString(1, reservationId);
                statement.executeUpdate();
            }
            return null;
        });
    }

    public void rollback(String tenantId, String reservationId) {
        requireText("reservationId", reservationId);
        write(tenantId, connection -> {
            Optional<QuotaReservation> reservation = reservationById(connection, reservationId);
            if (reservation.isEmpty()) {
                return null;
            }
            decrementCounts(connection, reservation.orElseThrow().logicalDirectory());
            try (PreparedStatement statement =
                    connection.prepareStatement("DELETE FROM quota_reservations WHERE reservation_id=?")) {
                statement.setString(1, reservationId);
                statement.executeUpdate();
            }
            return null;
        });
    }

    public void release(String tenantId, String eventId, long sequenceNumber, String fileKey, String logicalDirectory) {
        requireFileKey(fileKey);
        String normalized = normalizeDirectory(tenantId, logicalDirectory);
        write(tenantId, connection -> {
            if (!markEvent(connection, eventId, sequenceNumber)) {
                return null;
            }
            decrementCounts(connection, normalized);
            return null;
        });
    }

    private <T> T read(String tenantId, SqlOperation<T> operation) {
        return execute(tenantId, false, operation);
    }

    private <T> T write(String tenantId, SqlOperation<T> operation) {
        return execute(tenantId, true, operation);
    }

    private <T> T execute(String tenantId, boolean transactional, SqlOperation<T> operation) {
        Path databasePath = connections.databasePath(tenantId);
        ReentrantLock lock = LOCKS.computeIfAbsent(databasePath, ignored -> new ReentrantLock());
        acquireInterruptibly(lock);
        try (Connection connection = connections.open(tenantId)) {
            schema.ensureSchema(connection, CREATE_STATEMENTS);
            if (transactional) {
                connection.setAutoCommit(false);
            }
            try {
                ensureTenant(connection, tenantId);
                T result = operation.run(connection);
                if (transactional) {
                    connection.commit();
                }
                return result;
            } catch (SQLException | RuntimeException failure) {
                if (transactional) {
                    rollback(connection, failure);
                }
                throw failure;
            }
        } catch (SQLException exception) {
            throw new DatabaseRecoveryException("Unable to access quota database for tenant " + tenantId, exception);
        } finally {
            lock.unlock();
        }
    }

    private void ensureTenant(Connection connection, String tenantId) throws SQLException {
        long initialLimit = initialTenantLimit.applyAsLong(tenantId);
        requireNonNegative("initial tenant quota", initialLimit);
        try (PreparedStatement statement = connection.prepareStatement(
                """
                INSERT OR IGNORE INTO tenant_quota(singleton_id, current_count, max_count, updated_at_ms, row_version)
                VALUES(1, 0, ?, ?, 0)
                """)) {
            statement.setLong(1, initialLimit);
            statement.setLong(2, nowMillis());
            statement.executeUpdate();
        }
    }

    private void ensureDirectory(Connection connection, String logicalDirectory) throws SQLException {
        try (PreparedStatement statement = connection.prepareStatement(
                """
                INSERT OR IGNORE INTO directory_quotas(
                    logical_directory, current_count, max_count, enabled, created_at_ms, updated_at_ms, row_version)
                VALUES(?, 0, 0, 1, ?, ?, 0)
                """)) {
            long now = nowMillis();
            statement.setString(1, logicalDirectory);
            statement.setLong(2, now);
            statement.setLong(3, now);
            statement.executeUpdate();
        }
    }

    private static TenantRow tenantRow(Connection connection) throws SQLException {
        try (PreparedStatement statement = connection.prepareStatement(
                        "SELECT current_count, max_count, row_version FROM tenant_quota WHERE singleton_id=1");
                ResultSet result = statement.executeQuery()) {
            if (!result.next()) {
                throw new SQLException("Tenant quota row is missing");
            }
            return new TenantRow(result.getLong(1), result.getLong(2), result.getLong(3));
        }
    }

    private static DirectoryRow directoryRow(Connection connection, String logicalDirectory) throws SQLException {
        try (PreparedStatement statement = connection.prepareStatement(
                """
                SELECT current_count, max_count, enabled, row_version
                FROM directory_quotas WHERE logical_directory=?
                """)) {
            statement.setString(1, logicalDirectory);
            try (ResultSet result = statement.executeQuery()) {
                if (!result.next()) {
                    throw new SQLException("Directory quota row is missing");
                }
                return new DirectoryRow(result.getLong(1), result.getLong(2), result.getInt(3) == 1, result.getLong(4));
            }
        }
    }

    private void incrementTenant(Connection connection, TenantRow current) throws SQLException {
        try (PreparedStatement statement = connection.prepareStatement(
                """
                UPDATE tenant_quota
                SET current_count=current_count+1, updated_at_ms=?, row_version=row_version+1
                WHERE singleton_id=1 AND row_version=?
                """)) {
            statement.setLong(1, nowMillis());
            statement.setLong(2, current.rowVersion());
            requireSingleOptimisticUpdate(statement.executeUpdate(), "tenant reservation");
        }
    }

    private void incrementDirectory(Connection connection, String logicalDirectory, DirectoryRow current)
            throws SQLException {
        try (PreparedStatement statement = connection.prepareStatement(
                """
                UPDATE directory_quotas
                SET current_count=current_count+1, updated_at_ms=?, row_version=row_version+1
                WHERE logical_directory=? AND row_version=?
                """)) {
            statement.setLong(1, nowMillis());
            statement.setString(2, logicalDirectory);
            statement.setLong(3, current.rowVersion());
            requireSingleOptimisticUpdate(statement.executeUpdate(), "directory reservation");
        }
    }

    private void decrementCounts(Connection connection, String logicalDirectory) throws SQLException {
        TenantRow tenant = tenantRow(connection);
        ensureDirectory(connection, logicalDirectory);
        DirectoryRow directory = directoryRow(connection, logicalDirectory);
        if (tenant.currentCount() == 0 || directory.currentCount() == 0) {
            throw new ProjectionException("Quota count underflow for directory " + logicalDirectory);
        }
        try (PreparedStatement statement = connection.prepareStatement(
                """
                UPDATE tenant_quota
                SET current_count=current_count-1, updated_at_ms=?, row_version=row_version+1
                WHERE singleton_id=1 AND row_version=? AND current_count>0
                """)) {
            statement.setLong(1, nowMillis());
            statement.setLong(2, tenant.rowVersion());
            requireSingleOptimisticUpdate(statement.executeUpdate(), "tenant quota release");
        }
        try (PreparedStatement statement = connection.prepareStatement(
                """
                UPDATE directory_quotas
                SET current_count=current_count-1, updated_at_ms=?, row_version=row_version+1
                WHERE logical_directory=? AND row_version=? AND current_count>0
                """)) {
            statement.setLong(1, nowMillis());
            statement.setString(2, logicalDirectory);
            statement.setLong(3, directory.rowVersion());
            requireSingleOptimisticUpdate(statement.executeUpdate(), "directory quota release");
        }
    }

    private Optional<QuotaReservation> reservationById(Connection connection, String reservationId)
            throws SQLException {
        try (PreparedStatement statement = connection.prepareStatement(
                """
                SELECT reservation_id, file_key, logical_directory
                FROM quota_reservations WHERE reservation_id=?
                """)) {
            statement.setString(1, reservationId);
            try (ResultSet result = statement.executeQuery()) {
                if (!result.next()) {
                    return Optional.empty();
                }
                return Optional.of(new QuotaReservation(result.getString(1), result.getString(2), result.getString(3)));
            }
        }
    }

    private boolean markEvent(Connection connection, String eventId, long sequenceNumber) throws SQLException {
        requireText("eventId", eventId);
        if (sequenceNumber < 0) {
            throw new IllegalArgumentException("sequenceNumber must not be negative");
        }
        try (PreparedStatement statement =
                connection.prepareStatement("SELECT sequence_number FROM applied_quota_events WHERE event_id=?")) {
            statement.setString(1, eventId);
            try (ResultSet result = statement.executeQuery()) {
                if (result.next()) {
                    if (result.getLong(1) != sequenceNumber) {
                        throw new ProjectionException("Quota event ID was already applied with a different sequence");
                    }
                    return false;
                }
            }
        }
        try (PreparedStatement statement =
                connection.prepareStatement("SELECT event_id FROM applied_quota_events WHERE sequence_number=?")) {
            statement.setLong(1, sequenceNumber);
            try (ResultSet result = statement.executeQuery()) {
                if (result.next()) {
                    throw new ProjectionException("Quota sequence was already applied by a different event");
                }
            }
        }
        try (PreparedStatement statement = connection.prepareStatement(
                """
                INSERT INTO applied_quota_events(event_id, sequence_number, applied_at_ms)
                VALUES(?, ?, ?)
                """)) {
            statement.setString(1, eventId);
            statement.setLong(2, sequenceNumber);
            statement.setLong(3, nowMillis());
            statement.executeUpdate();
        }
        return true;
    }

    private long nowMillis() {
        return clock.instant().toEpochMilli();
    }

    private static String normalizeDirectory(String tenantId, String logicalDirectory) {
        return new DirectoryQuota(tenantId, logicalDirectory, 0, 0, true).logicalDirectory();
    }

    private static void requireFileKey(String fileKey) {
        if (fileKey == null || !FILE_KEY.matcher(fileKey).matches()) {
            throw new IllegalArgumentException("fileKey must contain exactly 32 lowercase hexadecimal characters");
        }
    }

    private static void requireText(String name, String value) {
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException(name + " must not be blank");
        }
    }

    private static void requireNonNegative(String name, long value) {
        if (value < 0) {
            throw new IllegalArgumentException(name + " must not be negative");
        }
    }

    private static void requireSingleOptimisticUpdate(int updated, String operation) {
        if (updated != 1) {
            throw new ProjectionException("Concurrent row-version conflict during " + operation);
        }
    }

    private static void acquireInterruptibly(ReentrantLock lock) {
        try {
            lock.lockInterruptibly();
        } catch (InterruptedException exception) {
            throw new StowInterruptedException("Interrupted while waiting for the quota repository lock", exception);
        }
    }

    private static void rollback(Connection connection, Throwable failure) {
        try {
            connection.rollback();
        } catch (SQLException rollbackFailure) {
            failure.addSuppressed(rollbackFailure);
        }
    }

    @FunctionalInterface
    private interface SqlOperation<T> {
        T run(Connection connection) throws SQLException;
    }

    private record TenantRow(long currentCount, long maxCount, long rowVersion) {}

    private record DirectoryRow(long currentCount, long maxCount, boolean enabled, long rowVersion) {}
}
