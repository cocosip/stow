package io.github.cocosip.stow.internal.quota;

import io.github.cocosip.stow.config.SqliteConfiguration;
import io.github.cocosip.stow.exception.DirectoryQuotaExceededException;
import io.github.cocosip.stow.exception.ProjectionException;
import io.github.cocosip.stow.exception.TenantQuotaExceededException;
import io.github.cocosip.stow.internal.projection.SqliteMetadataProjectionStore;
import io.github.cocosip.stow.model.DirectoryQuota;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Clock;
import java.util.Optional;
import java.util.UUID;
import java.util.function.ToLongFunction;
import java.util.regex.Pattern;

public final class SqliteQuotaRepository {

    private static final Pattern FILE_KEY = Pattern.compile("[0-9a-f]{32}");
    private final QuotaDatabaseExecutor database;
    private final QuotaEventLedger events = new QuotaEventLedger();

    public SqliteQuotaRepository(
            Path quotaDirectory,
            SqliteConfiguration sqliteConfiguration,
            Clock clock,
            ToLongFunction<String> initialTenantLimit) {
        database = new QuotaDatabaseExecutor(quotaDirectory, sqliteConfiguration, clock, initialTenantLimit);
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
                throw new TenantQuotaExceededException(tenantId, tenant.currentCount(), tenant.maxCount());
            }
            if (directory.enabled() && directory.maxCount() != 0 && directory.currentCount() >= directory.maxCount()) {
                throw new DirectoryQuotaExceededException(normalized, directory.currentCount(), directory.maxCount());
            }

            incrementTenant(connection, tenant);
            incrementDirectory(connection, normalized, directory);
            QuotaReservation reservation =
                    new QuotaReservation(UUID.randomUUID().toString(), fileKey, normalized);
            insertReservation(connection, reservation);
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
        events.validate(eventId, sequenceNumber);
        write(tenantId, connection -> {
            if (!events.mark(connection, eventId, sequenceNumber, nowMillis())) {
                return null;
            }
            // The reservation row is intentionally kept until release: it remembers the
            // directory the charge was made against so a later DELETE release decrements
            // the same directory even if the projected event lost the original one.
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
            deleteReservation(connection, reservationId);
            return null;
        });
    }

    public void release(String tenantId, String eventId, long sequenceNumber, String fileKey, String logicalDirectory) {
        requireFileKey(fileKey);
        events.validate(eventId, sequenceNumber);
        write(tenantId, connection -> {
            if (!events.mark(connection, eventId, sequenceNumber, nowMillis())) {
                return null;
            }
            // Release against the charged directory: the surviving reservation row is
            // authoritative, the event directory is only the fallback.
            String charged = reservationDirectoryByFileKey(connection, fileKey)
                    .orElse(normalizeDirectory(tenantId, logicalDirectory));
            decrementCounts(connection, charged);
            try (PreparedStatement statement =
                    connection.prepareStatement("DELETE FROM quota_reservations WHERE file_key=?")) {
                statement.setString(1, fileKey);
                statement.executeUpdate();
            }
            return null;
        });
    }

    /** Charges the counts unconditionally, bypassing limit checks; used by orphan recovery. */
    public QuotaReservation forceReserve(String tenantId, String fileKey, String logicalDirectory) {
        requireFileKey(fileKey);
        String normalized = normalizeDirectory(tenantId, logicalDirectory);
        return write(tenantId, connection -> {
            ensureDirectory(connection, normalized);
            incrementTenant(connection, tenantRow(connection));
            incrementDirectory(connection, normalized, directoryRow(connection, normalized));
            QuotaReservation reservation =
                    new QuotaReservation(UUID.randomUUID().toString(), fileKey, normalized);
            insertReservation(connection, reservation);
            return reservation;
        });
    }

    /**
     * Reconciles persisted reservations against projected facts once projections are
     * current: an established fact keeps the charge (its reservation row is retained
     * for the eventual release), a missing fact rolls the charge back.
     */
    public void reconcileReservations(String tenantId, java.util.Set<String> activeFileKeys) {
        write(tenantId, connection -> {
            for (QuotaReservation reservation : allReservations(connection)) {
                if (activeFileKeys.contains(reservation.fileKey())) continue;
                decrementCounts(connection, reservation.logicalDirectory());
                deleteReservation(connection, reservation.reservationId());
            }
            return null;
        });
    }

    public long directoryLimit(String tenantId, String logicalDirectory) {
        String normalized = normalizeDirectory(tenantId, logicalDirectory);
        return read(tenantId, connection -> {
            try (PreparedStatement statement =
                    connection.prepareStatement("SELECT max_count FROM directory_quotas WHERE logical_directory=?")) {
                statement.setString(1, normalized);
                try (ResultSet result = statement.executeQuery()) {
                    return result.next() ? result.getLong(1) : 0L;
                }
            }
        });
    }

    /**
     * Recomputes counts from the projected active set. Explicit directory limits are
     * preserved (rebuilding changes counts, never policy), and in-flight reservations
     * keep their charges so a concurrent consume or rollback stays consistent.
     */
    public void rebuildFromMetadata(String tenantId, java.util.List<SqliteMetadataProjectionStore.FileRow> files) {
        write(tenantId, connection -> {
            long now = nowMillis();
            try (PreparedStatement zero = connection.prepareStatement(
                    "UPDATE directory_quotas SET current_count=0, updated_at_ms=?, row_version=row_version+1")) {
                zero.setLong(1, now);
                zero.executeUpdate();
            }
            java.util.Map<String, Long> counts = new java.util.HashMap<>();
            for (SqliteMetadataProjectionStore.FileRow file : files)
                counts.merge(file.logicalDirectory(), 1L, Long::sum);
            // Reservation rows for active files were already consumed (the row is kept
            // for the release); only reservations without an active file still carry a
            // live charge and must contribute to the recomputed counts.
            java.util.Set<String> activeKeys = new java.util.HashSet<>();
            for (SqliteMetadataProjectionStore.FileRow file : files) activeKeys.add(file.fileKey());
            long liveReservations = 0;
            try (PreparedStatement reservationCounts = connection.prepareStatement(
                            "SELECT file_key, logical_directory, COUNT(*) FROM quota_reservations GROUP BY file_key, logical_directory");
                    ResultSet result = reservationCounts.executeQuery()) {
                while (result.next()) {
                    if (activeKeys.contains(result.getString(1))) continue;
                    counts.merge(result.getString(2), result.getLong(3), Long::sum);
                    liveReservations += result.getLong(3);
                }
            }
            for (var entry : counts.entrySet()) {
                ensureDirectory(connection, entry.getKey());
                try (PreparedStatement update = connection.prepareStatement(
                        "UPDATE directory_quotas SET current_count=?, updated_at_ms=?, row_version=row_version+1 WHERE logical_directory=?")) {
                    update.setLong(1, entry.getValue());
                    update.setLong(2, now);
                    update.setString(3, entry.getKey());
                    update.executeUpdate();
                }
            }
            try (PreparedStatement prune =
                    connection.prepareStatement("DELETE FROM directory_quotas WHERE current_count=0 AND max_count=0")) {
                prune.executeUpdate();
            }
            try (PreparedStatement update = connection.prepareStatement(
                    "UPDATE tenant_quota SET current_count=?, updated_at_ms=?, row_version=row_version+1 WHERE singleton_id=1")) {
                update.setLong(1, files.size() + liveReservations);
                update.setLong(2, now);
                update.executeUpdate();
            }
            return null;
        });
    }

    /** Deletes applied-quota-event ledger rows up to a compacted sequence. */
    public void pruneAppliedQuotaEvents(String tenantId, long throughSequence) {
        write(tenantId, connection -> {
            try (PreparedStatement statement =
                    connection.prepareStatement("DELETE FROM applied_quota_events WHERE sequence_number<=?")) {
                statement.setLong(1, throughSequence);
                statement.executeUpdate();
            }
            return null;
        });
    }

    private <T> T read(String tenantId, SqlOperation<T> operation) {
        return database.read(tenantId, operation::run);
    }

    private <T> T write(String tenantId, SqlOperation<T> operation) {
        return database.write(tenantId, operation::run);
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

    private void insertReservation(Connection connection, QuotaReservation reservation) throws SQLException {
        try (PreparedStatement statement = connection.prepareStatement(
                """
                INSERT INTO quota_reservations(reservation_id, file_key, logical_directory, created_at_ms)
                VALUES(?, ?, ?, ?)
                """)) {
            statement.setString(1, reservation.reservationId());
            statement.setString(2, reservation.fileKey());
            statement.setString(3, reservation.logicalDirectory());
            statement.setLong(4, nowMillis());
            statement.executeUpdate();
        }
    }

    private static void deleteReservation(Connection connection, String reservationId) throws SQLException {
        try (PreparedStatement statement =
                connection.prepareStatement("DELETE FROM quota_reservations WHERE reservation_id=?")) {
            statement.setString(1, reservationId);
            statement.executeUpdate();
        }
    }

    private static java.util.List<QuotaReservation> allReservations(Connection connection) throws SQLException {
        try (PreparedStatement statement = connection.prepareStatement(
                        "SELECT reservation_id, file_key, logical_directory FROM quota_reservations");
                ResultSet result = statement.executeQuery()) {
            java.util.List<QuotaReservation> reservations = new java.util.ArrayList<>();
            while (result.next()) {
                reservations.add(new QuotaReservation(result.getString(1), result.getString(2), result.getString(3)));
            }
            return reservations;
        }
    }

    private static Optional<String> reservationDirectoryByFileKey(Connection connection, String fileKey)
            throws SQLException {
        try (PreparedStatement statement =
                connection.prepareStatement("SELECT logical_directory FROM quota_reservations WHERE file_key=?")) {
            statement.setString(1, fileKey);
            try (ResultSet result = statement.executeQuery()) {
                return result.next() ? Optional.of(result.getString(1)) : Optional.empty();
            }
        }
    }

    private long nowMillis() {
        return database.nowMillis();
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

    @FunctionalInterface
    private interface SqlOperation<T> {
        T run(Connection connection) throws SQLException;
    }

    private record TenantRow(long currentCount, long maxCount, long rowVersion) {}

    private record DirectoryRow(long currentCount, long maxCount, boolean enabled, long rowVersion) {}
}
