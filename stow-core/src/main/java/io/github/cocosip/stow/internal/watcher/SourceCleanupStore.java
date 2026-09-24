package io.github.cocosip.stow.internal.watcher;

import io.github.cocosip.stow.config.SourceCleanupConfiguration;
import io.github.cocosip.stow.config.SqliteConfiguration;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

public final class SourceCleanupStore implements AutoCloseable {

    enum ReservationStatus {
        RESERVED,
        ALREADY_ACTIVE,
        CAPACITY_FULL
    }

    record Reservation(ReservationStatus status, SourceCleanupJob job) {}

    record OptimizationResult(long sizeBefore, long sizeAfter) {}

    private static final String CREATE_SCHEMA =
            """
            CREATE TABLE IF NOT EXISTS source_cleanup_jobs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                watcher_id TEXT NOT NULL,
                tenant_id TEXT NOT NULL,
                source_path TEXT NOT NULL,
                fingerprint_version INTEGER NOT NULL,
                fingerprint_path TEXT NOT NULL,
                fingerprint_size INTEGER NOT NULL,
                fingerprint_modified_ms INTEGER NOT NULL,
                fingerprint_created_ms INTEGER NOT NULL,
                fingerprint_hash TEXT NOT NULL,
                import_operation_id TEXT NOT NULL,
                file_key TEXT,
                action INTEGER NOT NULL,
                move_target_path TEXT,
                failure_directory TEXT,
                max_attempts INTEGER NOT NULL,
                retry_initial_delay_ms INTEGER NOT NULL,
                retry_max_delay_ms INTEGER NOT NULL,
                attempt_count INTEGER NOT NULL,
                state INTEGER NOT NULL,
                next_attempt_ms INTEGER,
                last_error TEXT,
                created_at_ms INTEGER NOT NULL,
                updated_at_ms INTEGER NOT NULL,
                lease_until_ms INTEGER,
                UNIQUE(watcher_id, source_path)
            );
            CREATE INDEX IF NOT EXISTS idx_source_cleanup_due
                ON source_cleanup_jobs(state, next_attempt_ms, lease_until_ms, updated_at_ms);
            """;

    private final SourceCleanupConfiguration configuration;
    private final SqliteConfiguration sqlite;
    private final Clock clock;
    private final Path databasePath;

    public SourceCleanupStore(SourceCleanupConfiguration configuration, SqliteConfiguration sqlite, Clock clock) {
        this.configuration = Objects.requireNonNull(configuration, "configuration");
        this.sqlite = Objects.requireNonNull(sqlite, "sqlite");
        this.clock = Objects.requireNonNull(clock, "clock");
        databasePath = configuration.databasePath().toAbsolutePath().normalize();
    }

    Reservation tryReserve(SourceCleanupJob requested) {
        Objects.requireNonNull(requested, "requested");
        try (Connection connection = open()) {
            beginImmediate(connection);
            try {
                Optional<SourceCleanupJob> existing = find(connection, requested.watcherId(), requested.sourcePath());
                if (existing.isPresent() && existing.orElseThrow().fingerprint().equals(requested.fingerprint())) {
                    commit(connection);
                    return new Reservation(ReservationStatus.ALREADY_ACTIVE, existing.orElseThrow());
                }
                if (existing.isPresent())
                    delete(connection, existing.orElseThrow().id());
                if (count(connection) >= configuration.maxActiveJobs()) {
                    commit(connection);
                    return new Reservation(ReservationStatus.CAPACITY_FULL, null);
                }
                long id = insert(connection, requested);
                SourceCleanupJob reserved = withId(requested, id);
                commit(connection);
                return new Reservation(ReservationStatus.RESERVED, reserved);
            } catch (SQLException | RuntimeException exception) {
                rollback(connection, exception);
                throw exception;
            }
        } catch (SQLException exception) {
            throw failure("Unable to reserve source cleanup work", exception);
        }
    }

    Optional<SourceCleanupJob> findActive(Path sourcePath) {
        Path normalized = sourcePath.toAbsolutePath().normalize();
        try (Connection connection = open();
                PreparedStatement statement =
                        connection.prepareStatement("SELECT * FROM source_cleanup_jobs WHERE source_path=? LIMIT 1")) {
            statement.setString(1, normalized.toString());
            try (ResultSet result = statement.executeQuery()) {
                return result.next() ? Optional.of(read(result)) : Optional.empty();
            }
        } catch (SQLException exception) {
            throw failure("Unable to read source cleanup work", exception);
        }
    }

    int activeCount() {
        try (Connection connection = open()) {
            return count(connection);
        } catch (SQLException exception) {
            throw failure("Unable to count source cleanup work", exception);
        }
    }

    SourceCleanupJob activate(long id, String fileKey) {
        Instant now = clock.instant();
        try (Connection connection = open()) {
            beginImmediate(connection);
            try {
                SourceCleanupJob current = find(connection, id)
                        .orElseThrow(
                                () -> new IllegalStateException("Source cleanup reservation does not exist: " + id));
                SourceCleanupState state = current.action() == SourceCleanupAction.KEEP
                        ? SourceCleanupState.TERMINAL_KEEP
                        : SourceCleanupState.PENDING;
                Instant nextAttempt = state == SourceCleanupState.PENDING ? now : null;
                SourceCleanupJob activated =
                        current.transition(fileKey, 0, state, nextAttempt, null, now, current.moveTargetPath());
                update(connection, activated);
                commit(connection);
                return activated;
            } catch (SQLException | RuntimeException exception) {
                rollback(connection, exception);
                throw exception;
            }
        } catch (SQLException exception) {
            throw failure("Unable to activate source cleanup work", exception);
        }
    }

    List<SourceCleanupJob> claimDue(Instant now, int limit, Duration leaseDuration) {
        if (limit <= 0) throw new IllegalArgumentException("limit must be positive");
        if (leaseDuration == null || leaseDuration.isZero() || leaseDuration.isNegative()) {
            throw new IllegalArgumentException("leaseDuration must be positive");
        }
        try (Connection connection = open()) {
            beginImmediate(connection);
            try {
                List<SourceCleanupJob> jobs = new ArrayList<>();
                try (PreparedStatement statement = connection.prepareStatement(
                        """
                        SELECT * FROM source_cleanup_jobs
                        WHERE state IN (?, ?, ?)
                          AND (next_attempt_ms IS NULL OR next_attempt_ms <= ?)
                          AND (lease_until_ms IS NULL OR lease_until_ms <= ?)
                        ORDER BY updated_at_ms, id LIMIT ?
                        """)) {
                    statement.setInt(1, SourceCleanupState.PENDING.ordinal());
                    statement.setInt(2, SourceCleanupState.RETRYING.ordinal());
                    statement.setInt(3, SourceCleanupState.MOVE_PENDING.ordinal());
                    statement.setLong(4, now.toEpochMilli());
                    statement.setLong(5, now.toEpochMilli());
                    statement.setInt(6, limit);
                    try (ResultSet result = statement.executeQuery()) {
                        while (result.next()) jobs.add(read(result));
                    }
                }
                Instant leaseUntil = now.plus(leaseDuration);
                List<SourceCleanupJob> claimed = new ArrayList<>(jobs.size());
                for (SourceCleanupJob job : jobs) {
                    SourceCleanupJob leased = job.withLease(now, leaseUntil);
                    update(connection, leased);
                    claimed.add(leased);
                }
                commit(connection);
                return List.copyOf(claimed);
            } catch (SQLException | RuntimeException exception) {
                rollback(connection, exception);
                throw exception;
            }
        } catch (SQLException exception) {
            throw failure("Unable to claim source cleanup work", exception);
        }
    }

    void update(SourceCleanupJob job) {
        try (Connection connection = open()) {
            update(connection, job);
        } catch (SQLException exception) {
            throw failure("Unable to update source cleanup work", exception);
        }
    }

    void remove(long id) {
        try (Connection connection = open()) {
            delete(connection, id);
        } catch (SQLException exception) {
            throw failure("Unable to remove source cleanup work", exception);
        }
    }

    int releaseStaleImports(Instant cutoff, int limit) {
        return deleteBounded(
                "state=? AND updated_at_ms<=?",
                statement -> {
                    statement.setInt(1, SourceCleanupState.IMPORTING.ordinal());
                    statement.setLong(2, cutoff.toEpochMilli());
                    statement.setInt(3, limit);
                },
                limit);
    }

    int pruneTerminal(Instant cutoff, int limit) {
        return deleteBounded(
                "state IN (?, ?) AND updated_at_ms<=?",
                statement -> {
                    statement.setInt(1, SourceCleanupState.TERMINAL_KEEP.ordinal());
                    statement.setInt(2, SourceCleanupState.TERMINAL_FAILED.ordinal());
                    statement.setLong(3, cutoff.toEpochMilli());
                    statement.setInt(4, limit);
                },
                limit);
    }

    OptimizationResult optimize() {
        try (Connection connection = open()) {
            long before = Files.exists(databasePath) ? Files.size(databasePath) : 0;
            try (Statement statement = connection.createStatement()) {
                statement.execute("VACUUM");
            }
            long after = Files.exists(databasePath) ? Files.size(databasePath) : 0;
            return new OptimizationResult(before, after);
        } catch (SQLException | IOException exception) {
            throw failure("Unable to optimize source cleanup database", exception);
        }
    }

    @Override
    public void close() {}

    private int deleteBounded(String predicate, StatementBinder binder, int limit) {
        if (limit <= 0) throw new IllegalArgumentException("limit must be positive");
        String sql = "DELETE FROM source_cleanup_jobs WHERE id IN (SELECT id FROM source_cleanup_jobs WHERE "
                + predicate + " ORDER BY updated_at_ms, id LIMIT ?)";
        try (Connection connection = open();
                PreparedStatement statement = connection.prepareStatement(sql)) {
            binder.bind(statement);
            return statement.executeUpdate();
        } catch (SQLException exception) {
            throw failure("Unable to prune source cleanup work", exception);
        }
    }

    private Connection open() throws SQLException {
        long timeoutNanos = sqlite.busyTimeout().toNanos();
        long deadline = System.nanoTime() + timeoutNanos;
        SQLException lastFailure;
        do {
            try {
                return openOnce();
            } catch (SQLException exception) {
                lastFailure = exception;
                if (exception.getErrorCode() != 5 || System.nanoTime() >= deadline) throw exception;
                try {
                    Thread.sleep(10);
                } catch (InterruptedException interrupted) {
                    Thread.currentThread().interrupt();
                    exception.addSuppressed(interrupted);
                    throw exception;
                }
            }
        } while (System.nanoTime() < deadline);
        throw lastFailure;
    }

    private Connection openOnce() throws SQLException {
        Path parent = databasePath.getParent();
        if (parent == null) throw new SQLException("Source cleanup database path must have a parent directory");
        try {
            Files.createDirectories(parent);
        } catch (IOException exception) {
            throw new SQLException("Unable to create source cleanup database directory", exception);
        }
        Connection connection = DriverManager.getConnection("jdbc:sqlite:" + databasePath);
        try (Statement statement = connection.createStatement()) {
            statement.execute("PRAGMA busy_timeout=" + sqlite.busyTimeout().toMillis());
            statement.execute("PRAGMA journal_mode=" + sqlite.journalMode().name());
            statement.execute("PRAGMA synchronous=" + sqlite.synchronousMode().name());
            statement.execute("PRAGMA cache_size=" + sqlite.cacheSizeKb());
            statement.execute("PRAGMA foreign_keys=ON");
            for (String sql : CREATE_SCHEMA.split(";")) {
                if (!sql.isBlank()) statement.execute(sql);
            }
            return connection;
        } catch (SQLException exception) {
            try {
                connection.close();
            } catch (SQLException closeFailure) {
                exception.addSuppressed(closeFailure);
            }
            throw exception;
        }
    }

    private static Optional<SourceCleanupJob> find(Connection connection, String watcherId, Path sourcePath)
            throws SQLException {
        try (PreparedStatement statement =
                connection.prepareStatement("SELECT * FROM source_cleanup_jobs WHERE watcher_id=? AND source_path=?")) {
            statement.setString(1, watcherId);
            statement.setString(2, sourcePath.toAbsolutePath().normalize().toString());
            try (ResultSet result = statement.executeQuery()) {
                return result.next() ? Optional.of(read(result)) : Optional.empty();
            }
        }
    }

    private static Optional<SourceCleanupJob> find(Connection connection, long id) throws SQLException {
        try (PreparedStatement statement =
                connection.prepareStatement("SELECT * FROM source_cleanup_jobs WHERE id=?")) {
            statement.setLong(1, id);
            try (ResultSet result = statement.executeQuery()) {
                return result.next() ? Optional.of(read(result)) : Optional.empty();
            }
        }
    }

    private static int count(Connection connection) throws SQLException {
        try (Statement statement = connection.createStatement();
                ResultSet result = statement.executeQuery("SELECT COUNT(*) FROM source_cleanup_jobs")) {
            result.next();
            return result.getInt(1);
        }
    }

    private static long insert(Connection connection, SourceCleanupJob job) throws SQLException {
        try (PreparedStatement statement = connection.prepareStatement(
                """
                INSERT INTO source_cleanup_jobs(
                    watcher_id, tenant_id, source_path, fingerprint_version, fingerprint_path,
                    fingerprint_size, fingerprint_modified_ms, fingerprint_created_ms, fingerprint_hash,
                    import_operation_id, file_key, action, move_target_path, failure_directory,
                    max_attempts, retry_initial_delay_ms, retry_max_delay_ms, attempt_count, state,
                    next_attempt_ms, last_error, created_at_ms, updated_at_ms, lease_until_ms)
                VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """)) {
            bind(statement, job);
            statement.executeUpdate();
        }
        try (Statement statement = connection.createStatement();
                ResultSet result = statement.executeQuery("SELECT last_insert_rowid()")) {
            result.next();
            return result.getLong(1);
        }
    }

    private static void update(Connection connection, SourceCleanupJob job) throws SQLException {
        try (PreparedStatement statement = connection.prepareStatement(
                """
                UPDATE source_cleanup_jobs SET
                    tenant_id=?, fingerprint_version=?, fingerprint_path=?, fingerprint_size=?,
                    fingerprint_modified_ms=?, fingerprint_created_ms=?, fingerprint_hash=?,
                    import_operation_id=?, file_key=?, action=?, move_target_path=?, failure_directory=?,
                    max_attempts=?, retry_initial_delay_ms=?, retry_max_delay_ms=?, attempt_count=?, state=?,
                    next_attempt_ms=?, last_error=?, updated_at_ms=?, lease_until_ms=?
                WHERE id=?
                """)) {
            bindUpdate(statement, job);
            if (statement.executeUpdate() != 1) {
                throw new IllegalStateException("Source cleanup job does not exist: " + job.id());
            }
        }
    }

    private static void delete(Connection connection, long id) throws SQLException {
        try (PreparedStatement statement = connection.prepareStatement("DELETE FROM source_cleanup_jobs WHERE id=?")) {
            statement.setLong(1, id);
            statement.executeUpdate();
        }
    }

    private static void bind(PreparedStatement statement, SourceCleanupJob job) throws SQLException {
        int index = 1;
        statement.setString(index++, job.watcherId());
        statement.setString(index++, job.tenantId());
        statement.setString(index++, job.sourcePath().toString());
        index = bindFingerprint(statement, index, job.fingerprint());
        statement.setString(index++, job.importOperationId());
        statement.setString(index++, job.fileKey());
        statement.setInt(index++, job.action().ordinal());
        statement.setString(index++, path(job.moveTargetPath()));
        statement.setString(index++, path(job.failureDirectory()));
        statement.setInt(index++, job.maxAttempts());
        statement.setLong(index++, job.retryInitialDelay().toMillis());
        statement.setLong(index++, job.retryMaxDelay().toMillis());
        statement.setInt(index++, job.attemptCount());
        statement.setInt(index++, job.state().ordinal());
        setInstant(statement, index++, job.nextAttemptAt());
        statement.setString(index++, job.lastError());
        statement.setLong(index++, job.createdAt().toEpochMilli());
        statement.setLong(index++, job.updatedAt().toEpochMilli());
        setInstant(statement, index, job.leaseUntil());
    }

    private static void bindUpdate(PreparedStatement statement, SourceCleanupJob job) throws SQLException {
        int index = 1;
        statement.setString(index++, job.tenantId());
        index = bindFingerprint(statement, index, job.fingerprint());
        statement.setString(index++, job.importOperationId());
        statement.setString(index++, job.fileKey());
        statement.setInt(index++, job.action().ordinal());
        statement.setString(index++, path(job.moveTargetPath()));
        statement.setString(index++, path(job.failureDirectory()));
        statement.setInt(index++, job.maxAttempts());
        statement.setLong(index++, job.retryInitialDelay().toMillis());
        statement.setLong(index++, job.retryMaxDelay().toMillis());
        statement.setInt(index++, job.attemptCount());
        statement.setInt(index++, job.state().ordinal());
        setInstant(statement, index++, job.nextAttemptAt());
        statement.setString(index++, job.lastError());
        statement.setLong(index++, job.updatedAt().toEpochMilli());
        setInstant(statement, index++, job.leaseUntil());
        statement.setLong(index, job.id());
    }

    private static int bindFingerprint(PreparedStatement statement, int index, SourceFingerprint fingerprint)
            throws SQLException {
        statement.setInt(index++, fingerprint.version());
        statement.setString(index++, fingerprint.path());
        statement.setLong(index++, fingerprint.size());
        statement.setLong(index++, fingerprint.lastModifiedMillis());
        statement.setLong(index++, fingerprint.creationTimeMillis());
        statement.setString(index++, fingerprint.sampleSha256());
        return index;
    }

    private static SourceCleanupJob read(ResultSet result) throws SQLException {
        SourceFingerprint fingerprint = new SourceFingerprint(
                result.getInt("fingerprint_version"),
                result.getString("fingerprint_path"),
                result.getLong("fingerprint_size"),
                result.getLong("fingerprint_modified_ms"),
                result.getLong("fingerprint_created_ms"),
                result.getString("fingerprint_hash"));
        return new SourceCleanupJob(
                result.getLong("id"),
                result.getString("watcher_id"),
                result.getString("tenant_id"),
                Path.of(result.getString("source_path")),
                fingerprint,
                result.getString("import_operation_id"),
                result.getString("file_key"),
                SourceCleanupAction.values()[result.getInt("action")],
                nullablePath(result, "move_target_path"),
                nullablePath(result, "failure_directory"),
                result.getInt("max_attempts"),
                Duration.ofMillis(result.getLong("retry_initial_delay_ms")),
                Duration.ofMillis(result.getLong("retry_max_delay_ms")),
                result.getInt("attempt_count"),
                SourceCleanupState.values()[result.getInt("state")],
                nullableInstant(result, "next_attempt_ms"),
                result.getString("last_error"),
                Instant.ofEpochMilli(result.getLong("created_at_ms")),
                Instant.ofEpochMilli(result.getLong("updated_at_ms")),
                nullableInstant(result, "lease_until_ms"));
    }

    private static SourceCleanupJob withId(SourceCleanupJob job, long id) {
        return new SourceCleanupJob(
                id,
                job.watcherId(),
                job.tenantId(),
                job.sourcePath(),
                job.fingerprint(),
                job.importOperationId(),
                job.fileKey(),
                job.action(),
                job.moveTargetPath(),
                job.failureDirectory(),
                job.maxAttempts(),
                job.retryInitialDelay(),
                job.retryMaxDelay(),
                job.attemptCount(),
                job.state(),
                job.nextAttemptAt(),
                job.lastError(),
                job.createdAt(),
                job.updatedAt(),
                job.leaseUntil());
    }

    private static Path nullablePath(ResultSet result, String name) throws SQLException {
        String value = result.getString(name);
        return value == null ? null : Path.of(value);
    }

    private static Instant nullableInstant(ResultSet result, String name) throws SQLException {
        long value = result.getLong(name);
        return result.wasNull() ? null : Instant.ofEpochMilli(value);
    }

    private static void setInstant(PreparedStatement statement, int index, Instant value) throws SQLException {
        if (value == null) statement.setObject(index, null);
        else statement.setLong(index, value.toEpochMilli());
    }

    private static String path(Path value) {
        return value == null ? null : value.toString();
    }

    private static void beginImmediate(Connection connection) throws SQLException {
        try (Statement statement = connection.createStatement()) {
            statement.execute("BEGIN IMMEDIATE");
        }
    }

    private static void commit(Connection connection) throws SQLException {
        try (Statement statement = connection.createStatement()) {
            statement.execute("COMMIT");
        }
    }

    private static void rollback(Connection connection, Throwable failure) {
        try (Statement statement = connection.createStatement()) {
            statement.execute("ROLLBACK");
        } catch (SQLException rollbackFailure) {
            failure.addSuppressed(rollbackFailure);
        }
    }

    private static IllegalStateException failure(String message, Exception cause) {
        return new IllegalStateException(message, cause);
    }

    @FunctionalInterface
    private interface StatementBinder {
        void bind(PreparedStatement statement) throws SQLException;
    }
}
