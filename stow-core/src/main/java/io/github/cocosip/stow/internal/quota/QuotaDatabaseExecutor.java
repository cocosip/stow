package io.github.cocosip.stow.internal.quota;

import io.github.cocosip.stow.config.SqliteConfiguration;
import io.github.cocosip.stow.exception.DatabaseRecoveryException;
import io.github.cocosip.stow.exception.StowInterruptedException;
import io.github.cocosip.stow.internal.sqlite.SqliteConnectionFactory;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.time.Clock;
import java.util.Objects;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.ToLongFunction;

final class QuotaDatabaseExecutor {

    private static final int LOCK_STRIPES = 256;
    private static final ReentrantLock[] LOCKS = createLocks();

    private final SqliteConnectionFactory connections;
    private final QuotaSchema schema = new QuotaSchema();
    private final Clock clock;
    private final ToLongFunction<String> initialTenantLimit;

    QuotaDatabaseExecutor(
            Path quotaDirectory,
            SqliteConfiguration sqliteConfiguration,
            Clock clock,
            ToLongFunction<String> initialTenantLimit) {
        connections = new SqliteConnectionFactory(quotaDirectory, "quotas.db", sqliteConfiguration);
        this.clock = Objects.requireNonNull(clock, "clock");
        this.initialTenantLimit = Objects.requireNonNull(initialTenantLimit, "initialTenantLimit");
    }

    <T> T read(String tenantId, SqlOperation<T> operation) {
        return execute(tenantId, false, operation);
    }

    <T> T write(String tenantId, SqlOperation<T> operation) {
        return execute(tenantId, true, operation);
    }

    long nowMillis() {
        return clock.instant().toEpochMilli();
    }

    private <T> T execute(String tenantId, boolean transactional, SqlOperation<T> operation) {
        Path databasePath = connections.databasePath(tenantId);
        ReentrantLock lock = lockFor(databasePath);
        acquireInterruptibly(lock);
        try (Connection connection = connections.open(tenantId)) {
            schema.ensure(connection);
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
        if (initialLimit < 0) {
            throw new IllegalArgumentException("initial tenant quota must not be negative");
        }
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

    private static ReentrantLock[] createLocks() {
        ReentrantLock[] locks = new ReentrantLock[LOCK_STRIPES];
        for (int index = 0; index < locks.length; index++) {
            locks[index] = new ReentrantLock();
        }
        return locks;
    }

    private static ReentrantLock lockFor(Path databasePath) {
        return LOCKS[Math.floorMod(databasePath.hashCode(), LOCKS.length)];
    }

    private static void acquireInterruptibly(ReentrantLock lock) {
        try {
            lock.lockInterruptibly();
        } catch (InterruptedException exception) {
            throw new StowInterruptedException("Interrupted while waiting for the quota database lock", exception);
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
    interface SqlOperation<T> {
        T run(Connection connection) throws SQLException;
    }
}
