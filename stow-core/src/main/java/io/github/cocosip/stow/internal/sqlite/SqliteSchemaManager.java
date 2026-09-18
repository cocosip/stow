package io.github.cocosip.stow.internal.sqlite;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;

public final class SqliteSchemaManager {

    private final int expectedVersion;

    public SqliteSchemaManager(int expectedVersion) {
        if (expectedVersion <= 0) {
            throw new IllegalArgumentException("expectedVersion must be greater than zero");
        }
        this.expectedVersion = expectedVersion;
    }

    public int userVersion(Connection connection) throws SQLException {
        try (Statement statement = connection.createStatement();
                ResultSet result = statement.executeQuery("PRAGMA user_version")) {
            if (!result.next()) {
                throw new SQLException("PRAGMA user_version returned no row");
            }
            return result.getInt(1);
        }
    }

    public void ensureSchema(Connection connection, List<String> createStatements) throws SQLException {
        int currentVersion = userVersion(connection);
        if (currentVersion == expectedVersion) {
            return;
        }
        if (currentVersion != 0) {
            throw new IllegalStateException(
                    "Unsupported SQLite schema version " + currentVersion + "; expected " + expectedVersion);
        }

        boolean initialAutoCommit = connection.getAutoCommit();
        connection.setAutoCommit(false);
        try (Statement statement = connection.createStatement()) {
            for (String sql : List.copyOf(createStatements)) {
                statement.execute(sql);
            }
            statement.execute("PRAGMA user_version=" + expectedVersion);
            connection.commit();
        } catch (SQLException | RuntimeException failure) {
            try {
                connection.rollback();
            } catch (SQLException rollbackFailure) {
                failure.addSuppressed(rollbackFailure);
            }
            throw failure;
        } finally {
            connection.setAutoCommit(initialAutoCommit);
        }
    }
}
