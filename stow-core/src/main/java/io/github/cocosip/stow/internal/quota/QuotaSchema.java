package io.github.cocosip.stow.internal.quota;

import io.github.cocosip.stow.internal.sqlite.SqliteSchemaManager;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;

final class QuotaSchema {

    private static final int VERSION = 1;
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

    private final SqliteSchemaManager schemaManager = new SqliteSchemaManager(VERSION);

    void ensure(Connection connection) throws SQLException {
        schemaManager.ensureSchema(connection, CREATE_STATEMENTS);
    }
}
