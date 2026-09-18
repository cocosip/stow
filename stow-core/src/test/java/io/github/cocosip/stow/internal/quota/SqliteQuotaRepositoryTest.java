package io.github.cocosip.stow.internal.quota;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.github.cocosip.stow.exception.DirectoryQuotaExceededException;
import io.github.cocosip.stow.exception.ProjectionException;
import io.github.cocosip.stow.exception.TenantQuotaExceededException;
import io.github.cocosip.stow.internal.sqlite.SqliteConnectionFactory;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.LinkedHashMap;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class SqliteQuotaRepositoryTest {

    private static final String TENANT_ID = "tenant-a";
    private static final String FILE_KEY = "0123456789abcdef0123456789abcdef";
    private static final Instant NOW = Instant.parse("2026-09-18T00:00:00Z");
    private static final Clock CLOCK = Clock.fixed(NOW, ZoneOffset.UTC);

    @TempDir
    Path temporaryDirectory;

    @Test
    void createsExactVersionOneQuotaSchema() throws Exception {
        SqliteQuotaRepository repository = repository(5);

        repository.tenantLimit(TENANT_ID);

        Path database = temporaryDirectory.resolve(TENANT_ID).resolve("quotas.db");
        try (Connection connection = DriverManager.getConnection("jdbc:sqlite:" + database);
                Statement statement = connection.createStatement()) {
            assertThat(queryLong(statement, "PRAGMA user_version")).isEqualTo(1);
            assertThat(tableSql(statement)).containsExactlyEntriesOf(expectedSchema());
        }
    }

    @Test
    void enforcesTenantAndDirectoryLimitsButTreatsZeroAsUnlimited() {
        SqliteQuotaRepository tenantLimited = repository(1);
        tenantLimited.reserve(TENANT_ID, FILE_KEY, "/incoming");

        assertThatThrownBy(() -> tenantLimited.reserve(TENANT_ID, secondFileKey(), "/other"))
                .isInstanceOf(TenantQuotaExceededException.class);

        SqliteQuotaRepository unlimited = repository(0);
        unlimited.setDirectoryLimit("tenant-unlimited", "/incoming", 0);
        unlimited.reserve("tenant-unlimited", FILE_KEY, "/incoming");
        unlimited.reserve("tenant-unlimited", secondFileKey(), "/incoming");
        assertThat(unlimited.tenantCurrentCount("tenant-unlimited")).isEqualTo(2);
        assertThat(unlimited.directoryQuota("tenant-unlimited", "/incoming").currentCount())
                .isEqualTo(2);

        unlimited.setDirectoryLimit("tenant-unlimited", "/limited", 1);
        unlimited.reserve("tenant-unlimited", thirdFileKey(), "/limited");
        assertThatThrownBy(() -> unlimited.reserve("tenant-unlimited", fourthFileKey(), "/limited"))
                .isInstanceOf(DirectoryQuotaExceededException.class);
    }

    @Test
    void managersNormalizeDirectoriesAndExposePersistedCountsAndLimits() {
        SqliteQuotaRepository repository = repository(7);
        DefaultTenantQuotaManager tenants = new DefaultTenantQuotaManager(repository);
        DefaultDirectoryQuotaManager directories = new DefaultDirectoryQuotaManager(repository);

        directories.setLimit(TENANT_ID, "incoming", 2);
        repository.reserve(TENANT_ID, FILE_KEY, "/incoming");

        assertThat(tenants.limit(TENANT_ID)).isEqualTo(7);
        assertThat(tenants.currentCount(TENANT_ID)).isEqualTo(1);
        assertThat(directories.get(TENANT_ID, "incoming")).satisfies(quota -> {
            assertThat(quota.logicalDirectory()).isEqualTo("/incoming");
            assertThat(quota.currentCount()).isEqualTo(1);
            assertThat(quota.maxFiles()).isEqualTo(2);
            assertThat(quota.enabled()).isTrue();
        });

        tenants.setLimit(TENANT_ID, 9);
        assertThat(tenants.limit(TENANT_ID)).isEqualTo(9);
        assertThatThrownBy(() -> directories.setLimit(TENANT_ID, "../unsafe", 1))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void consumeRollbackAndReleaseAreIdempotentAndUseRealJournalSequences() {
        SqliteQuotaRepository repository = repository(10);
        QuotaReservation consumed = repository.reserve(TENANT_ID, FILE_KEY, "incoming");

        repository.consume(TENANT_ID, "accepted-event", 41, consumed.reservationId());
        repository.consume(TENANT_ID, "accepted-event", 41, consumed.reservationId());
        assertThat(repository.tenantCurrentCount(TENANT_ID)).isEqualTo(1);

        assertThatThrownBy(() -> repository.consume(TENANT_ID, "different-event", 41, consumed.reservationId()))
                .isInstanceOf(ProjectionException.class);
        assertThatThrownBy(() -> repository.consume(TENANT_ID, "accepted-event", 42, consumed.reservationId()))
                .isInstanceOf(ProjectionException.class);

        repository.release(TENANT_ID, "delete-event", 42, FILE_KEY, "/incoming");
        repository.release(TENANT_ID, "delete-event", 42, FILE_KEY, "/incoming");
        assertThat(repository.tenantCurrentCount(TENANT_ID)).isZero();
        assertThat(repository.directoryQuota(TENANT_ID, "/incoming").currentCount())
                .isZero();

        QuotaReservation rolledBack = repository.reserve(TENANT_ID, secondFileKey(), "/other");
        repository.rollback(TENANT_ID, rolledBack.reservationId());
        repository.rollback(TENANT_ID, rolledBack.reservationId());
        assertThat(repository.tenantCurrentCount(TENANT_ID)).isZero();
        assertThat(repository.directoryQuota(TENANT_ID, "/other").currentCount())
                .isZero();
    }

    @Test
    void rejectsNonPositiveJournalSequencesWithoutChangingQuotaState() throws Exception {
        SqliteQuotaRepository repository = repository(10);
        QuotaReservation reservation = repository.reserve(TENANT_ID, FILE_KEY, "/incoming");

        assertThatThrownBy(() -> repository.consume(TENANT_ID, "zero-sequence", 0, reservation.reservationId()))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> repository.release(TENANT_ID, "negative-sequence", -1, FILE_KEY, "/incoming"))
                .isInstanceOf(IllegalArgumentException.class);

        assertThat(repository.tenantCurrentCount(TENANT_ID)).isEqualTo(1);
        assertThat(repository.directoryQuota(TENANT_ID, "/incoming").currentCount())
                .isEqualTo(1);
        assertThat(repository.reservation(TENANT_ID, FILE_KEY)).contains(reservation);
        assertThat(appliedEventCount()).isZero();
    }

    @Test
    void rejectsFirstConsumeWhenReservationDoesNotExistAndRollsBackEvent() throws Exception {
        SqliteQuotaRepository repository = repository(10);
        QuotaReservation reservation = repository.reserve(TENANT_ID, FILE_KEY, "/incoming");

        assertThatThrownBy(() -> repository.consume(TENANT_ID, "accepted-event", 41, "missing-reservation"))
                .isInstanceOf(ProjectionException.class);

        assertThat(repository.reservation(TENANT_ID, FILE_KEY)).contains(reservation);
        assertThat(repository.tenantCurrentCount(TENANT_ID)).isEqualTo(1);
        assertThat(appliedEventCount()).isZero();
    }

    @Test
    void reservationsPersistAcrossReopenAndCanBeConsumedOrRolledBack() {
        SqliteQuotaRepository first = repository(2);
        QuotaReservation toConsume = first.reserve(TENANT_ID, FILE_KEY, "incoming");
        QuotaReservation toRollback = first.reserve(TENANT_ID, secondFileKey(), "other");

        SqliteQuotaRepository reopened = repository(999);
        assertThat(reopened.reservation(TENANT_ID, FILE_KEY)).contains(toConsume);
        assertThat(reopened.reservation(TENANT_ID, secondFileKey())).contains(toRollback);
        assertThat(reopened.tenantLimit(TENANT_ID)).isEqualTo(2);

        reopened.consume(TENANT_ID, "accepted-event", 100, toConsume.reservationId());
        reopened.rollback(TENANT_ID, toRollback.reservationId());

        assertThat(reopened.reservation(TENANT_ID, FILE_KEY)).isEmpty();
        assertThat(reopened.reservation(TENANT_ID, secondFileKey())).isEmpty();
        assertThat(reopened.tenantCurrentCount(TENANT_ID)).isEqualTo(1);
        assertThat(reopened.directoryQuota(TENANT_ID, "/incoming").currentCount())
                .isEqualTo(1);
        assertThat(reopened.directoryQuota(TENANT_ID, "/other").currentCount()).isZero();
    }

    private SqliteQuotaRepository repository(long initialLimit) {
        return new SqliteQuotaRepository(
                temporaryDirectory, SqliteConnectionFactory.defaults(), CLOCK, ignored -> initialLimit);
    }

    private long appliedEventCount() throws Exception {
        Path database = temporaryDirectory.resolve(TENANT_ID).resolve("quotas.db");
        try (Connection connection = DriverManager.getConnection("jdbc:sqlite:" + database);
                Statement statement = connection.createStatement()) {
            return queryLong(statement, "SELECT COUNT(*) FROM applied_quota_events");
        }
    }

    private static long queryLong(Statement statement, String sql) throws Exception {
        try (ResultSet result = statement.executeQuery(sql)) {
            assertThat(result.next()).isTrue();
            return result.getLong(1);
        }
    }

    private static Map<String, String> tableSql(Statement statement) throws Exception {
        Map<String, String> tables = new LinkedHashMap<>();
        try (ResultSet result =
                statement.executeQuery("SELECT name, sql FROM sqlite_master WHERE type='table' ORDER BY name")) {
            while (result.next()) {
                tables.put(result.getString(1), normalizeSql(result.getString(2)));
            }
        }
        return tables;
    }

    private static Map<String, String> expectedSchema() {
        Map<String, String> tables = new LinkedHashMap<>();
        tables.put(
                "applied_quota_events",
                normalizeSql(
                        """
                        CREATE TABLE applied_quota_events (
                            event_id TEXT PRIMARY KEY NOT NULL,
                            sequence_number INTEGER NOT NULL UNIQUE,
                            applied_at_ms INTEGER NOT NULL
                        )
                        """));
        tables.put(
                "directory_quotas",
                normalizeSql(
                        """
                        CREATE TABLE directory_quotas (
                            logical_directory TEXT PRIMARY KEY NOT NULL,
                            current_count INTEGER NOT NULL DEFAULT 0 CHECK(current_count >= 0),
                            max_count INTEGER NOT NULL DEFAULT 0 CHECK(max_count >= 0),
                            enabled INTEGER NOT NULL DEFAULT 1 CHECK(enabled IN (0,1)),
                            created_at_ms INTEGER NOT NULL,
                            updated_at_ms INTEGER NOT NULL,
                            row_version INTEGER NOT NULL DEFAULT 0
                        )
                        """));
        tables.put(
                "quota_reservations",
                normalizeSql(
                        """
                        CREATE TABLE quota_reservations (
                            reservation_id TEXT PRIMARY KEY NOT NULL,
                            file_key TEXT NOT NULL UNIQUE,
                            logical_directory TEXT NOT NULL,
                            created_at_ms INTEGER NOT NULL
                        )
                        """));
        tables.put(
                "tenant_quota",
                normalizeSql(
                        """
                        CREATE TABLE tenant_quota (
                            singleton_id INTEGER PRIMARY KEY CHECK(singleton_id = 1),
                            current_count INTEGER NOT NULL DEFAULT 0 CHECK(current_count >= 0),
                            max_count INTEGER NOT NULL DEFAULT 0 CHECK(max_count >= 0),
                            updated_at_ms INTEGER NOT NULL,
                            row_version INTEGER NOT NULL DEFAULT 0
                        )
                        """));
        return tables;
    }

    private static String normalizeSql(String sql) {
        return sql.replaceAll("\\s+", " ").replace("( ", "(").replace(" )", ")").trim();
    }

    private static String secondFileKey() {
        return "1123456789abcdef0123456789abcdef";
    }

    private static String thirdFileKey() {
        return "2123456789abcdef0123456789abcdef";
    }

    private static String fourthFileKey() {
        return "3123456789abcdef0123456789abcdef";
    }
}
