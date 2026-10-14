package io.github.cocosip.stow.internal.recovery;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.cocosip.stow.model.DatabaseHealthReport;
import io.github.cocosip.stow.model.HealthStatus;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.time.Clock;
import org.junit.jupiter.api.Test;

class DatabaseHealthServiceTest {

    @Test
    void reportsUpForHealthyDatabase() throws Exception {
        Path root = Files.createTempDirectory(Path.of("target"), "health-up-");
        Path tenant = root.resolve("tenant-a");
        Files.createDirectories(tenant);
        try (Connection connection = DriverManager.getConnection("jdbc:sqlite:" + tenant.resolve("metadata.db"));
                Statement statement = connection.createStatement()) {
            statement.execute("CREATE TABLE marker(id INTEGER)");
        }

        DatabaseHealthReport report =
                new DatabaseHealthService(root, root.resolve("absent-quota"), Clock.systemUTC()).checkDatabases();

        assertThat(report.databases().get("metadata/tenant-a").status()).isEqualTo(HealthStatus.UP);
        assertThat(report.status()).isEqualTo(HealthStatus.UP);
    }

    @Test
    void reportsDownForCorruptDatabase() throws Exception {
        Path root = Files.createTempDirectory(Path.of("target"), "health-corrupt-");
        Path tenant = root.resolve("tenant-a");
        Files.createDirectories(tenant);
        Files.write(tenant.resolve("metadata.db"), new byte[] {1, 2, 3});

        DatabaseHealthReport report =
                new DatabaseHealthService(root, root.resolve("absent-quota"), Clock.systemUTC()).checkDatabases();

        // Corruption evidence still reports DOWN so startup recovery rebuilds the file.
        assertThat(report.databases().get("metadata/tenant-a").status()).isEqualTo(HealthStatus.DOWN);
        assertThat(report.status()).isEqualTo(HealthStatus.DOWN);
    }

    @Test
    void reportsDegradedForTransientlyUnopenableDatabase() throws Exception {
        Path root = Files.createTempDirectory(Path.of("target"), "health-cantopen-");
        Path tenant = root.resolve("tenant-a");
        Files.createDirectories(tenant);
        // A database path that cannot be opened (a directory) is a transient failure,
        // not corruption: no rebuild may be triggered for it.
        Files.createDirectories(tenant.resolve("metadata.db"));

        DatabaseHealthReport report =
                new DatabaseHealthService(root, root.resolve("absent-quota"), Clock.systemUTC()).checkDatabases();

        assertThat(report.databases().get("metadata/tenant-a").status()).isEqualTo(HealthStatus.DEGRADED);
        assertThat(report.status()).isEqualTo(HealthStatus.DEGRADED);
    }
}
