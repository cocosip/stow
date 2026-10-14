package io.github.cocosip.stow.internal.recovery;

import io.github.cocosip.stow.model.ComponentHealth;
import io.github.cocosip.stow.model.DatabaseHealthReport;
import io.github.cocosip.stow.model.HealthStatus;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.DriverManager;
import java.time.Clock;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.Map;

public final class DatabaseHealthService {
    private final Path metadataRoot;
    private final Path quotaRoot;
    private final Clock clock;

    public DatabaseHealthService(Path metadataRoot, Path quotaRoot, Clock clock) {
        this.metadataRoot = metadataRoot.toAbsolutePath().normalize();
        this.quotaRoot = quotaRoot.toAbsolutePath().normalize();
        this.clock = clock == null ? Clock.systemUTC() : clock;
    }

    public DatabaseHealthReport checkDatabases() {
        Instant checkedAt = clock.instant();
        Map<String, ComponentHealth> databases = new LinkedHashMap<>();
        checkRoot(metadataRoot, "metadata", databases, checkedAt);
        checkRoot(quotaRoot, "quota", databases, checkedAt);
        HealthStatus overall = databases.values().stream().anyMatch(value -> value.status() == HealthStatus.DOWN)
                ? HealthStatus.DOWN
                : databases.values().stream().anyMatch(value -> value.status() == HealthStatus.DEGRADED)
                        ? HealthStatus.DEGRADED
                        : HealthStatus.UP;
        return new DatabaseHealthReport(overall, checkedAt, databases);
    }

    public DatabaseHealthReport check() {
        return checkDatabases();
    }

    private void checkRoot(Path root, String name, Map<String, ComponentHealth> result, Instant checkedAt) {
        if (!Files.exists(root)) return;
        try (var tenants = Files.list(root)) {
            tenants.filter(Files::isDirectory).forEach(tenant -> {
                Path database = tenant.resolve(name.equals("metadata") ? "metadata.db" : "quotas.db");
                if (!Files.exists(database)) return;
                result.put(name + "/" + tenant.getFileName(), checkDatabase(database, checkedAt));
            });
        } catch (Exception exception) {
            result.put(name, new ComponentHealth(HealthStatus.DOWN, exception.getMessage(), checkedAt));
        }
    }

    private ComponentHealth checkDatabase(Path database, Instant checkedAt) {
        // Only corruption evidence reports DOWN (and lets startup recovery rebuild):
        // a failed quick_check verdict, or SQLITE_IOERR/CORRUPT/NOTADB — the Locus
        // rebuild triggers. A busy, locked, or otherwise transiently unopenable
        // database is temporarily unavailable, not corrupt: retry briefly and report
        // DEGRADED instead of DOWN.
        String lastTransientMessage = null;
        for (int attempt = 0; attempt < 3; attempt++) {
            try (var connection = DriverManager.getConnection("jdbc:sqlite:" + database);
                    var statement = connection.createStatement();
                    var rows = statement.executeQuery("PRAGMA quick_check")) {
                String result = rows.next() ? rows.getString(1) : "";
                return new ComponentHealth(
                        "ok".equalsIgnoreCase(result) ? HealthStatus.UP : HealthStatus.DOWN,
                        result.isBlank() ? "quick_check returned no result" : result,
                        checkedAt);
            } catch (Exception exception) {
                if (isCorruption(exception)) {
                    return new ComponentHealth(
                            HealthStatus.DOWN,
                            exception.getMessage() == null ? "database check failed" : exception.getMessage(),
                            checkedAt);
                }
                lastTransientMessage =
                        exception.getMessage() == null ? "database is temporarily unavailable" : exception.getMessage();
                try {
                    Thread.sleep(100L * (attempt + 1));
                } catch (InterruptedException interrupted) {
                    Thread.currentThread().interrupt();
                    break;
                }
            }
        }
        return new ComponentHealth(
                HealthStatus.DEGRADED,
                lastTransientMessage == null ? "database check failed" : lastTransientMessage,
                checkedAt);
    }

    private static boolean isCorruption(Exception exception) {
        if (exception instanceof java.sql.SQLException sql) {
            int code = Math.abs(sql.getErrorCode());
            return code == 10 || code == 11 || code == 26; // SQLITE_IOERR / SQLITE_CORRUPT / SQLITE_NOTADB
        }
        return false;
    }
}
