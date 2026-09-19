package io.github.cocosip.stow.internal.cleanup;

import io.github.cocosip.stow.model.CleanupStatistics;
import io.github.cocosip.stow.model.MaintenanceError;
import java.time.Clock;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public final class CleanupStatisticsBuilder {

    private final Instant startedAt;
    private final Set<String> tenants = new HashSet<>();
    private final List<MaintenanceError> errors = new ArrayList<>();
    private final Clock clock;
    private long scanned;
    private long succeeded;
    private long skipped;
    private long failed;
    private long releasedBytes;

    public CleanupStatisticsBuilder(Clock clock) {
        this.clock = clock;
        startedAt = clock.instant();
    }

    public void tenant(String tenantId) {
        tenants.add(tenantId);
    }

    public void scanned() {
        scanned++;
    }

    public void succeeded(String tenantId, long bytes) {
        tenant(tenantId);
        succeeded++;
        releasedBytes += Math.max(0, bytes);
    }

    public void skipped() {
        skipped++;
    }

    public void failed(String tenantId, String operation, Exception exception) {
        tenant(tenantId);
        failed++;
        String summary = exception.getMessage() == null ? exception.getClass().getSimpleName() : exception.getMessage();
        errors.add(new MaintenanceError(tenantId, operation, summary));
    }

    public void error(String tenantId, String operation, String summary) {
        tenant(tenantId);
        failed++;
        errors.add(new MaintenanceError(tenantId, operation, summary));
    }

    public void merge(CleanupStatistics result, String tenantId) {
        tenant(tenantId);
        scanned += result.scannedCount();
        succeeded += result.succeededCount();
        skipped += result.skippedCount();
        failed += result.failedCount();
        releasedBytes += result.releasedBytes();
        errors.addAll(result.errors());
    }

    public CleanupStatistics build() {
        return new CleanupStatistics(
                startedAt,
                clock.instant(),
                scanned,
                succeeded,
                skipped,
                failed,
                releasedBytes,
                tenants.size(),
                List.copyOf(errors));
    }
}
