package io.github.cocosip.stow.api;

import io.github.cocosip.stow.model.CleanupStatistics;
import io.github.cocosip.stow.model.DatabaseHealthReport;
import io.github.cocosip.stow.model.DatabaseOptimizationResult;
import io.github.cocosip.stow.model.DatabaseRebuildResult;
import java.time.Duration;

public interface StorageMaintenance {

    CleanupStatistics cleanupCompleted(Duration olderThan);

    CleanupStatistics cleanupPermanentlyFailed(Duration olderThan);

    CleanupStatistics reclaimTimedOutProcessing(Duration timeout);

    CleanupStatistics recoverOrphans(String tenantId);

    CleanupStatistics recoverAllOrphans();

    void reconcileQuota(String tenantId);

    void reconcileAllQuotas();

    DatabaseHealthReport checkDatabases();

    DatabaseRebuildResult rebuildMetadata(String tenantId);

    DatabaseRebuildResult rebuildQuota(String tenantId);

    DatabaseOptimizationResult optimizeDatabases();

    CleanupStatistics cleanupInvalidDatabaseBackups();

    CleanupStatistics cleanupEmptyDirectories();

    CleanupStatistics cleanupJunkFiles();

    /**
     * Removes metadata rows whose physical file no longer exists and releases their
     * quota charges. Rows on unavailable or unhealthy volumes are skipped, and a row
     * whose canonical volume path exists is repaired in place instead of removed.
     */
    default CleanupStatistics cleanupOrphanedMetadata() {
        throw new UnsupportedOperationException("cleanupOrphanedMetadata is not supported by this implementation");
    }
}
