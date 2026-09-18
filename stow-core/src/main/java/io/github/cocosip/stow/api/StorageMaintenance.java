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
}
