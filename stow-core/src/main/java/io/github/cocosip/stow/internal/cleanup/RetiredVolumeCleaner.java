package io.github.cocosip.stow.internal.cleanup;

import io.github.cocosip.stow.internal.projection.SqliteMetadataProjectionStore;
import io.github.cocosip.stow.internal.quota.SqliteQuotaRepository;
import io.github.cocosip.stow.model.CleanupStatistics;
import java.time.Clock;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;

/**
 * Applies the metadata policy for volumes that are intentionally retired
 * (Locus RetiredVolumeDisposition). {@code KEEP} leaves rows alone;
 * {@code PURGE_METADATA_ONLY} removes the projected rows that point at the retired
 * volume and releases their quota without touching physical storage — the residual
 * files stay on the retired volume and are never adopted back.
 */
public final class RetiredVolumeCleaner {

    private final SqliteMetadataProjectionStore metadata;
    private final SqliteQuotaRepository quota;
    private final Clock clock;

    public RetiredVolumeCleaner(SqliteMetadataProjectionStore metadata, SqliteQuotaRepository quota, Clock clock) {
        this.metadata = Objects.requireNonNull(metadata, "metadata");
        this.quota = Objects.requireNonNull(quota, "quota");
        this.clock = Objects.requireNonNull(clock, "clock");
    }

    public CleanupStatistics clean(Set<String> retiredVolumeIds, boolean purgeMetadataOnly, int maxFiles) {
        Objects.requireNonNull(retiredVolumeIds, "retiredVolumeIds");
        if (retiredVolumeIds.isEmpty()) return new CleanupStatisticsBuilder(clock).build();
        if (maxFiles <= 0) throw new IllegalArgumentException("maxFiles must be positive");
        CleanupStatisticsBuilder statistics = new CleanupStatisticsBuilder(clock);
        if (!purgeMetadataOnly) {
            // KEEP: rows pointing at the retired volume stay; nothing converges.
            return statistics.build();
        }
        for (String tenantId : metadata.tenantIds()) {
            statistics.tenant(tenantId);
            List<SqliteMetadataProjectionStore.FileRow> rows = metadata.activeFiles(tenantId);
            int purged = 0;
            for (SqliteMetadataProjectionStore.FileRow row : rows) {
                if (purged >= maxFiles) break;
                if (!retiredVolumeIds.contains(row.volumeId())) continue;
                statistics.scanned();
                if (metadata.removeFile(tenantId, row.fileKey(), row.rowVersion())) {
                    purgeQuota(tenantId);
                    statistics.succeeded(tenantId, 0);
                    purged++;
                } else {
                    // The row changed concurrently; the next cycle re-evaluates it.
                    statistics.skipped();
                }
            }
        }
        return statistics.build();
    }

    /** Releases the charges of rows removed by the purge (same primitive as orphan cleanup). */
    private void purgeQuota(String tenantId) {
        Set<String> activeKeys = new HashSet<>();
        for (SqliteMetadataProjectionStore.FileRow remaining : metadata.activeFiles(tenantId)) {
            activeKeys.add(remaining.fileKey());
        }
        quota.reconcileReservations(tenantId, activeKeys);
    }
}
