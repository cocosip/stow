package io.github.cocosip.stow.internal.cleanup;

import io.github.cocosip.stow.internal.projection.SqliteMetadataProjectionStore;
import io.github.cocosip.stow.internal.quota.SqliteQuotaRepository;
import io.github.cocosip.stow.model.CleanupStatistics;
import io.github.cocosip.stow.spi.StorageVolume;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.time.Clock;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

/**
 * Removes metadata rows whose physical file has vanished and releases their quota
 * charges (Locus CleanupOrphanedMetadataAsync). Rows on missing or unhealthy volumes
 * are skipped, and a row whose canonical path does exist is repaired in place instead
 * of being removed.
 */
public final class OrphanedMetadataCleaner {

    private final SqliteMetadataProjectionStore metadata;
    private final SqliteQuotaRepository quota;
    private final List<StorageVolume> volumes;
    private final Clock clock;

    public OrphanedMetadataCleaner(
            SqliteMetadataProjectionStore metadata,
            SqliteQuotaRepository quota,
            List<StorageVolume> volumes,
            Clock clock) {
        this.metadata = Objects.requireNonNull(metadata, "metadata");
        this.quota = Objects.requireNonNull(quota, "quota");
        this.volumes = List.copyOf(Objects.requireNonNull(volumes, "volumes"));
        this.clock = Objects.requireNonNull(clock, "clock");
    }

    public CleanupStatistics run() {
        CleanupStatisticsBuilder statistics = new CleanupStatisticsBuilder(clock);
        for (String tenantId : metadata.tenantIds()) {
            statistics.tenant(tenantId);
            cleanTenant(tenantId, statistics);
        }
        return statistics.build();
    }

    private void cleanTenant(String tenantId, CleanupStatisticsBuilder statistics) {
        List<SqliteMetadataProjectionStore.FileRow> rows = metadata.activeFiles(tenantId);
        Map<String, Optional<Boolean>> existenceCache = new HashMap<>();
        for (SqliteMetadataProjectionStore.FileRow row : rows) {
            statistics.scanned();
            if (row.physicalPath() == null || row.physicalPath().isBlank()) {
                statistics.skipped();
                continue;
            }
            StorageVolume volume = volumeFor(row.volumeId());
            if (volume == null || !healthy(volume)) {
                // A missing or unhealthy volume must not look like a missing file.
                statistics.skipped();
                continue;
            }
            Optional<Boolean> exists = existenceCache.computeIfAbsent(row.physicalPath(), path -> physicalExists(path));
            if (exists.isEmpty() || exists.get()) {
                // An unknown state (unreadable path) must not classify the file as missing.
                statistics.skipped();
                continue;
            }
            Path corrected = correctedExistingPath(row, volume, existenceCache);
            if (corrected != null) {
                // The canonical path exists: repair the row instead of removing it.
                metadata.correctPhysicalPath(tenantId, row.fileKey(), corrected.toString(), row.rowVersion());
                statistics.succeeded(tenantId, 0);
                continue;
            }
            if (removeOrphan(tenantId, row)) {
                statistics.succeeded(tenantId, 0);
            } else {
                // The row changed concurrently; the next cycle re-evaluates it.
                statistics.skipped();
            }
        }
    }

    private boolean removeOrphan(String tenantId, SqliteMetadataProjectionStore.FileRow row) {
        if (!metadata.removeFile(tenantId, row.fileKey(), row.rowVersion())) {
            return false;
        }
        // Without the active row the reservation has no fact to back it, so the
        // reconciliation releases the tenant and directory charge and deletes the
        // reservation row (the same idempotent primitive as startup recovery).
        Set<String> activeKeys = new HashSet<>();
        for (SqliteMetadataProjectionStore.FileRow remaining : metadata.activeFiles(tenantId)) {
            activeKeys.add(remaining.fileKey());
        }
        quota.reconcileReservations(tenantId, activeKeys);
        return true;
    }

    private Path correctedExistingPath(
            SqliteMetadataProjectionStore.FileRow row,
            StorageVolume volume,
            Map<String, Optional<Boolean>> existenceCache) {
        Path rebuilt;
        try {
            rebuilt = volume.buildPath(row.tenantId(), row.fileKey(), row.fileExtension());
        } catch (RuntimeException exception) {
            return null;
        }
        if (rebuilt.toAbsolutePath()
                .normalize()
                .toString()
                .equals(Path.of(row.physicalPath()).toString())) {
            return null;
        }
        Optional<Boolean> correctedExists =
                existenceCache.computeIfAbsent(rebuilt.toString(), path -> physicalExists(path));
        return correctedExists.isPresent() && correctedExists.get() ? rebuilt : null;
    }

    private Optional<Boolean> physicalExists(String path) {
        try {
            return Optional.of(Files.isRegularFile(Path.of(path), LinkOption.NOFOLLOW_LINKS));
        } catch (RuntimeException exception) {
            // Security/invalid path failures must not classify the file as missing.
            return Optional.empty();
        }
    }

    private StorageVolume volumeFor(String volumeId) {
        if (volumeId == null) return null;
        for (StorageVolume volume : volumes) {
            if (volume.id().equals(volumeId)) return volume;
        }
        return null;
    }

    private static boolean healthy(StorageVolume volume) {
        try {
            return volume.healthy();
        } catch (RuntimeException ignored) {
            return false;
        }
    }
}
