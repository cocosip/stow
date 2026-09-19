package io.github.cocosip.stow.internal.cleanup;

import io.github.cocosip.stow.internal.projection.QueueProjectionService;
import io.github.cocosip.stow.internal.projection.SqliteMetadataProjectionStore;
import io.github.cocosip.stow.model.CleanupStatistics;
import io.github.cocosip.stow.model.FileProcessingStatus;
import io.github.cocosip.stow.model.QueueEventRecord;
import io.github.cocosip.stow.model.QueueEventType;
import io.github.cocosip.stow.spi.QueueEventJournal;
import io.github.cocosip.stow.spi.StorageVolume;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;
import java.util.stream.Collectors;

public final class CompletedFileReaper {

    private final QueueEventJournal journal;
    private final SqliteMetadataProjectionStore metadata;
    private final QueueProjectionService projection;
    private final Map<String, StorageVolume> volumes;
    private final Clock clock;
    private final MaintenanceEventAppender appender;

    public CompletedFileReaper(
            QueueEventJournal journal,
            SqliteMetadataProjectionStore metadata,
            QueueProjectionService projection,
            List<StorageVolume> volumes,
            Clock clock) {
        this.journal = Objects.requireNonNull(journal, "journal");
        this.metadata = Objects.requireNonNull(metadata, "metadata");
        this.projection = Objects.requireNonNull(projection, "projection");
        this.volumes = Objects.requireNonNull(volumes, "volumes").stream()
                .collect(Collectors.toUnmodifiableMap(StorageVolume::id, Function.identity()));
        this.clock = Objects.requireNonNull(clock, "clock");
        appender = new MaintenanceEventAppender(journal);
    }

    public CleanupStatistics run(Duration olderThan, int batchSizePerTenant) {
        if (olderThan == null || olderThan.isNegative()) {
            throw new IllegalArgumentException("olderThan must be non-negative");
        }
        if (batchSizePerTenant <= 0) {
            throw new IllegalArgumentException("batchSizePerTenant must be positive");
        }
        CleanupStatisticsBuilder statistics = new CleanupStatisticsBuilder(clock);
        Instant cutoff = clock.instant().minus(olderThan);
        for (String tenantId : journal.tenantIds().stream().sorted().toList()) {
            List<SqliteMetadataProjectionStore.FileRow> rows = metadata.activeFiles(tenantId).stream()
                    .filter(row -> eligible(row, cutoff))
                    .limit(batchSizePerTenant)
                    .toList();
            for (SqliteMetadataProjectionStore.FileRow row : rows) {
                statistics.scanned();
                statistics.tenant(tenantId);
                if (row.status() == FileProcessingStatus.COMPLETED) {
                    try {
                        requestDelete(row);
                    } catch (RuntimeException exception) {
                        statistics.failed(tenantId, "delete-request", exception);
                    }
                }
            }
            for (SqliteMetadataProjectionStore.FileRow row : metadata.activeFiles(tenantId).stream()
                    .filter(candidate -> candidate.status() == FileProcessingStatus.DELETE_REQUESTED)
                    .limit(batchSizePerTenant)
                    .toList()) {
                try {
                    delete(row);
                    statistics.succeeded(tenantId, row.fileSize());
                } catch (RuntimeException exception) {
                    statistics.failed(tenantId, "delete", exception);
                }
            }
        }
        return statistics.build();
    }

    private boolean eligible(SqliteMetadataProjectionStore.FileRow row, Instant cutoff) {
        if (row.status() == FileProcessingStatus.DELETE_REQUESTED) return true;
        if (row.status() != FileProcessingStatus.COMPLETED) return false;
        return row.completedAtMillis() != null && row.completedAtMillis() <= cutoff.toEpochMilli();
    }

    private void requestDelete(SqliteMetadataProjectionStore.FileRow row) {
        appender.append(event(row, QueueEventType.DELETE_REQUESTED, FileProcessingStatus.DELETE_REQUESTED));
        project(row.tenantId());
    }

    private void delete(SqliteMetadataProjectionStore.FileRow row) {
        StorageVolume volume = volumes.get(row.volumeId());
        if (volume == null) throw new IllegalStateException("Unknown storage volume: " + row.volumeId());
        Path physical = Path.of(row.physicalPath());
        if (Files.exists(physical, LinkOption.NOFOLLOW_LINKS)) volume.delete(physical);
        if (Files.exists(physical, LinkOption.NOFOLLOW_LINKS)) {
            throw new IllegalStateException("Physical file still exists after delete: " + physical);
        }
        appender.append(event(row, QueueEventType.DELETE_SUCCEEDED, FileProcessingStatus.DELETE_SUCCEEDED));
        project(row.tenantId());
    }

    private void project(String tenantId) {
        projection.projectTenantUntilCaughtUp(tenantId, 256);
    }

    private QueueEventRecord event(
            SqliteMetadataProjectionStore.FileRow row, QueueEventType type, FileProcessingStatus status) {
        return new QueueEventRecord(
                1,
                java.util.UUID.randomUUID(),
                row.tenantId(),
                row.fileKey(),
                type,
                clock.instant(),
                1,
                row.volumeId(),
                Path.of(row.physicalPath()),
                row.logicalDirectory(),
                row.fileSize(),
                status,
                null,
                null,
                row.retryCount(),
                null,
                null,
                row.originalFileName(),
                row.fileExtension());
    }
}
