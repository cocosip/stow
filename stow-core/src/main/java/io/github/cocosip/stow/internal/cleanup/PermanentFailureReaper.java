package io.github.cocosip.stow.internal.cleanup;

import io.github.cocosip.stow.config.PermanentlyFailedDisposition;
import io.github.cocosip.stow.internal.journal.SequencedJournalAppender;
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
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;
import java.util.stream.Collectors;

public final class PermanentFailureReaper {

    private static final DateTimeFormatter DAY =
            DateTimeFormatter.ofPattern("yyyyMMdd").withZone(ZoneOffset.UTC);
    private final QueueEventJournal journal;
    private final SqliteMetadataProjectionStore metadata;
    private final QueueProjectionService projection;
    private final Map<String, StorageVolume> volumes;
    private final Clock clock;
    private final MaintenanceEventAppender appender;

    public PermanentFailureReaper(
            QueueEventJournal journal,
            SqliteMetadataProjectionStore metadata,
            QueueProjectionService projection,
            List<StorageVolume> volumes,
            Clock clock) {
        this(journal, metadata, projection, volumes, clock, new SequencedJournalAppender(journal));
    }

    public PermanentFailureReaper(
            QueueEventJournal journal,
            SqliteMetadataProjectionStore metadata,
            QueueProjectionService projection,
            List<StorageVolume> volumes,
            Clock clock,
            SequencedJournalAppender appender) {
        this.journal = Objects.requireNonNull(journal, "journal");
        this.metadata = Objects.requireNonNull(metadata, "metadata");
        this.projection = Objects.requireNonNull(projection, "projection");
        this.volumes = Objects.requireNonNull(volumes, "volumes").stream()
                .collect(Collectors.toUnmodifiableMap(StorageVolume::id, Function.identity()));
        this.clock = Objects.requireNonNull(clock, "clock");
        this.appender = new MaintenanceEventAppender(appender);
    }

    public CleanupStatistics run(
            java.time.Duration olderThan, int batchSizePerTenant, PermanentlyFailedDisposition disposition) {
        if (olderThan == null || olderThan.isNegative()) {
            throw new IllegalArgumentException("olderThan must be non-negative");
        }
        if (batchSizePerTenant <= 0) {
            throw new IllegalArgumentException("batchSizePerTenant must be positive");
        }
        Objects.requireNonNull(disposition, "disposition");
        CleanupStatisticsBuilder statistics = new CleanupStatisticsBuilder(clock);
        long cutoff = clock.instant().minus(olderThan).toEpochMilli();
        for (String tenantId : journal.tenantIds().stream().sorted().toList()) {
            List<SqliteMetadataProjectionStore.FileRow> rows = metadata.activeFiles(tenantId).stream()
                    .filter(row -> row.status() == FileProcessingStatus.PERMANENTLY_FAILED)
                    .filter(row -> (row.lastFailedAtMillis() == null ? row.createdAtMillis() : row.lastFailedAtMillis())
                            <= cutoff)
                    .limit(batchSizePerTenant)
                    .toList();
            for (SqliteMetadataProjectionStore.FileRow row : rows) {
                statistics.scanned();
                statistics.tenant(tenantId);
                if (disposition == PermanentlyFailedDisposition.KEEP) {
                    statistics.skipped();
                    continue;
                }
                try {
                    StorageVolume volume = volume(row);
                    if (disposition == PermanentlyFailedDisposition.MOVE_TO_DEAD_LETTER) {
                        moveToDeadLetter(row, volume);
                    } else {
                        delete(row, volume);
                    }
                    appender.append(event(row));
                    project(tenantId);
                    statistics.succeeded(tenantId, row.fileSize());
                } catch (RuntimeException exception) {
                    statistics.failed(tenantId, "permanent-failure", exception);
                }
            }
        }
        return statistics.build();
    }

    private void project(String tenantId) {
        try {
            projection.projectTenantUntilCaughtUp(tenantId, 256);
        } catch (RuntimeException ignored) {
            // Journal admission is durable; background projection will retry from its cursor.
        }
    }

    private StorageVolume volume(SqliteMetadataProjectionStore.FileRow row) {
        StorageVolume volume = volumes.get(row.volumeId());
        if (volume == null) throw new IllegalStateException("Unknown storage volume: " + row.volumeId());
        return volume;
    }

    private void delete(SqliteMetadataProjectionStore.FileRow row, StorageVolume volume) {
        Path source = Path.of(row.physicalPath());
        if (Files.exists(source, LinkOption.NOFOLLOW_LINKS)) volume.delete(source);
        if (Files.exists(source, LinkOption.NOFOLLOW_LINKS)) {
            throw new IllegalStateException("Physical file still exists after delete: " + source);
        }
    }

    private void moveToDeadLetter(SqliteMetadataProjectionStore.FileRow row, StorageVolume volume) {
        Path source = Path.of(row.physicalPath());
        Path target = deadLetterPath(row, volume);
        if (!Files.exists(source, LinkOption.NOFOLLOW_LINKS) && Files.exists(target, LinkOption.NOFOLLOW_LINKS)) return;
        if (!Files.exists(source, LinkOption.NOFOLLOW_LINKS)) {
            throw new IllegalStateException("Physical file does not exist: " + source);
        }
        Path targetParent = target.getParent();
        if (targetParent == null) {
            throw new IllegalStateException("Dead-letter target has no parent: " + target);
        }
        try {
            Files.createDirectories(targetParent);
        } catch (java.io.IOException exception) {
            throw new IllegalStateException("Unable to create dead-letter directory", exception);
        }
        volume.move(source, target);
        if (Files.exists(source, LinkOption.NOFOLLOW_LINKS) || !Files.exists(target, LinkOption.NOFOLLOW_LINKS)) {
            throw new IllegalStateException("Dead-letter move did not complete");
        }
    }

    private Path deadLetterPath(SqliteMetadataProjectionStore.FileRow row, StorageVolume volume) {
        Path configuredMount = volume.mountPath();
        if (configuredMount == null) throw new IllegalStateException("Storage volume has no mount path");
        Path mount = configuredMount.toAbsolutePath().normalize();
        Path source = Path.of(row.physicalPath()).toAbsolutePath().normalize();
        Path relative = mount.relativize(source);
        if (relative.getNameCount() < 2 || !relative.getName(0).toString().equals(row.tenantId())) {
            throw new IllegalStateException("Physical path is outside its storage volume");
        }
        Path target = mount.resolve(".deadletter")
                .resolve(row.tenantId())
                .resolve(DAY.format(Instant.ofEpochMilli(
                        row.lastFailedAtMillis() == null ? row.createdAtMillis() : row.lastFailedAtMillis())));
        for (int index = 1; index < relative.getNameCount() - 1; index++) {
            target = target.resolve(relative.getName(index).toString());
        }
        Path fileName = relative.getFileName();
        if (fileName == null) {
            throw new IllegalStateException("Physical path has no file name: " + source);
        }
        return target.resolve(fileName.toString()).normalize();
    }

    private QueueEventRecord event(SqliteMetadataProjectionStore.FileRow row) {
        return new QueueEventRecord(
                1,
                java.util.UUID.randomUUID(),
                row.tenantId(),
                row.fileKey(),
                QueueEventType.DEAD_LETTERED,
                clock.instant(),
                1,
                row.volumeId(),
                Path.of(row.physicalPath()),
                row.logicalDirectory(),
                row.fileSize(),
                FileProcessingStatus.DEAD_LETTERED,
                null,
                null,
                row.retryCount(),
                null,
                row.lastError(),
                row.originalFileName(),
                row.fileExtension(),
                row.importOperationId());
    }
}
