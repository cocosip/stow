package io.github.cocosip.stow.internal.recovery;

import io.github.cocosip.stow.internal.cleanup.CleanupStatisticsBuilder;
import io.github.cocosip.stow.internal.cleanup.MaintenanceEventAppender;
import io.github.cocosip.stow.internal.journal.SequencedJournalAppender;
import io.github.cocosip.stow.internal.projection.QueueProjectionService;
import io.github.cocosip.stow.internal.projection.SqliteMetadataProjectionStore;
import io.github.cocosip.stow.internal.quota.SqliteQuotaRepository;
import io.github.cocosip.stow.model.CleanupStatistics;
import io.github.cocosip.stow.model.FileProcessingStatus;
import io.github.cocosip.stow.model.QueueEventRecord;
import io.github.cocosip.stow.model.QueueEventType;
import io.github.cocosip.stow.spi.QueueEventJournal;
import io.github.cocosip.stow.spi.StorageVolume;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.attribute.BasicFileAttributes;
import java.time.Clock;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Function;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

public final class OrphanFileRecovery {

    private static final Pattern FILE_NAME =
            Pattern.compile("^(?<key>[0-9a-f]{32})(?<extension>\\.[A-Za-z0-9._-]{1,31})?$");
    private final QueueEventJournal journal;
    private final SqliteMetadataProjectionStore metadata;
    private final SqliteQuotaRepository quota;
    private final QueueProjectionService projection;
    private final Map<String, StorageVolume> volumes;
    private final Clock clock;
    private final MaintenanceEventAppender appender;

    public OrphanFileRecovery(
            QueueEventJournal journal,
            SqliteMetadataProjectionStore metadata,
            SqliteQuotaRepository quota,
            QueueProjectionService projection,
            List<StorageVolume> volumes,
            Clock clock) {
        this(journal, metadata, quota, projection, volumes, clock, new SequencedJournalAppender(journal));
    }

    public OrphanFileRecovery(
            QueueEventJournal journal,
            SqliteMetadataProjectionStore metadata,
            SqliteQuotaRepository quota,
            QueueProjectionService projection,
            List<StorageVolume> volumes,
            Clock clock,
            SequencedJournalAppender appender) {
        this.journal = Objects.requireNonNull(journal, "journal");
        this.metadata = Objects.requireNonNull(metadata, "metadata");
        this.quota = Objects.requireNonNull(quota, "quota");
        this.projection = Objects.requireNonNull(projection, "projection");
        this.volumes = Objects.requireNonNull(volumes, "volumes").stream()
                .collect(Collectors.toUnmodifiableMap(StorageVolume::id, Function.identity()));
        this.clock = Objects.requireNonNull(clock, "clock");
        this.appender = new MaintenanceEventAppender(appender);
    }

    public CleanupStatistics recover(String tenantId, int maxFiles) {
        if (tenantId == null || tenantId.isBlank()) throw new IllegalArgumentException("tenantId must not be blank");
        if (maxFiles <= 0) throw new IllegalArgumentException("maxFiles must be positive");
        CleanupStatisticsBuilder statistics = new CleanupStatisticsBuilder(clock);
        for (StorageVolume volume : volumes.values()) {
            recoverFromVolume(volume, tenantId, maxFiles, statistics);
        }
        return statistics.build();
    }

    public CleanupStatistics recoverAll(int maxFilesPerTenant) {
        if (maxFilesPerTenant <= 0) throw new IllegalArgumentException("maxFilesPerTenant must be positive");
        Set<String> tenants = new java.util.TreeSet<>(journal.tenantIds());
        for (StorageVolume volume : volumes.values()) {
            Path mountPath = volume.mountPath();
            if (mountPath == null) continue;
            try (var paths = Files.list(mountPath)) {
                paths.filter(Files::isDirectory)
                        .map(path -> path.getFileName().toString())
                        .forEach(tenants::add);
            } catch (IOException exception) {
                // The per-tenant call records actionable errors when the tenant is known.
            }
        }
        CleanupStatisticsBuilder aggregate = new CleanupStatisticsBuilder(clock);
        for (String tenantId : tenants) {
            CleanupStatistics result = recover(tenantId, maxFilesPerTenant);
            aggregate.merge(result, tenantId);
        }
        return aggregate.build();
    }

    private void recoverFromVolume(
            StorageVolume volume, String tenantId, int maxFiles, CleanupStatisticsBuilder statistics) {
        Path configuredMount = volume.mountPath();
        if (configuredMount == null) {
            statistics.failed(tenantId, "orphan-scan", new IllegalStateException("Storage volume has no mount path"));
            return;
        }
        Path mount = configuredMount.toAbsolutePath().normalize();
        Path tenantRoot = mount.resolve(tenantId).normalize();
        if (!tenantRoot.startsWith(mount) || !Files.isDirectory(tenantRoot, LinkOption.NOFOLLOW_LINKS)) return;
        List<Path> candidates;
        try (var paths = Files.walk(tenantRoot)) {
            candidates = paths.filter(path -> !path.startsWith(tenantRoot.resolve(".deadletter")))
                    .filter(path -> Files.isRegularFile(path, LinkOption.NOFOLLOW_LINKS))
                    .sorted()
                    .limit(maxFiles)
                    .toList();
        } catch (IOException exception) {
            statistics.failed(tenantId, "orphan-scan", exception);
            return;
        }
        for (Path path : candidates) {
            Path fileName = path.getFileName();
            if (fileName == null) {
                statistics.skipped();
                continue;
            }
            String fileNameText = fileName.toString();
            Matcher matcher = FILE_NAME.matcher(fileNameText);
            if (!matcher.matches()) {
                statistics.skipped();
                continue;
            }
            statistics.scanned();
            statistics.tenant(tenantId);
            recoverFile(volume, tenantId, path, matcher, statistics);
        }
    }

    private void recoverFile(
            StorageVolume volume, String tenantId, Path path, Matcher matcher, CleanupStatisticsBuilder statistics) {
        String fileKey = matcher.group("key");
        if (metadata.find(tenantId, fileKey).isPresent() || journalContains(tenantId, fileKey)) {
            statistics.skipped();
            return;
        }
        boolean createdReservation = false;
        try {
            if (quota.reservation(tenantId, fileKey).isEmpty()) {
                quota.reserve(tenantId, fileKey, "/");
                createdReservation = true;
            }
            BasicFileAttributes attributes =
                    Files.readAttributes(path, BasicFileAttributes.class, LinkOption.NOFOLLOW_LINKS);
            Instant createdAt = attributes.creationTime().toInstant();
            String extension = matcher.group("extension");
            Path fileName = path.getFileName();
            if (fileName == null) {
                throw new IllegalStateException("Recovered orphan has no file name: " + path);
            }
            QueueEventRecord accepted = new QueueEventRecord(
                    1,
                    java.util.UUID.randomUUID(),
                    tenantId,
                    fileKey,
                    QueueEventType.ACCEPTED,
                    createdAt.isAfter(clock.instant()) ? clock.instant() : createdAt,
                    1,
                    volume.id(),
                    path,
                    "/",
                    attributes.size(),
                    FileProcessingStatus.PENDING,
                    null,
                    null,
                    0,
                    null,
                    null,
                    fileName.toString(),
                    extension);
            appender.append(accepted);
            project(tenantId);
            statistics.succeeded(tenantId, attributes.size());
        } catch (RuntimeException | IOException exception) {
            if (createdReservation)
                quota.reservation(tenantId, fileKey)
                        .ifPresent(reservation -> quota.rollback(tenantId, reservation.reservationId()));
            if (exception instanceof RuntimeException runtime) statistics.failed(tenantId, "orphan-recover", runtime);
            else statistics.failed(tenantId, "orphan-recover", new IllegalStateException(exception));
        }
    }

    private void project(String tenantId) {
        try {
            projection.projectTenantUntilCaughtUp(tenantId, 256);
        } catch (RuntimeException ignored) {
            // Journal admission is durable; background projection will retry from its cursor.
        }
    }

    private boolean journalContains(String tenantId, String fileKey) {
        long cursor = journal.baseOffset(tenantId);
        long tail = journal.tailOffset(tenantId);
        while (cursor < tail) {
            var batch = journal.readBatch(tenantId, cursor, 512);
            if (batch.events().isEmpty() || batch.nextOffset() <= cursor) return false;
            if (batch.events().stream().anyMatch(event -> event.fileKey().equals(fileKey))) return true;
            cursor = batch.nextOffset();
        }
        return false;
    }
}
