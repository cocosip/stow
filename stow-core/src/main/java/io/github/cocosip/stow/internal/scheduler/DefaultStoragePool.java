package io.github.cocosip.stow.internal.scheduler;

import io.github.cocosip.stow.api.ContentSource;
import io.github.cocosip.stow.api.StoragePool;
import io.github.cocosip.stow.api.TenantManager;
import io.github.cocosip.stow.config.RetryConfiguration;
import io.github.cocosip.stow.exception.InsufficientStorageException;
import io.github.cocosip.stow.exception.LeaseMismatchException;
import io.github.cocosip.stow.exception.PhysicalFileMissingException;
import io.github.cocosip.stow.exception.RuntimeNotReadyException;
import io.github.cocosip.stow.exception.StoredFileNotFoundException;
import io.github.cocosip.stow.exception.TenantDisabledException;
import io.github.cocosip.stow.internal.journal.SequencedJournalAppender;
import io.github.cocosip.stow.internal.projection.QueueProjectionService;
import io.github.cocosip.stow.internal.projection.SqliteMetadataProjectionStore;
import io.github.cocosip.stow.internal.quota.QuotaReservation;
import io.github.cocosip.stow.internal.quota.SqliteQuotaRepository;
import io.github.cocosip.stow.internal.statistics.NoopStatisticsRecorder;
import io.github.cocosip.stow.internal.statistics.StatisticsRecorder;
import io.github.cocosip.stow.model.ClaimedFile;
import io.github.cocosip.stow.model.FileLocation;
import io.github.cocosip.stow.model.FileProcessingStatus;
import io.github.cocosip.stow.model.ProcessingLease;
import io.github.cocosip.stow.model.QueueEventRecord;
import io.github.cocosip.stow.model.QueueEventType;
import io.github.cocosip.stow.model.StoredFileInfo;
import io.github.cocosip.stow.model.TenantContext;
import io.github.cocosip.stow.model.TenantStatus;
import io.github.cocosip.stow.model.WriteOptions;
import io.github.cocosip.stow.spi.QueueEventJournal;
import io.github.cocosip.stow.spi.StorageVolume;
import java.io.InputStream;
import java.nio.file.Path;
import java.security.SecureRandom;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.UUID;

public final class DefaultStoragePool implements StoragePool {

    @FunctionalInterface
    public interface TenantLookup {
        TenantContext find(String tenantId);
    }

    private final TenantLookup tenants;
    private final SqliteQuotaRepository quota;
    private final SqliteMetadataProjectionStore metadata;
    private final QueueProjectionService projection;
    private final QueueEventJournal journal;
    private final SequencedJournalAppender appender;
    private final StatisticsRecorder statistics;
    private final List<StorageVolume> volumes;
    private final Clock clock;
    private final SecureRandom random = new SecureRandom();
    private final StripedFileLock fileLocks = new StripedFileLock();
    private final TerminalLeaseIndex terminalLeases;
    private final RetryDelayCalculator retryDelays;

    public DefaultStoragePool(
            TenantLookup tenants,
            SqliteQuotaRepository quota,
            SqliteMetadataProjectionStore metadata,
            QueueProjectionService projection,
            QueueEventJournal journal,
            StorageVolume volume,
            Clock clock) {
        this(tenants, quota, metadata, projection, journal, List.of(volume), clock, defaultRetryConfiguration());
    }

    public DefaultStoragePool(
            TenantManager tenants,
            SqliteQuotaRepository quota,
            SqliteMetadataProjectionStore metadata,
            QueueProjectionService projection,
            QueueEventJournal journal,
            StorageVolume volume,
            Clock clock) {
        this(tenantManager(tenants), quota, metadata, projection, journal, volume, clock);
    }

    public DefaultStoragePool(
            TenantLookup tenants,
            SqliteQuotaRepository quota,
            SqliteMetadataProjectionStore metadata,
            QueueProjectionService projection,
            QueueEventJournal journal,
            List<StorageVolume> volumes,
            Clock clock) {
        this(tenants, quota, metadata, projection, journal, volumes, clock, defaultRetryConfiguration());
    }

    public DefaultStoragePool(
            TenantLookup tenants,
            SqliteQuotaRepository quota,
            SqliteMetadataProjectionStore metadata,
            QueueProjectionService projection,
            QueueEventJournal journal,
            List<StorageVolume> volumes,
            Clock clock,
            RetryConfiguration retryConfiguration) {
        this(
                tenants,
                quota,
                metadata,
                projection,
                journal,
                volumes,
                clock,
                retryConfiguration,
                new SequencedJournalAppender(journal));
    }

    public DefaultStoragePool(
            TenantLookup tenants,
            SqliteQuotaRepository quota,
            SqliteMetadataProjectionStore metadata,
            QueueProjectionService projection,
            QueueEventJournal journal,
            List<StorageVolume> volumes,
            Clock clock,
            RetryConfiguration retryConfiguration,
            SequencedJournalAppender appender) {
        this(
                tenants,
                quota,
                metadata,
                projection,
                journal,
                volumes,
                clock,
                retryConfiguration,
                appender,
                new NoopStatisticsRecorder(clock));
    }

    public DefaultStoragePool(
            TenantLookup tenants,
            SqliteQuotaRepository quota,
            SqliteMetadataProjectionStore metadata,
            QueueProjectionService projection,
            QueueEventJournal journal,
            List<StorageVolume> volumes,
            Clock clock,
            RetryConfiguration retryConfiguration,
            SequencedJournalAppender appender,
            StatisticsRecorder statistics) {
        this(
                tenants,
                quota,
                metadata,
                projection,
                journal,
                volumes,
                clock,
                retryConfiguration,
                appender,
                statistics,
                new TerminalLeaseIndex());
    }

    public DefaultStoragePool(
            TenantLookup tenants,
            SqliteQuotaRepository quota,
            SqliteMetadataProjectionStore metadata,
            QueueProjectionService projection,
            QueueEventJournal journal,
            List<StorageVolume> volumes,
            Clock clock,
            RetryConfiguration retryConfiguration,
            SequencedJournalAppender appender,
            StatisticsRecorder statistics,
            TerminalLeaseIndex terminalLeases) {
        this.tenants = Objects.requireNonNull(tenants, "tenants");
        this.quota = Objects.requireNonNull(quota, "quota");
        this.metadata = Objects.requireNonNull(metadata, "metadata");
        this.projection = Objects.requireNonNull(projection, "projection");
        this.journal = Objects.requireNonNull(journal, "journal");
        this.appender = Objects.requireNonNull(appender, "appender");
        this.statistics = Objects.requireNonNull(statistics, "statistics");
        this.terminalLeases = Objects.requireNonNull(terminalLeases, "terminalLeases");
        this.volumes = List.copyOf(volumes);
        if (this.volumes.isEmpty()) throw new IllegalArgumentException("volumes must not be empty");
        this.clock = Objects.requireNonNull(clock, "clock");
        this.retryDelays = new RetryDelayCalculator(Objects.requireNonNull(retryConfiguration, "retryConfiguration"));
    }

    public DefaultStoragePool(
            TenantLookup tenants,
            SqliteQuotaRepository quota,
            SqliteMetadataProjectionStore metadata,
            QueueProjectionService projection,
            QueueEventJournal journal,
            StorageVolume volume,
            Clock clock,
            RetryConfiguration retryConfiguration) {
        this(tenants, quota, metadata, projection, journal, List.of(volume), clock, retryConfiguration);
    }

    public static TenantLookup tenantManager(io.github.cocosip.stow.api.TenantManager manager) {
        Objects.requireNonNull(manager, "manager");
        return tenantId -> manager.find(tenantId).orElse(null);
    }

    @Override
    public String write(TenantContext tenant, InputStream content, WriteOptions options) {
        Objects.requireNonNull(content, "content");
        requireEnabled(tenant);
        return writeInternal(
                tenant, () -> new CountingInputStream(content), OptionalLong.empty(), false, false, options);
    }

    @Override
    public String write(TenantContext tenant, ContentSource content, WriteOptions options) {
        Objects.requireNonNull(content, "content");
        requireEnabled(tenant);
        return writeInternal(tenant, content::openStream, content.length(), content.repeatable(), true, options);
    }

    private String writeInternal(
            TenantContext tenant,
            StreamOpener opener,
            OptionalLong knownLength,
            boolean repeatable,
            boolean closeStream,
            WriteOptions suppliedOptions) {
        WriteOptions options = suppliedOptions == null ? WriteOptions.defaults() : suppliedOptions;
        String fileKey = nextFileKey();
        String logicalDirectory = options.logicalDirectory() == null ? "/" : options.logicalDirectory();
        String extension = extensionFrom(options.originalFileName());
        QuotaReservation reservation = quota.reserve(tenant.tenantId(), fileKey, logicalDirectory);
        WriteCompensation compensation = new WriteCompensation(quota, tenant.tenantId(), reservation);
        List<StorageVolume> candidates = candidates(knownLength.orElse(0));
        RuntimeException lastFailure = null;
        StorageVolume selected = null;
        Path finalPath = null;
        long fileSize = -1;
        for (StorageVolume volume : candidates) {
            try {
                finalPath = volume.buildPath(tenant.tenantId(), fileKey, extension);
                Path temporary = finalPath.resolveSibling(
                        "." + finalPath.getFileName() + "." + java.util.UUID.randomUUID() + ".tmp");
                compensation.selected(volume, temporary, finalPath);
                InputStream opened = opener.open();
                RuntimeException bodyFailure = null;
                try {
                    CountingInputStream counted =
                            opened instanceof CountingInputStream c ? c : new CountingInputStream(opened);
                    volume.write(temporary, counted);
                    fileSize = counted.count();
                } catch (RuntimeException failure) {
                    bodyFailure = failure;
                    throw failure;
                } finally {
                    if (closeStream) {
                        try {
                            opened.close();
                        } catch (java.io.IOException exception) {
                            // a close failure while unwinding must not mask the real write failure
                            if (bodyFailure != null) {
                                bodyFailure.addSuppressed(exception);
                            } else {
                                throw new java.io.UncheckedIOException("Unable to close content stream", exception);
                            }
                        }
                    }
                }
                compensation.beforePublish();
                volume.move(temporary, finalPath);
                selected = volume;
                break;
            } catch (RuntimeException failure) {
                lastFailure = failure;
                compensation.cleanupCandidate();
                if (!repeatable) break;
            }
        }
        if (selected == null) {
            if (!compensation.publishAttempted()) compensation.cleanupBeforePublish();
            if (lastFailure != null) throw lastFailure;
            throw new InsufficientStorageException("No healthy storage volume is available");
        }

        QueueEventRecord accepted = new QueueEventRecord(
                1,
                java.util.UUID.randomUUID(),
                tenant.tenantId(),
                fileKey,
                QueueEventType.ACCEPTED,
                clock.instant(),
                1,
                selected.id(),
                finalPath,
                logicalDirectory,
                fileSize,
                FileProcessingStatus.PENDING,
                null,
                null,
                0,
                null,
                null,
                options.originalFileName(),
                extension);
        try {
            appender.append(accepted);
        } catch (RuntimeException failure) {
            compensation.cleanupAfterFailure();
            throw failure;
        }
        projectBestEffort(tenant.tenantId(), 128);
        statistics.recordWrite(tenant.tenantId(), selected.id(), fileSize);
        return fileKey;
    }

    private List<StorageVolume> candidates(long requiredBytes) {
        return volumes.stream()
                .filter(volume -> {
                    try {
                        return volume.healthy() && volume.availableCapacity() >= requiredBytes;
                    } catch (RuntimeException ignored) {
                        return false;
                    }
                })
                .sorted(Comparator.comparingLong(DefaultStoragePool::available)
                        .reversed()
                        .thenComparing(StorageVolume::id))
                .toList();
    }

    private static long available(StorageVolume volume) {
        try {
            return volume.availableCapacity();
        } catch (RuntimeException ignored) {
            return 0;
        }
    }

    @Override
    public InputStream read(TenantContext tenant, String fileKey) {
        FileLocation location = findFileLocation(tenant, fileKey)
                .orElseThrow(() -> new StoredFileNotFoundException("Stored file does not exist"));
        StorageVolume volume = volumes.stream()
                .filter(candidate -> candidate.id().equals(location.volumeId()))
                .findFirst()
                .orElseThrow(() ->
                        new PhysicalFileMissingException("Storage volume is unavailable: " + location.volumeId()));
        try {
            InputStream content = volume.read(location.physicalPath());
            try {
                statistics.recordRead(tenant.tenantId(), volume.id());
            } catch (RuntimeException exception) {
                // never leak the opened stream because a statistics recorder failed
                try {
                    content.close();
                } catch (java.io.IOException suppressed) {
                    exception.addSuppressed(suppressed);
                }
                throw exception;
            }
            return content;
        } catch (StoredFileNotFoundException exception) {
            throw exception;
        } catch (RuntimeException exception) {
            throw new PhysicalFileMissingException("Stored file is unavailable: " + fileKey);
        }
    }

    @Override
    public Optional<StoredFileInfo> findFileInfo(TenantContext tenant, String fileKey) {
        requireTenant(tenant);
        return metadata.find(tenant.tenantId(), fileKey)
                .map(row -> new StoredFileInfo(
                        row.fileKey(),
                        row.tenantId(),
                        row.fileSize(),
                        java.time.Instant.ofEpochMilli(row.createdAtMillis()),
                        row.status(),
                        row.retryCount(),
                        row.originalFileName(),
                        row.fileExtension()));
    }

    @Override
    public Optional<FileLocation> findFileLocation(TenantContext tenant, String fileKey) {
        requireTenant(tenant);
        return metadata.find(tenant.tenantId(), fileKey)
                .map(row -> new FileLocation(
                        row.fileKey(),
                        row.tenantId(),
                        row.volumeId(),
                        Path.of(row.physicalPath()),
                        row.logicalDirectory(),
                        row.fileSize(),
                        java.time.Instant.ofEpochMilli(row.createdAtMillis()),
                        row.status(),
                        row.retryCount(),
                        row.lastFailedAtMillis() == null
                                ? null
                                : java.time.Instant.ofEpochMilli(row.lastFailedAtMillis()),
                        row.lastError(),
                        row.availableAtMillis() == null
                                ? null
                                : java.time.Instant.ofEpochMilli(row.availableAtMillis())));
    }

    @Override
    public Optional<ClaimedFile> claimNext(TenantContext tenant) {
        List<ClaimedFile> claimed = claimBatch(tenant, 1);
        return claimed.isEmpty() ? Optional.empty() : Optional.of(claimed.get(0));
    }

    @Override
    public List<ClaimedFile> claimBatch(TenantContext tenant, int batchSize) {
        if (batchSize <= 0) throw new IllegalArgumentException("batchSize must be positive");
        requireEnabled(tenant);
        List<SqliteMetadataProjectionStore.ClaimedRow> rows =
                metadata.claimAvailable(tenant.tenantId(), batchSize, clock.instant());
        if (rows.isEmpty()) return List.of();
        List<QueueEventRecord> events = new java.util.ArrayList<>(rows.size());
        for (SqliteMetadataProjectionStore.ClaimedRow claimed : rows) {
            SqliteMetadataProjectionStore.FileRow row = claimed.row();
            events.add(new QueueEventRecord(
                    1,
                    UUID.randomUUID(),
                    tenant.tenantId(),
                    row.fileKey(),
                    QueueEventType.PROCESSING_STARTED,
                    clock.instant(),
                    1,
                    row.volumeId(),
                    Path.of(row.physicalPath()),
                    row.logicalDirectory(),
                    row.fileSize(),
                    FileProcessingStatus.PROCESSING,
                    UUID.fromString(row.leaseId()),
                    row.processingStartedAtMillis() == null
                            ? clock.instant()
                            : Instant.ofEpochMilli(row.processingStartedAtMillis()),
                    row.retryCount(),
                    null,
                    null,
                    row.originalFileName(),
                    row.fileExtension()));
        }
        boolean appended = false;
        try {
            appender.appendBatch(events);
            appended = true;
        } catch (RuntimeException failure) {
            if (!appended) {
                for (SqliteMetadataProjectionStore.ClaimedRow claimed : rows) {
                    rollbackClaim(tenant.tenantId(), claimed);
                }
            }
            throw failure;
        }
        projectBestEffort(tenant.tenantId(), Math.max(128, batchSize));
        for (int index = 0; index < rows.size(); index++) statistics.recordClaim(tenant.tenantId());
        return rows.stream().map(claimed -> claimedFile(claimed.row())).toList();
    }

    @Override
    public void complete(ProcessingLease lease) {
        requireLease(lease);
        fileLocks.withLock(lease.fileKey(), () -> {
            TerminalLeaseIndex.Outcome previous = knownOutcome(lease);
            if (previous == TerminalLeaseIndex.Outcome.COMPLETED) return;
            if (previous != null) throw new LeaseMismatchException("Lease already has a different terminal outcome");
            Optional<SqliteMetadataProjectionStore.FileRow> found = metadata.find(lease.tenantId(), lease.fileKey());
            if (found.isEmpty()) throw new LeaseMismatchException("Lease does not own stored file");
            SqliteMetadataProjectionStore.FileRow row = found.orElseThrow();
            requireActiveLease(row, lease);
            QueueEventRecord event = transitionEvent(
                    row,
                    lease,
                    QueueEventType.PROCESSING_COMPLETED,
                    FileProcessingStatus.COMPLETED,
                    row.retryCount(),
                    null,
                    null);
            appendTransition(lease.tenantId(), event);
            statistics.recordCompleted(lease.tenantId());
        });
    }

    @Override
    public void fail(ProcessingLease lease, String errorMessage) {
        requireLease(lease);
        fileLocks.withLock(lease.fileKey(), () -> {
            TerminalLeaseIndex.Outcome previous = knownOutcome(lease);
            if (previous == TerminalLeaseIndex.Outcome.FAILED) return;
            if (previous != null) throw new LeaseMismatchException("Lease already has a different terminal outcome");
            Optional<SqliteMetadataProjectionStore.FileRow> found = metadata.find(lease.tenantId(), lease.fileKey());
            if (found.isEmpty()) throw new LeaseMismatchException("Lease does not own stored file");
            SqliteMetadataProjectionStore.FileRow row = found.orElseThrow();
            requireActiveLease(row, lease);
            int retryCount = Math.addExact(row.retryCount(), 1);
            boolean permanent = retryDelays.isPermanent(retryCount);
            Instant failedAt = clock.instant();
            Instant availableAt = permanent ? null : retryDelays.availableAt(failedAt, retryCount);
            FileProcessingStatus status =
                    permanent ? FileProcessingStatus.PERMANENTLY_FAILED : FileProcessingStatus.FAILED;
            QueueEventRecord event = transitionEvent(
                    row, lease, QueueEventType.PROCESSING_FAILED, status, retryCount, availableAt, errorMessage);
            appendTransition(lease.tenantId(), event);
        });
    }

    /** Recovers timed out leases and returns the number of events admitted. */
    public int recoverTimedOut(Duration timeout) {
        if (timeout == null || timeout.isNegative()) throw new IllegalArgumentException("timeout must be non-negative");
        Instant cutoff = clock.instant().minus(timeout);
        int recovered = 0;
        for (String tenantId : journal.tenantIds()) {
            List<SqliteMetadataProjectionStore.FileRow> rows = metadata.processingBefore(tenantId, cutoff, 1_000);
            for (SqliteMetadataProjectionStore.FileRow row : rows) {
                final int[] count = {0};
                fileLocks.withLock(row.fileKey(), () -> {
                    Optional<SqliteMetadataProjectionStore.FileRow> current = metadata.find(tenantId, row.fileKey());
                    if (current.isEmpty()
                            || current.orElseThrow().status() != FileProcessingStatus.PROCESSING
                            || current.orElseThrow().processingStartedAtMillis() == null
                            || current.orElseThrow().processingStartedAtMillis() > cutoff.toEpochMilli()) return;
                    UUID leaseId = UUID.fromString(current.orElseThrow().leaseId());
                    QueueEventRecord event = transitionEvent(
                            current.orElseThrow(),
                            new ProcessingLease(
                                    tenantId,
                                    row.fileKey(),
                                    leaseId,
                                    Instant.ofEpochMilli(current.orElseThrow().processingStartedAtMillis())),
                            QueueEventType.PROCESSING_TIMED_OUT,
                            FileProcessingStatus.PENDING,
                            current.orElseThrow().retryCount(),
                            null,
                            null);
                    appendTransition(tenantId, event);
                    count[0] = 1;
                });
                recovered += count[0];
            }
        }
        return recovered;
    }

    @Override
    public FileProcessingStatus status(TenantContext tenant, String fileKey) {
        return findFileInfo(tenant, fileKey)
                .map(StoredFileInfo::status)
                .orElseThrow(() -> new StoredFileNotFoundException("Stored file does not exist"));
    }

    private ClaimedFile claimedFile(SqliteMetadataProjectionStore.FileRow row) {
        Instant startedAt = row.processingStartedAtMillis() == null
                ? clock.instant()
                : Instant.ofEpochMilli(row.processingStartedAtMillis());
        return new ClaimedFile(
                toLocation(row),
                new ProcessingLease(row.tenantId(), row.fileKey(), UUID.fromString(row.leaseId()), startedAt));
    }

    private FileLocation toLocation(SqliteMetadataProjectionStore.FileRow row) {
        return new FileLocation(
                row.fileKey(),
                row.tenantId(),
                row.volumeId(),
                Path.of(row.physicalPath()),
                row.logicalDirectory(),
                row.fileSize(),
                Instant.ofEpochMilli(row.createdAtMillis()),
                row.status(),
                row.retryCount(),
                row.lastFailedAtMillis() == null ? null : Instant.ofEpochMilli(row.lastFailedAtMillis()),
                row.lastError(),
                row.availableAtMillis() == null ? null : Instant.ofEpochMilli(row.availableAtMillis()));
    }

    private void rollbackClaim(String tenantId, SqliteMetadataProjectionStore.ClaimedRow claimed) {
        SqliteMetadataProjectionStore.FileRow row = claimed.row();
        metadata.rollbackClaim(
                tenantId,
                row.fileKey(),
                UUID.fromString(row.leaseId()),
                claimed.previousStatus(),
                claimed.previousAvailableAtMillis());
    }

    private QueueEventRecord transitionEvent(
            SqliteMetadataProjectionStore.FileRow row,
            ProcessingLease lease,
            QueueEventType type,
            FileProcessingStatus status,
            int retryCount,
            Instant availableAt,
            String errorMessage) {
        return new QueueEventRecord(
                1,
                UUID.randomUUID(),
                lease.tenantId(),
                lease.fileKey(),
                type,
                clock.instant(),
                1,
                row.volumeId(),
                Path.of(row.physicalPath()),
                row.logicalDirectory(),
                row.fileSize(),
                status,
                lease.leaseId(),
                type == QueueEventType.PROCESSING_TIMED_OUT ? lease.startedAt() : null,
                retryCount,
                availableAt,
                errorMessage,
                row.originalFileName(),
                row.fileExtension());
    }

    private void appendTransition(String tenantId, QueueEventRecord event) {
        QueueEventRecord admitted = appender.append(event);
        terminalLeases.record(admitted);
        projectBestEffort(tenantId, 128);
    }

    private void projectBestEffort(String tenantId, int maxRecords) {
        try {
            projection.projectTenantUntilCaughtUp(tenantId, maxRecords);
        } catch (RuntimeException ignored) {
            // Journal admission is the commit point; a later projector retries from its durable cursor.
        }
    }

    private static void requireLease(ProcessingLease lease) {
        Objects.requireNonNull(lease, "lease");
    }

    private static void requireActiveLease(SqliteMetadataProjectionStore.FileRow row, ProcessingLease lease) {
        if (row.status() != FileProcessingStatus.PROCESSING
                || row.leaseId() == null
                || !row.leaseId().equals(lease.leaseId().toString())
                || !row.tenantId().equals(lease.tenantId())
                || !row.fileKey().equals(lease.fileKey())) {
            throw new LeaseMismatchException("Lease does not own active processing file");
        }
    }

    private TerminalLeaseIndex.Outcome knownOutcome(ProcessingLease lease) {
        return terminalLeases.find(lease);
    }

    private static RetryConfiguration defaultRetryConfiguration() {
        return new RetryConfiguration(3, Duration.ofSeconds(5), true, Duration.ofMinutes(5));
    }

    @Override
    public long totalCapacity() {
        return volumes.stream()
                .filter(DefaultStoragePool::healthy)
                .mapToLong(DefaultStoragePool::total)
                .reduce(0, DefaultStoragePool::saturatedAdd);
    }

    @Override
    public long availableCapacity() {
        return volumes.stream()
                .filter(DefaultStoragePool::healthy)
                .mapToLong(DefaultStoragePool::available)
                .reduce(0, DefaultStoragePool::saturatedAdd);
    }

    private static boolean healthy(StorageVolume volume) {
        try {
            return volume.healthy();
        } catch (RuntimeException ignored) {
            return false;
        }
    }

    private static long total(StorageVolume volume) {
        try {
            return Math.max(0, volume.totalCapacity());
        } catch (RuntimeException ignored) {
            return 0;
        }
    }

    private static long saturatedAdd(long left, long right) {
        return Long.MAX_VALUE - left < right ? Long.MAX_VALUE : left + right;
    }

    private void requireEnabled(TenantContext tenant) {
        requireTenant(tenant);
        TenantContext current = tenants.find(tenant.tenantId());
        if (current == null || current.status() != TenantStatus.ENABLED)
            throw new TenantDisabledException("Tenant is disabled: " + tenant.tenantId());
    }

    private static void requireTenant(TenantContext tenant) {
        Objects.requireNonNull(tenant, "tenant");
    }

    private static RuntimeNotReadyException unavailable() {
        return new RuntimeNotReadyException("Storage processing is not available in Task 10");
    }

    private static String extensionFrom(String name) {
        if (name == null) return "";
        int dot = name.lastIndexOf('.');
        if (dot <= 0 || dot == name.length() - 1) return "";
        String extension = name.substring(dot);
        return extension.matches("\\.[A-Za-z0-9._-]{1,31}") ? extension : "";
    }

    private String nextFileKey() {
        byte[] bytes = new byte[16];
        random.nextBytes(bytes);
        char[] result = new char[32];
        char[] hex = "0123456789abcdef".toCharArray();
        for (int index = 0; index < bytes.length; index++) {
            int value = Byte.toUnsignedInt(bytes[index]);
            result[index * 2] = hex[value >>> 4];
            result[index * 2 + 1] = hex[value & 15];
        }
        return new String(result);
    }

    @FunctionalInterface
    private interface StreamOpener {
        InputStream open();
    }
}
