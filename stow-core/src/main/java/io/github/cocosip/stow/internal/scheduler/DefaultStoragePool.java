package io.github.cocosip.stow.internal.scheduler;

import io.github.cocosip.stow.api.ContentSource;
import io.github.cocosip.stow.api.StoragePool;
import io.github.cocosip.stow.api.TenantManager;
import io.github.cocosip.stow.exception.InsufficientStorageException;
import io.github.cocosip.stow.exception.PhysicalFileMissingException;
import io.github.cocosip.stow.exception.RuntimeNotReadyException;
import io.github.cocosip.stow.exception.StoredFileNotFoundException;
import io.github.cocosip.stow.exception.TenantDisabledException;
import io.github.cocosip.stow.internal.projection.QueueProjectionService;
import io.github.cocosip.stow.internal.projection.SqliteMetadataProjectionStore;
import io.github.cocosip.stow.internal.quota.QuotaReservation;
import io.github.cocosip.stow.internal.quota.SqliteQuotaRepository;
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
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

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
    private final List<StorageVolume> volumes;
    private final Clock clock;
    private final SecureRandom random = new SecureRandom();
    private final Map<String, AtomicLong> sequences = new ConcurrentHashMap<>();

    public DefaultStoragePool(
            TenantLookup tenants,
            SqliteQuotaRepository quota,
            SqliteMetadataProjectionStore metadata,
            QueueProjectionService projection,
            QueueEventJournal journal,
            StorageVolume volume,
            Clock clock) {
        this(tenants, quota, metadata, projection, journal, List.of(volume), clock);
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
        this.tenants = Objects.requireNonNull(tenants, "tenants");
        this.quota = Objects.requireNonNull(quota, "quota");
        this.metadata = Objects.requireNonNull(metadata, "metadata");
        this.projection = Objects.requireNonNull(projection, "projection");
        this.journal = Objects.requireNonNull(journal, "journal");
        this.volumes = List.copyOf(volumes);
        if (this.volumes.isEmpty()) throw new IllegalArgumentException("volumes must not be empty");
        this.clock = Objects.requireNonNull(clock, "clock");
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
                try {
                    CountingInputStream counted =
                            opened instanceof CountingInputStream c ? c : new CountingInputStream(opened);
                    volume.write(temporary, counted);
                    fileSize = counted.count();
                } finally {
                    if (closeStream) {
                        try {
                            opened.close();
                        } catch (java.io.IOException exception) {
                            throw new java.io.UncheckedIOException("Unable to close content stream", exception);
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

        boolean appended = false;
        try {
            long sequence = nextSequence(tenant.tenantId());
            QueueEventRecord accepted = new QueueEventRecord(
                    1,
                    java.util.UUID.randomUUID(),
                    tenant.tenantId(),
                    fileKey,
                    QueueEventType.ACCEPTED,
                    clock.instant(),
                    sequence,
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
            journal.append(accepted);
            appended = true;
            projection.projectTenantUntilCaughtUp(tenant.tenantId(), 128);
            return fileKey;
        } catch (RuntimeException failure) {
            if (!appended) sequences.get(tenant.tenantId()).decrementAndGet();
            compensation.cleanupAfterFailure();
            throw failure;
        }
    }

    private long nextSequence(String tenantId) {
        AtomicLong next = sequences.computeIfAbsent(tenantId, id -> {
            long current = journal.readBatch(id, journal.baseOffset(id), Integer.MAX_VALUE)
                    .lastSequenceNumber();
            return new AtomicLong(current);
        });
        return next.incrementAndGet();
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
            return volume.read(location.physicalPath());
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
        throw unavailable();
    }

    @Override
    public List<ClaimedFile> claimBatch(TenantContext tenant, int batchSize) {
        throw unavailable();
    }

    @Override
    public void complete(ProcessingLease lease) {
        throw unavailable();
    }

    @Override
    public void fail(ProcessingLease lease, String errorMessage) {
        throw unavailable();
    }

    @Override
    public FileProcessingStatus status(TenantContext tenant, String fileKey) {
        return findFileInfo(tenant, fileKey)
                .map(StoredFileInfo::status)
                .orElseThrow(() -> new StoredFileNotFoundException("Stored file does not exist"));
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
