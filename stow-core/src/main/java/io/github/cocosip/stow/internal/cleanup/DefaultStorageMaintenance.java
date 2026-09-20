package io.github.cocosip.stow.internal.cleanup;

import io.github.cocosip.stow.api.StorageMaintenance;
import io.github.cocosip.stow.config.CleanupConfiguration;
import io.github.cocosip.stow.config.SqliteConfiguration;
import io.github.cocosip.stow.internal.journal.SequencedJournalAppender;
import io.github.cocosip.stow.internal.projection.QueueProjectionService;
import io.github.cocosip.stow.internal.projection.SqliteMetadataProjectionStore;
import io.github.cocosip.stow.internal.quota.SqliteQuotaRepository;
import io.github.cocosip.stow.internal.recovery.DatabaseHealthService;
import io.github.cocosip.stow.internal.recovery.DatabaseRecoveryService;
import io.github.cocosip.stow.internal.recovery.OrphanFileRecovery;
import io.github.cocosip.stow.internal.scheduler.ProcessingTimeoutRecovery;
import io.github.cocosip.stow.model.CleanupStatistics;
import io.github.cocosip.stow.model.DatabaseHealthReport;
import io.github.cocosip.stow.model.DatabaseOptimizationResult;
import io.github.cocosip.stow.model.DatabaseRebuildResult;
import io.github.cocosip.stow.model.MaintenanceError;
import io.github.cocosip.stow.spi.QueueEventJournal;
import io.github.cocosip.stow.spi.StorageVolume;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.DriverManager;
import java.time.Clock;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.function.ToLongFunction;

public final class DefaultStorageMaintenance implements StorageMaintenance {

    private final QueueEventJournal journal;
    private final SqliteMetadataProjectionStore metadata;
    private final SqliteQuotaRepository quota;
    private final QueueProjectionService projection;
    private final List<StorageVolume> volumes;
    private final Clock clock;
    private final CleanupConfiguration configuration;
    private final CompletedFileReaper completed;
    private final PermanentFailureReaper permanentFailure;
    private final OrphanFileRecovery orphans;
    private final JunkFileCleaner junk;
    private final DatabaseHealthService health;
    private final DatabaseRecoveryService recovery;
    private final ProcessingTimeoutRecovery timeoutRecovery;
    private final Path metadataRoot;
    private final Path quotaRoot;

    public DefaultStorageMaintenance(
            QueueEventJournal journal,
            SqliteMetadataProjectionStore metadata,
            SqliteQuotaRepository quota,
            QueueProjectionService projection,
            List<StorageVolume> volumes,
            Path metadataRoot,
            Path quotaRoot,
            SqliteConfiguration sqlite,
            Clock clock,
            CleanupConfiguration configuration,
            ToLongFunction<String> initialTenantLimit) {
        this(
                journal,
                metadata,
                quota,
                projection,
                volumes,
                metadataRoot,
                quotaRoot,
                sqlite,
                clock,
                configuration,
                initialTenantLimit,
                null,
                new SequencedJournalAppender(journal));
    }

    public DefaultStorageMaintenance(
            QueueEventJournal journal,
            SqliteMetadataProjectionStore metadata,
            SqliteQuotaRepository quota,
            QueueProjectionService projection,
            List<StorageVolume> volumes,
            Path metadataRoot,
            Path quotaRoot,
            SqliteConfiguration sqlite,
            Clock clock,
            CleanupConfiguration configuration,
            ToLongFunction<String> initialTenantLimit,
            ProcessingTimeoutRecovery timeoutRecovery) {
        this(
                journal,
                metadata,
                quota,
                projection,
                volumes,
                metadataRoot,
                quotaRoot,
                sqlite,
                clock,
                configuration,
                initialTenantLimit,
                timeoutRecovery,
                new SequencedJournalAppender(journal));
    }

    public DefaultStorageMaintenance(
            QueueEventJournal journal,
            SqliteMetadataProjectionStore metadata,
            SqliteQuotaRepository quota,
            QueueProjectionService projection,
            List<StorageVolume> volumes,
            Path metadataRoot,
            Path quotaRoot,
            SqliteConfiguration sqlite,
            Clock clock,
            CleanupConfiguration configuration,
            ToLongFunction<String> initialTenantLimit,
            ProcessingTimeoutRecovery timeoutRecovery,
            SequencedJournalAppender appender) {
        this.journal = Objects.requireNonNull(journal, "journal");
        this.metadata = Objects.requireNonNull(metadata, "metadata");
        this.quota = Objects.requireNonNull(quota, "quota");
        this.projection = Objects.requireNonNull(projection, "projection");
        this.volumes = List.copyOf(Objects.requireNonNull(volumes, "volumes"));
        this.metadataRoot = Objects.requireNonNull(metadataRoot, "metadataRoot")
                .toAbsolutePath()
                .normalize();
        this.quotaRoot =
                Objects.requireNonNull(quotaRoot, "quotaRoot").toAbsolutePath().normalize();
        this.clock = Objects.requireNonNull(clock, "clock");
        this.configuration = Objects.requireNonNull(configuration, "configuration");
        SqliteConfiguration effectiveSqlite =
                sqlite == null ? io.github.cocosip.stow.internal.sqlite.SqliteConnectionFactory.defaults() : sqlite;
        ToLongFunction<String> effectiveLimit = initialTenantLimit == null ? ignored -> 0 : initialTenantLimit;
        SequencedJournalAppender sharedAppender = Objects.requireNonNull(appender, "appender");
        completed = new CompletedFileReaper(journal, metadata, projection, this.volumes, clock, sharedAppender);
        permanentFailure =
                new PermanentFailureReaper(journal, metadata, projection, this.volumes, clock, sharedAppender);
        orphans = new OrphanFileRecovery(journal, metadata, quota, projection, this.volumes, clock, sharedAppender);
        junk = new JunkFileCleaner(this.volumes, List.of(this.metadataRoot, this.quotaRoot), clock);
        health = new DatabaseHealthService(this.metadataRoot, this.quotaRoot, clock);
        recovery = new DatabaseRecoveryService(
                this.metadataRoot, this.quotaRoot, journal, effectiveSqlite, clock, effectiveLimit);
        this.timeoutRecovery = timeoutRecovery;
    }

    @Override
    public CleanupStatistics cleanupCompleted(java.time.Duration olderThan) {
        return completed.run(olderThan, configuration.batchSizePerTenant());
    }

    @Override
    public CleanupStatistics cleanupPermanentlyFailed(java.time.Duration olderThan) {
        return permanentFailure.run(
                olderThan, configuration.batchSizePerTenant(), configuration.permanentlyFailedDisposition());
    }

    @Override
    public CleanupStatistics reclaimTimedOutProcessing(java.time.Duration timeout) {
        if (timeoutRecovery == null) throw new IllegalStateException("processing timeout recovery is not configured");
        if (timeout == null || timeout.isNegative()) throw new IllegalArgumentException("timeout must be non-negative");
        CleanupStatisticsBuilder statistics = new CleanupStatisticsBuilder(clock);
        int recovered = timeoutRecovery.recover(timeout);
        if (recovered > 0) statistics.succeeded(null, 0);
        return statistics.build();
    }

    @Override
    public CleanupStatistics recoverOrphans(String tenantId) {
        return orphans.recover(tenantId, configuration.batchSizePerTenant());
    }

    @Override
    public CleanupStatistics recoverAllOrphans() {
        return orphans.recoverAll(configuration.batchSizePerTenant());
    }

    @Override
    public void reconcileQuota(String tenantId) {
        quota.rebuildFromMetadata(tenantId, metadata.activeFiles(tenantId));
    }

    @Override
    public void reconcileAllQuotas() {
        for (String tenantId : journal.tenantIds()) reconcileQuota(tenantId);
    }

    @Override
    public DatabaseHealthReport checkDatabases() {
        return health.checkDatabases();
    }

    @Override
    public DatabaseRebuildResult rebuildMetadata(String tenantId) {
        return recovery.rebuildMetadata(tenantId);
    }

    @Override
    public DatabaseRebuildResult rebuildQuota(String tenantId) {
        return recovery.rebuildQuota(tenantId);
    }

    @Override
    public DatabaseOptimizationResult optimizeDatabases() {
        OptimizationBuilder result = new OptimizationBuilder(clock);
        for (String tenantId : journal.tenantIds()) {
            result.scanned++;
            boolean optimized = false;
            for (DatabasePath database :
                    List.of(new DatabasePath(metadataRoot, "metadata.db"), new DatabasePath(quotaRoot, "quotas.db"))) {
                Path path = database.root.resolve(tenantId).resolve(database.fileName);
                if (!Files.exists(path)) continue;
                try (var connection = DriverManager.getConnection("jdbc:sqlite:" + path);
                        var statement = connection.createStatement()) {
                    statement.execute("PRAGMA wal_checkpoint(TRUNCATE)");
                    statement.execute("VACUUM");
                    optimized = true;
                } catch (Exception exception) {
                    result.failed++;
                    result.errors.add(new MaintenanceError(tenantId, "optimize-database", message(exception)));
                }
            }
            if (optimized) {
                result.optimized++;
                result.tenants.add(tenantId);
            } else {
                result.skipped++;
            }
        }
        return result.build();
    }

    @Override
    public CleanupStatistics cleanupInvalidDatabaseBackups() {
        return junk.cleanupInvalidDatabaseBackups(configuration.batchSizePerTenant());
    }

    @Override
    public CleanupStatistics cleanupEmptyDirectories() {
        return junk.cleanupEmptyDirectories();
    }

    @Override
    public CleanupStatistics cleanupJunkFiles() {
        return junk.cleanupJunkFiles(configuration.batchSizePerTenant());
    }

    private static String message(Exception exception) {
        return exception.getMessage() == null ? exception.getClass().getSimpleName() : exception.getMessage();
    }

    private record DatabasePath(Path root, String fileName) {}

    private static final class OptimizationBuilder {
        private final Instant startedAt;
        private final Clock clock;
        private final java.util.Set<String> tenants = new java.util.HashSet<>();
        private final List<MaintenanceError> errors = new ArrayList<>();
        private long scanned;
        private long optimized;
        private long skipped;
        private long failed;

        private OptimizationBuilder(Clock clock) {
            this.clock = clock;
            startedAt = clock.instant();
        }

        private DatabaseOptimizationResult build() {
            return new DatabaseOptimizationResult(
                    startedAt,
                    clock.instant(),
                    scanned,
                    optimized,
                    skipped,
                    failed,
                    0,
                    tenants.size(),
                    List.copyOf(errors));
        }
    }
}
