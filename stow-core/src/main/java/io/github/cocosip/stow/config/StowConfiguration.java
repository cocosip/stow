package io.github.cocosip.stow.config;

import io.github.cocosip.stow.model.WatcherConfiguration;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;

public record StowConfiguration(
        PathsConfiguration paths,
        TenantConfiguration tenant,
        List<VolumeConfiguration> volumes,
        MetadataConfiguration metadata,
        StorageConfiguration storage,
        SqliteConfiguration sqlite,
        RetryConfiguration retry,
        JournalConfiguration journal,
        ProjectionConfiguration projection,
        SnapshotConfiguration snapshot,
        CompactionConfiguration compaction,
        CleanupConfiguration cleanup,
        OrphanRecoveryConfiguration orphanRecovery,
        SourceCleanupConfiguration sourceCleanup,
        StatisticsConfiguration statistics,
        List<WatcherConfiguration> watchers) {

    public StowConfiguration {
        ConfigurationValidation.nonNull("paths", paths);
        ConfigurationValidation.nonNull("tenant", tenant);
        ConfigurationValidation.nonNull("metadata", metadata);
        ConfigurationValidation.nonNull("storage", storage);
        ConfigurationValidation.nonNull("sqlite", sqlite);
        ConfigurationValidation.nonNull("retry", retry);
        ConfigurationValidation.nonNull("journal", journal);
        ConfigurationValidation.nonNull("projection", projection);
        ConfigurationValidation.nonNull("snapshot", snapshot);
        ConfigurationValidation.nonNull("compaction", compaction);
        ConfigurationValidation.nonNull("cleanup", cleanup);
        ConfigurationValidation.nonNull("orphanRecovery", orphanRecovery);
        ConfigurationValidation.nonNull("sourceCleanup", sourceCleanup);
        ConfigurationValidation.nonNull("statistics", statistics);
        ConfigurationValidation.nonNull("volumes", volumes);
        ConfigurationValidation.nonNull("watchers", watchers);
        volumes = List.copyOf(volumes);
        watchers = List.copyOf(watchers);
    }

    public StowConfiguration(
            PathsConfiguration paths,
            TenantConfiguration tenant,
            List<VolumeConfiguration> volumes,
            MetadataConfiguration metadata,
            StorageConfiguration storage,
            SqliteConfiguration sqlite,
            RetryConfiguration retry,
            JournalConfiguration journal,
            ProjectionConfiguration projection,
            SnapshotConfiguration snapshot,
            CompactionConfiguration compaction,
            CleanupConfiguration cleanup,
            OrphanRecoveryConfiguration orphanRecovery,
            StatisticsConfiguration statistics,
            List<WatcherConfiguration> watchers) {
        this(
                paths,
                tenant,
                volumes,
                metadata,
                storage,
                sqlite,
                retry,
                journal,
                projection,
                snapshot,
                compaction,
                cleanup,
                orphanRecovery,
                defaultSourceCleanup(paths.watcherDirectory()),
                statistics,
                watchers);
    }

    public static Builder builder() {
        return new Builder();
    }

    public static final class Builder {

        private Path metadataDirectory = Path.of("stow-metadata");
        private Path quotaDirectory = Path.of("stow-quota");
        private Path queueDirectory = Path.of("stow-queue");
        private Path watcherDirectory = Path.of("stow-watchers");
        private boolean autoCreateTenants;
        private long defaultQuota;
        private List<String> preconfiguredTenants = List.of();
        private List<VolumeConfiguration> volumes = List.of();
        private boolean backgroundPersistence = true;
        private int maxQueueSize = 100_000;
        private int drainBatchSize = 2_000;
        private int softMergeThresholdPercent = 90;
        private int startupLoadBatchSize = 2_000;
        private Duration shutdownDrainTimeout = Duration.ofSeconds(30);
        private Duration persistenceInterval = Duration.ofSeconds(2);
        private int completionGuardStripes = 256;
        private int emptyQueueReclaimBatchSize = 32;
        private int backgroundReclaimBatchSize = 8;
        private Duration reclaimCooldown = Duration.ofSeconds(30);
        private boolean backgroundReclaimEnabled = true;
        private SqliteJournalMode sqliteJournalMode = SqliteJournalMode.WAL;
        private SqliteSynchronousMode sqliteSynchronousMode = SqliteSynchronousMode.NORMAL;
        private int sqliteCacheSizeKb = -4_000;
        private Duration sqliteBusyTimeout = Duration.ofSeconds(5);
        private boolean checkpointAfterBatch;
        private int maxRetryCount = 3;
        private Duration retryInitialDelay = Duration.ofSeconds(5);
        private boolean exponentialBackoff = true;
        private Duration retryMaxDelay = Duration.ofMinutes(5);
        private boolean journalProjectionEnabled = true;
        private JournalFormat journalFormat = JournalFormat.BINARY_V1;
        private JournalAckMode journalAckMode = JournalAckMode.DURABLE;
        private Duration journalStateFlushDebounce = Duration.ofSeconds(1);
        private Duration journalLinger = Duration.ofMillis(1);
        private int journalMaxBatchRecords = 16;
        private int journalMaxBatchBytes = 262_144;
        private Duration journalWriterIdleTimeout = Duration.ofSeconds(30);
        private int journalAsyncQueueCapacityPerTenant = 8_192;
        private Duration journalBalancedFlushWindow = Duration.ofMillis(5);
        private int projectionMaxRecordsPerTenantCycle = 64;
        private int projectionMaxTenantsPerCycle = 8;
        private Duration projectionBusyCycleDelay = Duration.ofMillis(500);
        private Duration projectionIdleCycleDelay = Duration.ofSeconds(5);
        private Duration projectionCycleTimeBudget = Duration.ofSeconds(2);
        private boolean snapshotEnabled = true;
        private Duration snapshotInterval = Duration.ofMinutes(15);
        private long snapshotMinimumProgressBytes = 1_048_576;
        private boolean compactionEnabled = true;
        private long compactionMinimumProcessedBytes = 4_194_304;
        private boolean cleanupEnabled = true;
        private Duration cleanupInterval = Duration.ofHours(1);
        private Duration cleanupInitialDelay = Duration.ofMinutes(1);
        private Duration processingTimeout = Duration.ofMinutes(30);
        private Duration completedRetention = Duration.ZERO;
        private Duration failedRetention = Duration.ofDays(3);
        private PermanentlyFailedDisposition permanentlyFailedDisposition =
                PermanentlyFailedDisposition.MOVE_TO_DEAD_LETTER;
        private int cleanupBatchSizePerTenant = 500;
        private boolean orphanRecoveryEnabled;
        private boolean orphanRecoveryRunOnStartup;
        private Duration orphanRecoveryInterval = Duration.ofHours(6);
        private boolean sourceCleanupEnabled = true;
        private Path sourceCleanupDatabasePath;
        private Duration sourceCleanupPollInterval = Duration.ofSeconds(5);
        private int sourceCleanupMaxConcurrentActions = 2;
        private int sourceCleanupMaxActiveJobs = 10_000;
        private Duration sourceCleanupTerminalRetention = Duration.ofDays(1);
        private Duration sourceCleanupImportReservationTimeout = Duration.ofMinutes(10);
        private boolean sourceCleanupDatabaseOptimizationEnabled = true;
        private Duration sourceCleanupDatabaseOptimizationInterval = Duration.ofDays(1);
        private int sourceCleanupTerminalPruneBatchSize = 5_000;
        private boolean statisticsEnabled;
        private Duration statisticsWindowSize = Duration.ofMinutes(5);
        private Duration statisticsRetention = Duration.ofHours(1);
        private int statisticsMaxSeries = 16_384;
        private List<WatcherConfiguration> watchers = List.of();

        private Builder() {}

        public Builder metadataDirectory(Path value) {
            metadataDirectory = value;
            return this;
        }

        public Builder quotaDirectory(Path value) {
            quotaDirectory = value;
            return this;
        }

        public Builder queueDirectory(Path value) {
            queueDirectory = value;
            return this;
        }

        public Builder watcherDirectory(Path value) {
            watcherDirectory = value;
            return this;
        }

        public Builder autoCreateTenants(boolean value) {
            autoCreateTenants = value;
            return this;
        }

        public Builder defaultQuota(long value) {
            defaultQuota = value;
            return this;
        }

        public Builder preconfiguredTenants(List<String> value) {
            preconfiguredTenants = List.copyOf(ConfigurationValidation.nonNull("preconfiguredTenants", value));
            return this;
        }

        public Builder volumes(List<VolumeConfiguration> value) {
            volumes = List.copyOf(ConfigurationValidation.nonNull("volumes", value));
            return this;
        }

        public Builder backgroundPersistence(boolean value) {
            backgroundPersistence = value;
            return this;
        }

        public Builder maxQueueSize(int value) {
            maxQueueSize = value;
            return this;
        }

        public Builder drainBatchSize(int value) {
            drainBatchSize = value;
            return this;
        }

        public Builder softMergeThresholdPercent(int value) {
            softMergeThresholdPercent = value;
            return this;
        }

        public Builder startupLoadBatchSize(int value) {
            startupLoadBatchSize = value;
            return this;
        }

        public Builder shutdownDrainTimeout(Duration value) {
            shutdownDrainTimeout = value;
            return this;
        }

        public Builder persistenceInterval(Duration value) {
            persistenceInterval = value;
            return this;
        }

        public Builder completionGuardStripes(int value) {
            completionGuardStripes = value;
            return this;
        }

        public Builder emptyQueueReclaimBatchSize(int value) {
            emptyQueueReclaimBatchSize = value;
            return this;
        }

        public Builder backgroundReclaimBatchSize(int value) {
            backgroundReclaimBatchSize = value;
            return this;
        }

        public Builder reclaimCooldown(Duration value) {
            reclaimCooldown = value;
            return this;
        }

        public Builder backgroundReclaimEnabled(boolean value) {
            backgroundReclaimEnabled = value;
            return this;
        }

        public Builder sqliteJournalMode(SqliteJournalMode value) {
            sqliteJournalMode = value;
            return this;
        }

        public Builder sqliteSynchronousMode(SqliteSynchronousMode value) {
            sqliteSynchronousMode = value;
            return this;
        }

        public Builder sqliteCacheSizeKb(int value) {
            sqliteCacheSizeKb = value;
            return this;
        }

        public Builder sqliteBusyTimeout(Duration value) {
            sqliteBusyTimeout = value;
            return this;
        }

        public Builder checkpointAfterBatch(boolean value) {
            checkpointAfterBatch = value;
            return this;
        }

        public Builder maxRetryCount(int value) {
            maxRetryCount = value;
            return this;
        }

        public Builder retryInitialDelay(Duration value) {
            retryInitialDelay = value;
            return this;
        }

        public Builder exponentialBackoff(boolean value) {
            exponentialBackoff = value;
            return this;
        }

        public Builder retryMaxDelay(Duration value) {
            retryMaxDelay = value;
            return this;
        }

        public Builder journalProjectionEnabled(boolean value) {
            journalProjectionEnabled = value;
            return this;
        }

        public Builder journalFormat(JournalFormat value) {
            journalFormat = value;
            return this;
        }

        public Builder journalAckMode(JournalAckMode value) {
            journalAckMode = value;
            return this;
        }

        public Builder journalStateFlushDebounce(Duration value) {
            journalStateFlushDebounce = value;
            return this;
        }

        public Builder journalLinger(Duration value) {
            journalLinger = value;
            return this;
        }

        public Builder journalMaxBatchRecords(int value) {
            journalMaxBatchRecords = value;
            return this;
        }

        public Builder journalMaxBatchBytes(int value) {
            journalMaxBatchBytes = value;
            return this;
        }

        public Builder journalWriterIdleTimeout(Duration value) {
            journalWriterIdleTimeout = value;
            return this;
        }

        public Builder journalAsyncQueueCapacityPerTenant(int value) {
            journalAsyncQueueCapacityPerTenant = value;
            return this;
        }

        public Builder journalBalancedFlushWindow(Duration value) {
            journalBalancedFlushWindow = value;
            return this;
        }

        public Builder projectionMaxRecordsPerTenantCycle(int value) {
            projectionMaxRecordsPerTenantCycle = value;
            return this;
        }

        public Builder projectionMaxTenantsPerCycle(int value) {
            projectionMaxTenantsPerCycle = value;
            return this;
        }

        public Builder projectionBusyCycleDelay(Duration value) {
            projectionBusyCycleDelay = value;
            return this;
        }

        public Builder projectionIdleCycleDelay(Duration value) {
            projectionIdleCycleDelay = value;
            return this;
        }

        public Builder projectionCycleTimeBudget(Duration value) {
            projectionCycleTimeBudget = value;
            return this;
        }

        public Builder snapshotEnabled(boolean value) {
            snapshotEnabled = value;
            return this;
        }

        public Builder snapshotInterval(Duration value) {
            snapshotInterval = value;
            return this;
        }

        public Builder snapshotMinimumProgressBytes(long value) {
            snapshotMinimumProgressBytes = value;
            return this;
        }

        public Builder compactionEnabled(boolean value) {
            compactionEnabled = value;
            return this;
        }

        public Builder compactionMinimumProcessedBytes(long value) {
            compactionMinimumProcessedBytes = value;
            return this;
        }

        public Builder cleanupEnabled(boolean value) {
            cleanupEnabled = value;
            return this;
        }

        public Builder cleanupInterval(Duration value) {
            cleanupInterval = value;
            return this;
        }

        public Builder cleanupInitialDelay(Duration value) {
            cleanupInitialDelay = value;
            return this;
        }

        public Builder processingTimeout(Duration value) {
            processingTimeout = value;
            return this;
        }

        public Builder completedRetention(Duration value) {
            completedRetention = value;
            return this;
        }

        public Builder failedRetention(Duration value) {
            failedRetention = value;
            return this;
        }

        public Builder permanentlyFailedDisposition(PermanentlyFailedDisposition value) {
            permanentlyFailedDisposition = value;
            return this;
        }

        public Builder cleanupBatchSizePerTenant(int value) {
            cleanupBatchSizePerTenant = value;
            return this;
        }

        public Builder orphanRecoveryEnabled(boolean value) {
            orphanRecoveryEnabled = value;
            return this;
        }

        public Builder orphanRecoveryRunOnStartup(boolean value) {
            orphanRecoveryRunOnStartup = value;
            return this;
        }

        public Builder orphanRecoveryInterval(Duration value) {
            orphanRecoveryInterval = value;
            return this;
        }

        public Builder sourceCleanupEnabled(boolean value) {
            sourceCleanupEnabled = value;
            return this;
        }

        public Builder sourceCleanupDatabasePath(Path value) {
            sourceCleanupDatabasePath = value;
            return this;
        }

        public Builder sourceCleanupPollInterval(Duration value) {
            sourceCleanupPollInterval = value;
            return this;
        }

        public Builder sourceCleanupMaxConcurrentActions(int value) {
            sourceCleanupMaxConcurrentActions = value;
            return this;
        }

        public Builder sourceCleanupMaxActiveJobs(int value) {
            sourceCleanupMaxActiveJobs = value;
            return this;
        }

        public Builder sourceCleanupTerminalRetention(Duration value) {
            sourceCleanupTerminalRetention = value;
            return this;
        }

        public Builder sourceCleanupImportReservationTimeout(Duration value) {
            sourceCleanupImportReservationTimeout = value;
            return this;
        }

        public Builder sourceCleanupDatabaseOptimizationEnabled(boolean value) {
            sourceCleanupDatabaseOptimizationEnabled = value;
            return this;
        }

        public Builder sourceCleanupDatabaseOptimizationInterval(Duration value) {
            sourceCleanupDatabaseOptimizationInterval = value;
            return this;
        }

        public Builder sourceCleanupTerminalPruneBatchSize(int value) {
            sourceCleanupTerminalPruneBatchSize = value;
            return this;
        }

        public Builder statisticsEnabled(boolean value) {
            statisticsEnabled = value;
            return this;
        }

        public Builder statisticsWindowSize(Duration value) {
            statisticsWindowSize = value;
            return this;
        }

        public Builder statisticsRetention(Duration value) {
            statisticsRetention = value;
            return this;
        }

        public Builder statisticsMaxSeries(int value) {
            statisticsMaxSeries = value;
            return this;
        }

        public Builder watchers(List<WatcherConfiguration> value) {
            watchers = List.copyOf(ConfigurationValidation.nonNull("watchers", value));
            return this;
        }

        public StowConfiguration build() {
            return new StowConfiguration(
                    new PathsConfiguration(metadataDirectory, quotaDirectory, queueDirectory, watcherDirectory),
                    new TenantConfiguration(autoCreateTenants, defaultQuota, preconfiguredTenants),
                    volumes,
                    new MetadataConfiguration(
                            backgroundPersistence,
                            maxQueueSize,
                            drainBatchSize,
                            softMergeThresholdPercent,
                            startupLoadBatchSize,
                            shutdownDrainTimeout,
                            persistenceInterval),
                    new StorageConfiguration(
                            completionGuardStripes,
                            emptyQueueReclaimBatchSize,
                            backgroundReclaimBatchSize,
                            reclaimCooldown,
                            backgroundReclaimEnabled),
                    new SqliteConfiguration(
                            sqliteJournalMode,
                            sqliteSynchronousMode,
                            sqliteCacheSizeKb,
                            sqliteBusyTimeout,
                            checkpointAfterBatch),
                    new RetryConfiguration(maxRetryCount, retryInitialDelay, exponentialBackoff, retryMaxDelay),
                    new JournalConfiguration(
                            true,
                            journalProjectionEnabled,
                            journalFormat,
                            journalAckMode,
                            journalStateFlushDebounce,
                            journalLinger,
                            journalMaxBatchRecords,
                            journalMaxBatchBytes,
                            journalWriterIdleTimeout,
                            journalAsyncQueueCapacityPerTenant,
                            journalBalancedFlushWindow),
                    new ProjectionConfiguration(
                            projectionMaxRecordsPerTenantCycle,
                            projectionMaxTenantsPerCycle,
                            projectionBusyCycleDelay,
                            projectionIdleCycleDelay,
                            projectionCycleTimeBudget),
                    new SnapshotConfiguration(snapshotEnabled, snapshotInterval, snapshotMinimumProgressBytes),
                    new CompactionConfiguration(compactionEnabled, compactionMinimumProcessedBytes),
                    new CleanupConfiguration(
                            cleanupEnabled,
                            cleanupInterval,
                            cleanupInitialDelay,
                            processingTimeout,
                            completedRetention,
                            failedRetention,
                            permanentlyFailedDisposition,
                            cleanupBatchSizePerTenant),
                    new OrphanRecoveryConfiguration(
                            orphanRecoveryEnabled, orphanRecoveryRunOnStartup, orphanRecoveryInterval),
                    new SourceCleanupConfiguration(
                            sourceCleanupEnabled,
                            sourceCleanupDatabasePath == null
                                    ? watcherDirectory.resolve("source-cleanup.db")
                                    : sourceCleanupDatabasePath,
                            sourceCleanupPollInterval,
                            sourceCleanupMaxConcurrentActions,
                            sourceCleanupMaxActiveJobs,
                            sourceCleanupTerminalRetention,
                            sourceCleanupImportReservationTimeout,
                            sourceCleanupDatabaseOptimizationEnabled,
                            sourceCleanupDatabaseOptimizationInterval,
                            sourceCleanupTerminalPruneBatchSize),
                    new StatisticsConfiguration(
                            statisticsEnabled, statisticsWindowSize, statisticsRetention, statisticsMaxSeries),
                    watchers);
        }
    }

    private static SourceCleanupConfiguration defaultSourceCleanup(Path watcherDirectory) {
        return new SourceCleanupConfiguration(
                true,
                watcherDirectory.resolve("source-cleanup.db"),
                Duration.ofSeconds(5),
                2,
                10_000,
                Duration.ofDays(1),
                Duration.ofMinutes(10),
                true,
                Duration.ofDays(1),
                5_000);
    }
}
