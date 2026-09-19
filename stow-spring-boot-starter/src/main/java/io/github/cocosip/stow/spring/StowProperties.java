package io.github.cocosip.stow.spring;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import io.github.cocosip.stow.config.JournalAckMode;
import io.github.cocosip.stow.config.JournalFormat;
import io.github.cocosip.stow.config.PermanentlyFailedDisposition;
import io.github.cocosip.stow.config.SqliteJournalMode;
import io.github.cocosip.stow.config.SqliteSynchronousMode;
import io.github.cocosip.stow.config.StowConfiguration;
import io.github.cocosip.stow.config.VolumeConfiguration;
import io.github.cocosip.stow.exception.InvalidConfigurationException;
import io.github.cocosip.stow.model.PostImportAction;
import io.github.cocosip.stow.model.WatcherConfiguration;
import io.github.cocosip.stow.model.WatcherTenantMode;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import org.springframework.boot.context.properties.ConfigurationProperties;

/** Spring-bound representation of the framework-neutral Stow configuration. */
@ConfigurationProperties("stow")
@SuppressFBWarnings(
        value = "EI_EXPOSE_REP",
        justification = "Spring Boot binding requires mutable nested configuration objects and lists")
public class StowProperties {

    private final PathsProperties paths = new PathsProperties();
    private final TenantProperties tenant = new TenantProperties();
    private final MetadataProperties metadata = new MetadataProperties();
    private final StorageProperties storage = new StorageProperties();
    private final SqliteProperties sqlite = new SqliteProperties();
    private final RetryProperties retry = new RetryProperties();
    private final JournalProperties journal = new JournalProperties();
    private final ProjectionProperties projection = new ProjectionProperties();
    private final SnapshotProperties snapshot = new SnapshotProperties();
    private final CompactionProperties compaction = new CompactionProperties();
    private final CleanupProperties cleanup = new CleanupProperties();
    private final OrphanRecoveryProperties orphanRecovery = new OrphanRecoveryProperties();
    private final StatisticsProperties statistics = new StatisticsProperties();
    private final ActuatorProperties actuator = new ActuatorProperties();
    private final MetricsProperties metrics = new MetricsProperties();
    private List<VolumeProperties> volumes = new ArrayList<>();
    private List<WatcherProperties> watchers = new ArrayList<>();

    public StowConfiguration toConfiguration() {
        if (!journal.enabled) {
            throw new InvalidConfigurationException("journal.enabled cannot be disabled");
        }
        StowConfiguration.Builder builder = StowConfiguration.builder()
                .metadataDirectory(paths.metadataDirectory)
                .quotaDirectory(paths.quotaDirectory)
                .queueDirectory(paths.queueDirectory)
                .watcherDirectory(paths.watcherDirectory)
                .autoCreateTenants(tenant.autoCreateTenants)
                .defaultQuota(tenant.defaultQuota)
                .preconfiguredTenants(tenant.preconfiguredTenants)
                .backgroundPersistence(metadata.backgroundPersistence)
                .maxQueueSize(metadata.maxQueueSize)
                .drainBatchSize(metadata.drainBatchSize)
                .softMergeThresholdPercent(metadata.softMergeThresholdPercent)
                .startupLoadBatchSize(metadata.startupLoadBatchSize)
                .shutdownDrainTimeout(metadata.shutdownDrainTimeout)
                .persistenceInterval(metadata.persistenceInterval)
                .completionGuardStripes(storage.completionGuardStripes)
                .emptyQueueReclaimBatchSize(storage.emptyQueueReclaimBatchSize)
                .backgroundReclaimBatchSize(storage.backgroundReclaimBatchSize)
                .reclaimCooldown(storage.reclaimCooldown)
                .backgroundReclaimEnabled(storage.backgroundReclaimEnabled)
                .sqliteJournalMode(sqlite.journalMode)
                .sqliteSynchronousMode(sqlite.synchronousMode)
                .sqliteCacheSizeKb(sqlite.cacheSizeKb)
                .sqliteBusyTimeout(sqlite.busyTimeout)
                .checkpointAfterBatch(sqlite.checkpointAfterBatch)
                .maxRetryCount(retry.maxRetryCount)
                .retryInitialDelay(retry.initialDelay)
                .exponentialBackoff(retry.exponentialBackoff)
                .retryMaxDelay(retry.maxDelay)
                .journalProjectionEnabled(journal.projectionEnabled)
                .journalFormat(journal.format)
                .journalAckMode(journal.ackMode)
                .journalStateFlushDebounce(journal.stateFlushDebounce)
                .journalLinger(journal.linger)
                .journalMaxBatchRecords(journal.maxBatchRecords)
                .journalMaxBatchBytes(journal.maxBatchBytes)
                .journalWriterIdleTimeout(journal.writerIdleTimeout)
                .journalAsyncQueueCapacityPerTenant(journal.asyncQueueCapacityPerTenant)
                .journalBalancedFlushWindow(journal.balancedFlushWindow)
                .projectionMaxRecordsPerTenantCycle(projection.maxRecordsPerTenantCycle)
                .projectionMaxTenantsPerCycle(projection.maxTenantsPerCycle)
                .projectionBusyCycleDelay(projection.busyCycleDelay)
                .projectionIdleCycleDelay(projection.idleCycleDelay)
                .projectionCycleTimeBudget(projection.cycleTimeBudget)
                .snapshotEnabled(snapshot.enabled)
                .snapshotInterval(snapshot.interval)
                .snapshotMinimumProgressBytes(snapshot.minimumProgressBytes)
                .compactionEnabled(compaction.enabled)
                .compactionMinimumProcessedBytes(compaction.minimumProcessedBytes)
                .cleanupEnabled(cleanup.enabled)
                .cleanupInterval(cleanup.interval)
                .cleanupInitialDelay(cleanup.initialDelay)
                .processingTimeout(cleanup.processingTimeout)
                .completedRetention(cleanup.completedRetention)
                .failedRetention(cleanup.failedRetention)
                .permanentlyFailedDisposition(cleanup.permanentlyFailedDisposition)
                .cleanupBatchSizePerTenant(cleanup.batchSizePerTenant)
                .orphanRecoveryEnabled(orphanRecovery.enabled)
                .orphanRecoveryRunOnStartup(orphanRecovery.runOnStartup)
                .orphanRecoveryInterval(orphanRecovery.interval)
                .statisticsEnabled(statistics.enabled)
                .statisticsWindowSize(statistics.windowSize)
                .statisticsRetention(statistics.retention)
                .statisticsMaxSeries(statistics.maxSeries)
                .volumes(volumes.stream().map(VolumeProperties::toConfiguration).toList())
                .watchers(watchers.stream()
                        .map(WatcherProperties::toConfiguration)
                        .toList());
        return builder.build();
    }

    public PathsProperties getPaths() {
        return paths;
    }

    public TenantProperties getTenant() {
        return tenant;
    }

    public MetadataProperties getMetadata() {
        return metadata;
    }

    public StorageProperties getStorage() {
        return storage;
    }

    public SqliteProperties getSqlite() {
        return sqlite;
    }

    public RetryProperties getRetry() {
        return retry;
    }

    public JournalProperties getJournal() {
        return journal;
    }

    public ProjectionProperties getProjection() {
        return projection;
    }

    public SnapshotProperties getSnapshot() {
        return snapshot;
    }

    public CompactionProperties getCompaction() {
        return compaction;
    }

    public CleanupProperties getCleanup() {
        return cleanup;
    }

    public OrphanRecoveryProperties getOrphanRecovery() {
        return orphanRecovery;
    }

    public StatisticsProperties getStatistics() {
        return statistics;
    }

    public ActuatorProperties getActuator() {
        return actuator;
    }

    public MetricsProperties getMetrics() {
        return metrics;
    }

    public List<VolumeProperties> getVolumes() {
        return volumes;
    }

    public void setVolumes(List<VolumeProperties> value) {
        volumes = value == null ? new ArrayList<>() : new ArrayList<>(value);
    }

    public List<WatcherProperties> getWatchers() {
        return watchers;
    }

    public void setWatchers(List<WatcherProperties> value) {
        watchers = value == null ? new ArrayList<>() : new ArrayList<>(value);
    }

    public static class PathsProperties {
        private Path metadataDirectory = Path.of("stow-metadata");
        private Path quotaDirectory = Path.of("stow-quota");
        private Path queueDirectory = Path.of("stow-queue");
        private Path watcherDirectory = Path.of("stow-watchers");

        public Path getMetadataDirectory() {
            return metadataDirectory;
        }

        public void setMetadataDirectory(Path value) {
            metadataDirectory = value;
        }

        public Path getQuotaDirectory() {
            return quotaDirectory;
        }

        public void setQuotaDirectory(Path value) {
            quotaDirectory = value;
        }

        public Path getQueueDirectory() {
            return queueDirectory;
        }

        public void setQueueDirectory(Path value) {
            queueDirectory = value;
        }

        public Path getWatcherDirectory() {
            return watcherDirectory;
        }

        public void setWatcherDirectory(Path value) {
            watcherDirectory = value;
        }
    }

    public static class TenantProperties {
        private boolean autoCreateTenants;
        private long defaultQuota;
        private List<String> preconfiguredTenants = new ArrayList<>();

        public boolean isAutoCreateTenants() {
            return autoCreateTenants;
        }

        public void setAutoCreateTenants(boolean value) {
            autoCreateTenants = value;
        }

        public long getDefaultQuota() {
            return defaultQuota;
        }

        public void setDefaultQuota(long value) {
            defaultQuota = value;
        }

        public List<String> getPreconfiguredTenants() {
            return preconfiguredTenants;
        }

        public void setPreconfiguredTenants(List<String> value) {
            preconfiguredTenants = value == null ? new ArrayList<>() : new ArrayList<>(value);
        }
    }

    public static class MetadataProperties {
        private boolean backgroundPersistence = true;
        private int maxQueueSize = 100_000;
        private int drainBatchSize = 2_000;
        private int softMergeThresholdPercent = 90;
        private int startupLoadBatchSize = 2_000;
        private Duration shutdownDrainTimeout = Duration.ofSeconds(30);
        private Duration persistenceInterval = Duration.ofSeconds(2);

        public boolean isBackgroundPersistence() {
            return backgroundPersistence;
        }

        public void setBackgroundPersistence(boolean value) {
            backgroundPersistence = value;
        }

        public int getMaxQueueSize() {
            return maxQueueSize;
        }

        public void setMaxQueueSize(int value) {
            maxQueueSize = value;
        }

        public int getDrainBatchSize() {
            return drainBatchSize;
        }

        public void setDrainBatchSize(int value) {
            drainBatchSize = value;
        }

        public int getSoftMergeThresholdPercent() {
            return softMergeThresholdPercent;
        }

        public void setSoftMergeThresholdPercent(int value) {
            softMergeThresholdPercent = value;
        }

        public int getStartupLoadBatchSize() {
            return startupLoadBatchSize;
        }

        public void setStartupLoadBatchSize(int value) {
            startupLoadBatchSize = value;
        }

        public Duration getShutdownDrainTimeout() {
            return shutdownDrainTimeout;
        }

        public void setShutdownDrainTimeout(Duration value) {
            shutdownDrainTimeout = value;
        }

        public Duration getPersistenceInterval() {
            return persistenceInterval;
        }

        public void setPersistenceInterval(Duration value) {
            persistenceInterval = value;
        }
    }

    public static class StorageProperties {
        private int completionGuardStripes = 256;
        private int emptyQueueReclaimBatchSize = 32;
        private int backgroundReclaimBatchSize = 8;
        private Duration reclaimCooldown = Duration.ofSeconds(30);
        private boolean backgroundReclaimEnabled = true;

        public int getCompletionGuardStripes() {
            return completionGuardStripes;
        }

        public void setCompletionGuardStripes(int value) {
            completionGuardStripes = value;
        }

        public int getEmptyQueueReclaimBatchSize() {
            return emptyQueueReclaimBatchSize;
        }

        public void setEmptyQueueReclaimBatchSize(int value) {
            emptyQueueReclaimBatchSize = value;
        }

        public int getBackgroundReclaimBatchSize() {
            return backgroundReclaimBatchSize;
        }

        public void setBackgroundReclaimBatchSize(int value) {
            backgroundReclaimBatchSize = value;
        }

        public Duration getReclaimCooldown() {
            return reclaimCooldown;
        }

        public void setReclaimCooldown(Duration value) {
            reclaimCooldown = value;
        }

        public boolean isBackgroundReclaimEnabled() {
            return backgroundReclaimEnabled;
        }

        public void setBackgroundReclaimEnabled(boolean value) {
            backgroundReclaimEnabled = value;
        }
    }

    public static class SqliteProperties {
        private SqliteJournalMode journalMode = SqliteJournalMode.WAL;
        private SqliteSynchronousMode synchronousMode = SqliteSynchronousMode.NORMAL;
        private int cacheSizeKb = -4_000;
        private Duration busyTimeout = Duration.ofSeconds(5);
        private boolean checkpointAfterBatch;

        public SqliteJournalMode getJournalMode() {
            return journalMode;
        }

        public void setJournalMode(SqliteJournalMode value) {
            journalMode = value;
        }

        public SqliteSynchronousMode getSynchronousMode() {
            return synchronousMode;
        }

        public void setSynchronousMode(SqliteSynchronousMode value) {
            synchronousMode = value;
        }

        public int getCacheSizeKb() {
            return cacheSizeKb;
        }

        public void setCacheSizeKb(int value) {
            cacheSizeKb = value;
        }

        public Duration getBusyTimeout() {
            return busyTimeout;
        }

        public void setBusyTimeout(Duration value) {
            busyTimeout = value;
        }

        public boolean isCheckpointAfterBatch() {
            return checkpointAfterBatch;
        }

        public void setCheckpointAfterBatch(boolean value) {
            checkpointAfterBatch = value;
        }
    }

    public static class RetryProperties {
        private int maxRetryCount = 3;
        private Duration initialDelay = Duration.ofSeconds(5);
        private boolean exponentialBackoff = true;
        private Duration maxDelay = Duration.ofMinutes(5);

        public int getMaxRetryCount() {
            return maxRetryCount;
        }

        public void setMaxRetryCount(int value) {
            maxRetryCount = value;
        }

        public Duration getInitialDelay() {
            return initialDelay;
        }

        public void setInitialDelay(Duration value) {
            initialDelay = value;
        }

        public boolean isExponentialBackoff() {
            return exponentialBackoff;
        }

        public void setExponentialBackoff(boolean value) {
            exponentialBackoff = value;
        }

        public Duration getMaxDelay() {
            return maxDelay;
        }

        public void setMaxDelay(Duration value) {
            maxDelay = value;
        }
    }

    public static class JournalProperties {
        private boolean enabled = true;
        private boolean projectionEnabled = true;
        private JournalFormat format = JournalFormat.BINARY_V1;
        private JournalAckMode ackMode = JournalAckMode.DURABLE;
        private Duration stateFlushDebounce = Duration.ofSeconds(1);
        private Duration linger = Duration.ofMillis(1);
        private int maxBatchRecords = 16;
        private int maxBatchBytes = 262_144;
        private Duration writerIdleTimeout = Duration.ofSeconds(30);
        private int asyncQueueCapacityPerTenant = 8_192;
        private Duration balancedFlushWindow = Duration.ofMillis(5);

        public boolean isEnabled() {
            return enabled;
        }

        public void setEnabled(boolean value) {
            enabled = value;
        }

        public boolean isProjectionEnabled() {
            return projectionEnabled;
        }

        public void setProjectionEnabled(boolean value) {
            projectionEnabled = value;
        }

        public JournalFormat getFormat() {
            return format;
        }

        public void setFormat(JournalFormat value) {
            format = value;
        }

        public JournalAckMode getAckMode() {
            return ackMode;
        }

        public void setAckMode(JournalAckMode value) {
            ackMode = value;
        }

        public Duration getStateFlushDebounce() {
            return stateFlushDebounce;
        }

        public void setStateFlushDebounce(Duration value) {
            stateFlushDebounce = value;
        }

        public Duration getLinger() {
            return linger;
        }

        public void setLinger(Duration value) {
            linger = value;
        }

        public int getMaxBatchRecords() {
            return maxBatchRecords;
        }

        public void setMaxBatchRecords(int value) {
            maxBatchRecords = value;
        }

        public int getMaxBatchBytes() {
            return maxBatchBytes;
        }

        public void setMaxBatchBytes(int value) {
            maxBatchBytes = value;
        }

        public Duration getWriterIdleTimeout() {
            return writerIdleTimeout;
        }

        public void setWriterIdleTimeout(Duration value) {
            writerIdleTimeout = value;
        }

        public int getAsyncQueueCapacityPerTenant() {
            return asyncQueueCapacityPerTenant;
        }

        public void setAsyncQueueCapacityPerTenant(int value) {
            asyncQueueCapacityPerTenant = value;
        }

        public Duration getBalancedFlushWindow() {
            return balancedFlushWindow;
        }

        public void setBalancedFlushWindow(Duration value) {
            balancedFlushWindow = value;
        }
    }

    public static class ProjectionProperties {
        private int maxRecordsPerTenantCycle = 64;
        private int maxTenantsPerCycle = 8;
        private Duration busyCycleDelay = Duration.ofMillis(500);
        private Duration idleCycleDelay = Duration.ofSeconds(5);
        private Duration cycleTimeBudget = Duration.ofSeconds(2);

        public int getMaxRecordsPerTenantCycle() {
            return maxRecordsPerTenantCycle;
        }

        public void setMaxRecordsPerTenantCycle(int value) {
            maxRecordsPerTenantCycle = value;
        }

        public int getMaxTenantsPerCycle() {
            return maxTenantsPerCycle;
        }

        public void setMaxTenantsPerCycle(int value) {
            maxTenantsPerCycle = value;
        }

        public Duration getBusyCycleDelay() {
            return busyCycleDelay;
        }

        public void setBusyCycleDelay(Duration value) {
            busyCycleDelay = value;
        }

        public Duration getIdleCycleDelay() {
            return idleCycleDelay;
        }

        public void setIdleCycleDelay(Duration value) {
            idleCycleDelay = value;
        }

        public Duration getCycleTimeBudget() {
            return cycleTimeBudget;
        }

        public void setCycleTimeBudget(Duration value) {
            cycleTimeBudget = value;
        }
    }

    public static class SnapshotProperties {
        private boolean enabled = true;
        private Duration interval = Duration.ofMinutes(15);
        private long minimumProgressBytes = 1_048_576;

        public boolean isEnabled() {
            return enabled;
        }

        public void setEnabled(boolean value) {
            enabled = value;
        }

        public Duration getInterval() {
            return interval;
        }

        public void setInterval(Duration value) {
            interval = value;
        }

        public long getMinimumProgressBytes() {
            return minimumProgressBytes;
        }

        public void setMinimumProgressBytes(long value) {
            minimumProgressBytes = value;
        }
    }

    public static class CompactionProperties {
        private boolean enabled = true;
        private long minimumProcessedBytes = 4_194_304;

        public boolean isEnabled() {
            return enabled;
        }

        public void setEnabled(boolean value) {
            enabled = value;
        }

        public long getMinimumProcessedBytes() {
            return minimumProcessedBytes;
        }

        public void setMinimumProcessedBytes(long value) {
            minimumProcessedBytes = value;
        }
    }

    public static class CleanupProperties {
        private boolean enabled = true;
        private Duration interval = Duration.ofHours(1);
        private Duration initialDelay = Duration.ofMinutes(1);
        private Duration processingTimeout = Duration.ofMinutes(30);
        private Duration completedRetention = Duration.ZERO;
        private Duration failedRetention = Duration.ofDays(3);
        private PermanentlyFailedDisposition permanentlyFailedDisposition =
                PermanentlyFailedDisposition.MOVE_TO_DEAD_LETTER;
        private int batchSizePerTenant = 500;

        public boolean isEnabled() {
            return enabled;
        }

        public void setEnabled(boolean value) {
            enabled = value;
        }

        public Duration getInterval() {
            return interval;
        }

        public void setInterval(Duration value) {
            interval = value;
        }

        public Duration getInitialDelay() {
            return initialDelay;
        }

        public void setInitialDelay(Duration value) {
            initialDelay = value;
        }

        public Duration getProcessingTimeout() {
            return processingTimeout;
        }

        public void setProcessingTimeout(Duration value) {
            processingTimeout = value;
        }

        public Duration getCompletedRetention() {
            return completedRetention;
        }

        public void setCompletedRetention(Duration value) {
            completedRetention = value;
        }

        public Duration getFailedRetention() {
            return failedRetention;
        }

        public void setFailedRetention(Duration value) {
            failedRetention = value;
        }

        public PermanentlyFailedDisposition getPermanentlyFailedDisposition() {
            return permanentlyFailedDisposition;
        }

        public void setPermanentlyFailedDisposition(PermanentlyFailedDisposition value) {
            permanentlyFailedDisposition = value;
        }

        public int getBatchSizePerTenant() {
            return batchSizePerTenant;
        }

        public void setBatchSizePerTenant(int value) {
            batchSizePerTenant = value;
        }
    }

    public static class OrphanRecoveryProperties {
        private boolean enabled;
        private boolean runOnStartup;
        private Duration interval = Duration.ofHours(6);

        public boolean isEnabled() {
            return enabled;
        }

        public void setEnabled(boolean value) {
            enabled = value;
        }

        public boolean isRunOnStartup() {
            return runOnStartup;
        }

        public void setRunOnStartup(boolean value) {
            runOnStartup = value;
        }

        public Duration getInterval() {
            return interval;
        }

        public void setInterval(Duration value) {
            interval = value;
        }
    }

    public static class StatisticsProperties {
        private boolean enabled;
        private Duration windowSize = Duration.ofMinutes(5);
        private Duration retention = Duration.ofHours(1);
        private int maxSeries = 16_384;

        public boolean isEnabled() {
            return enabled;
        }

        public void setEnabled(boolean value) {
            enabled = value;
        }

        public Duration getWindowSize() {
            return windowSize;
        }

        public void setWindowSize(Duration value) {
            windowSize = value;
        }

        public Duration getRetention() {
            return retention;
        }

        public void setRetention(Duration value) {
            retention = value;
        }

        public int getMaxSeries() {
            return maxSeries;
        }

        public void setMaxSeries(int value) {
            maxSeries = value;
        }
    }

    public static class ActuatorProperties {
        private boolean enabled = true;

        public boolean isEnabled() {
            return enabled;
        }

        public void setEnabled(boolean value) {
            enabled = value;
        }
    }

    public static class MetricsProperties {
        private boolean enabled = true;

        public boolean isEnabled() {
            return enabled;
        }

        public void setEnabled(boolean value) {
            enabled = value;
        }
    }

    public static class VolumeProperties {
        private String id;
        private Path mountPath;
        private int shardingDepth;
        private int bufferSize = 65_536;
        private boolean forceFlushAfterWrite;

        VolumeConfiguration toConfiguration() {
            return new VolumeConfiguration(id, mountPath, shardingDepth, bufferSize, forceFlushAfterWrite);
        }

        public String getId() {
            return id;
        }

        public void setId(String value) {
            id = value;
        }

        public Path getMountPath() {
            return mountPath;
        }

        public void setMountPath(Path value) {
            mountPath = value;
        }

        public int getShardingDepth() {
            return shardingDepth;
        }

        public void setShardingDepth(int value) {
            shardingDepth = value;
        }

        public int getBufferSize() {
            return bufferSize;
        }

        public void setBufferSize(int value) {
            bufferSize = value;
        }

        public boolean isForceFlushAfterWrite() {
            return forceFlushAfterWrite;
        }

        public void setForceFlushAfterWrite(boolean value) {
            forceFlushAfterWrite = value;
        }
    }

    public static class WatcherProperties {
        private String watcherId;
        private String tenantId;
        private WatcherTenantMode tenantMode = WatcherTenantMode.SINGLE_TENANT;
        private boolean autoCreateTenantDirectories;
        private Path watchPath;
        private boolean enabled = true;
        private boolean recursive;
        private List<String> globs = new ArrayList<>(List.of("*"));
        private PostImportAction postImportAction = PostImportAction.KEEP;
        private Path moveDirectory;
        private Duration pollInterval = Duration.ofSeconds(5);
        private long maxFileSize;
        private Duration minimumFileAge = Duration.ZERO;
        private Duration stabilityCheckInterval = Duration.ofMillis(100);
        private int stabilityCheckCount = 2;
        private int concurrentImports = 1;
        private Duration historyRetention = Duration.ofDays(7);
        private Duration historyFlushInterval = Duration.ofSeconds(1);

        WatcherConfiguration toConfiguration() {
            return new WatcherConfiguration(
                    watcherId,
                    tenantId,
                    tenantMode,
                    autoCreateTenantDirectories,
                    watchPath,
                    enabled,
                    recursive,
                    globs,
                    postImportAction,
                    moveDirectory,
                    pollInterval,
                    maxFileSize,
                    minimumFileAge,
                    stabilityCheckInterval,
                    stabilityCheckCount,
                    concurrentImports,
                    historyRetention,
                    historyFlushInterval);
        }

        public String getWatcherId() {
            return watcherId;
        }

        public void setWatcherId(String value) {
            watcherId = value;
        }

        public String getTenantId() {
            return tenantId;
        }

        public void setTenantId(String value) {
            tenantId = value;
        }

        public WatcherTenantMode getTenantMode() {
            return tenantMode;
        }

        public void setTenantMode(WatcherTenantMode value) {
            tenantMode = value;
        }

        public boolean isAutoCreateTenantDirectories() {
            return autoCreateTenantDirectories;
        }

        public void setAutoCreateTenantDirectories(boolean value) {
            autoCreateTenantDirectories = value;
        }

        public Path getWatchPath() {
            return watchPath;
        }

        public void setWatchPath(Path value) {
            watchPath = value;
        }

        public boolean isEnabled() {
            return enabled;
        }

        public void setEnabled(boolean value) {
            enabled = value;
        }

        public boolean isRecursive() {
            return recursive;
        }

        public void setRecursive(boolean value) {
            recursive = value;
        }

        public List<String> getGlobs() {
            return globs;
        }

        public void setGlobs(List<String> value) {
            globs = value == null ? new ArrayList<>() : new ArrayList<>(value);
        }

        public PostImportAction getPostImportAction() {
            return postImportAction;
        }

        public void setPostImportAction(PostImportAction value) {
            postImportAction = value;
        }

        public Path getMoveDirectory() {
            return moveDirectory;
        }

        public void setMoveDirectory(Path value) {
            moveDirectory = value;
        }

        public Duration getPollInterval() {
            return pollInterval;
        }

        public void setPollInterval(Duration value) {
            pollInterval = value;
        }

        public long getMaxFileSize() {
            return maxFileSize;
        }

        public void setMaxFileSize(long value) {
            maxFileSize = value;
        }

        public Duration getMinimumFileAge() {
            return minimumFileAge;
        }

        public void setMinimumFileAge(Duration value) {
            minimumFileAge = value;
        }

        public Duration getStabilityCheckInterval() {
            return stabilityCheckInterval;
        }

        public void setStabilityCheckInterval(Duration value) {
            stabilityCheckInterval = value;
        }

        public int getStabilityCheckCount() {
            return stabilityCheckCount;
        }

        public void setStabilityCheckCount(int value) {
            stabilityCheckCount = value;
        }

        public int getConcurrentImports() {
            return concurrentImports;
        }

        public void setConcurrentImports(int value) {
            concurrentImports = value;
        }

        public Duration getHistoryRetention() {
            return historyRetention;
        }

        public void setHistoryRetention(Duration value) {
            historyRetention = value;
        }

        public Duration getHistoryFlushInterval() {
            return historyFlushInterval;
        }

        public void setHistoryFlushInterval(Duration value) {
            historyFlushInterval = value;
        }
    }
}
