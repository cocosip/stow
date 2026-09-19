# Stow 1.0 Public API And Configuration Contract

## 1. Compatibility Scope

- Maven group: `io.github.cocosip`
- Java root package: `io.github.cocosip.stow`
- Java baseline: OpenJDK 21 with `--release 21`
- Core artifact: `stow-core`
- Framework adapter: `stow-spring-boot-starter`
- API model: synchronous, blocking, thread-safe, and virtual-thread friendly
- Time types: `java.time.Instant` and `java.time.Duration`
- Optional queries: `Optional<T>`; collections never return `null`
- Logging: `slf4j-api:2.0.17` only; the host application selects the provider

Types under `io.github.cocosip.stow.internal` are not compatibility promises.
Public APIs may expose only root-package, `api`, `model`, `config`,
`exception`, and explicitly public `spi` types.

## 2. Runtime Entry Points

```java
package io.github.cocosip.stow;

public final class Stow {
    public static StowBuilder builder();
    public static StowRuntime open(StowConfiguration configuration);
}

public interface StowRuntime extends AutoCloseable {
    RuntimeState state();
    void start();
    StoragePool storagePool();
    TenantManager tenantManager();
    TenantQuotaManager tenantQuotaManager();
    DirectoryQuotaManager directoryQuotaManager();
    StorageMaintenance maintenance();
    QueueProjectionMaintenance projectionMaintenance();
    FileWatcherManager fileWatcherManager();
    FileWatcherOptionsManager fileWatcherOptionsManager();
    FileWatcherAutoManager fileWatcherAutoManager();
    StatisticsReader statisticsReader();
    RuntimeHealth health();
    @Override void close();
}

public enum RuntimeState {
    NEW, STARTING, RUNNING, STOPPING, TERMINATED, FAILED
}
```

`Stow.builder().build()` returns `NEW` without creating background threads.
`start()` performs recovery and enters `RUNNING`; `Stow.open` is build plus
start. `start()` is valid only from `NEW`; a repeated call throws
`IllegalStateException`. `close()` is idempotent in every state.

```java
public final class StowBuilder {
    public StowBuilder configuration(StowConfiguration configuration);
    public StowBuilder clock(Clock clock);
    public StowBuilder workerExecutor(ExecutorService executor);
    public StowBuilder scheduler(ScheduledExecutorService scheduler);
    public StowBuilder storageVolumeProvider(StorageVolumeProvider provider);
    public StowBuilder journalCodec(JournalCodec codec);
    public StowRuntime build();
}
```

The builder accepts only complete configuration, `Clock`, executors, scheduler,
and public SPI implementations. Runtime-owned resources are closed by the
runtime; caller-owned resources remain the caller's responsibility.

## 3. StoragePool

```java
package io.github.cocosip.stow.api;

public interface StoragePool {
    String write(TenantContext tenant, InputStream content, WriteOptions options);
    String write(TenantContext tenant, ContentSource content, WriteOptions options);
    InputStream read(TenantContext tenant, String fileKey);
    Optional<StoredFileInfo> findFileInfo(TenantContext tenant, String fileKey);
    Optional<FileLocation> findFileLocation(TenantContext tenant, String fileKey);
    Optional<ClaimedFile> claimNext(TenantContext tenant);
    List<ClaimedFile> claimBatch(TenantContext tenant, int batchSize);
    void complete(ProcessingLease lease);
    void fail(ProcessingLease lease, String errorMessage);
    FileProcessingStatus status(TenantContext tenant, String fileKey);
    long totalCapacity();
    long availableCapacity();
}

public interface ContentSource {
    InputStream openStream();
    OptionalLong length();
    boolean repeatable();
}

public final class ContentSources {
    public static ContentSource of(Path path);
    public static ContentSource of(byte[] bytes);
    public static ContentSource singleUse(InputStream input, OptionalLong length);
}
```

The `InputStream` overload makes one volume attempt and does not close the
caller's stream. A repeatable `ContentSource` may be opened again for a volume
retry and each opened stream is closed by Stow. A non-repeatable source makes
one attempt. Built-in factories cover paths, byte arrays, and single-use
streams.

```java
public record WriteOptions(String originalFileName, String logicalDirectory) {
    public static WriteOptions defaults();
    public static WriteOptions ofOriginalFileName(String originalFileName);
}

public record ProcessingLease(
        String tenantId,
        String fileKey,
        UUID leaseId,
        Instant startedAt) {}

public record ClaimedFile(FileLocation location, ProcessingLease lease) {}

public record StoredFileInfo(
        String fileKey,
        String tenantId,
        long fileSize,
        Instant createdAt,
        FileProcessingStatus status,
        int retryCount,
        String originalFileName,
        String fileExtension) {}

public record FileLocation(
        String fileKey,
        String tenantId,
        String volumeId,
        Path physicalPath,
        String logicalDirectory,
        long fileSize,
        Instant createdAt,
        FileProcessingStatus status,
        int retryCount,
        Instant lastFailedAt,
        String lastError,
        Instant availableAt) {}
```

The two `WriteOptions` fields may be `null`; the core normalizes them to no
original filename and `/`. Other records validate required fields in compact
constructors. Error messages are limited to 4096 characters, original names to
255 characters, and extensions to 32 characters.

`read` returns a caller-owned stream. Cross-tenant queries return empty and
cross-tenant reads throw `StoredFileNotFoundException` without revealing file
existence.

## 4. States And Events

```java
public enum FileProcessingStatus {
    PENDING, PROCESSING, COMPLETED, FAILED,
    PERMANENTLY_FAILED, DELETE_REQUESTED, DELETE_SUCCEEDED, DEAD_LETTERED
}

public enum QueueEventType {
    ACCEPTED, PROCESSING_STARTED, PROCESSING_FAILED, PROCESSING_COMPLETED,
    DELETE_REQUESTED, DELETE_SUCCEEDED, PROCESSING_TIMED_OUT, DEAD_LETTERED
}
```

`QueueEventRecord` is immutable and contains schema version, event ID, tenant,
file key, event type, UTC time, per-tenant sequence, volume, physical path,
logical directory, size, status, lease, processing time, retry count,
availability time, error message, original filename, and extension. CRC is a
journal-frame field, not a business-record field.

## 5. Tenant And Quota APIs

```java
public interface TenantManager {
    TenantContext get(String tenantId);
    Optional<TenantContext> find(String tenantId);
    List<TenantContext> list();
    TenantContext create(String tenantId);
    void enable(String tenantId);
    void disable(String tenantId);
}

public record TenantContext(String tenantId, TenantStatus status,
                            Instant createdAt, Instant updatedAt) {}
public enum TenantStatus { ENABLED, DISABLED }

public interface TenantQuotaManager {
    long currentCount(String tenantId);
    long limit(String tenantId);
    void setLimit(String tenantId, long maxFiles);
}

public interface DirectoryQuotaManager {
    DirectoryQuota get(String tenantId, String logicalDirectory);
    void setLimit(String tenantId, String logicalDirectory, long maxFiles);
}

public record DirectoryQuota(
        String tenantId, String logicalDirectory, long currentCount,
        long maxFiles, boolean enabled) {}
```

Quota value `0` means unlimited. Counts use `long`. The configured default
tenant quota is copied only when a tenant is created; changing the default does
not rewrite existing tenants.

## 6. Maintenance And Projection APIs

```java
public interface StorageMaintenance {
    CleanupStatistics cleanupCompleted(Duration olderThan);
    CleanupStatistics cleanupPermanentlyFailed(Duration olderThan);
    CleanupStatistics reclaimTimedOutProcessing(Duration timeout);
    CleanupStatistics recoverOrphans(String tenantId);
    CleanupStatistics recoverAllOrphans();
    void reconcileQuota(String tenantId);
    void reconcileAllQuotas();
    DatabaseHealthReport checkDatabases();
    DatabaseRebuildResult rebuildMetadata(String tenantId);
    DatabaseRebuildResult rebuildQuota(String tenantId);
    DatabaseOptimizationResult optimizeDatabases();
    CleanupStatistics cleanupInvalidDatabaseBackups();
    CleanupStatistics cleanupEmptyDirectories();
    CleanupStatistics cleanupJunkFiles();
}

public interface QueueProjectionMaintenance {
    ProjectionTenantState state(String tenantId);
    ProjectionTenantState replay(String tenantId);
    ProjectionTenantState snapshot(String tenantId);
    ProjectionTenantState rebuild(String tenantId);
}
```

Maintenance result records include start/end time, scanned, succeeded,
skipped, failed, released bytes, affected tenants, and immutable error
summaries. Fact corruption or an operation that cannot continue still throws.

## 7. Watcher API

```java
public interface FileWatcherManager {
    WatcherConfiguration register(WatcherConfiguration configuration);
    WatcherConfiguration update(WatcherConfiguration configuration);
    void remove(String watcherId);
    void enable(String watcherId);
    void disable(String watcherId);
    Optional<WatcherConfiguration> find(String watcherId);
    List<WatcherConfiguration> list();
    List<WatcherConfiguration> listForTenant(String tenantId);
    WatcherScanResult scanNow(String watcherId);
}

public interface FileWatcherOptionsManager {
    WatcherOptions get();
    void update(WatcherOptions options);
    void enable();
    void disable();
}

public interface FileWatcherAutoManager {
    int apply(WatcherRootConfiguration configuration);
    int discoverAndCreate();
    void removeManagedWatchers();
    Optional<WatcherRootConfiguration> currentRoot();
}

public enum PostImportAction { DELETE, MOVE, KEEP }
```

`WatcherConfiguration` contains watcher and tenant IDs, single- or
multi-tenant mode, automatic tenant-directory creation, path, enabled and
recursive flags, globs, post-import action and move directory, polling and
stability settings, size/age limits, import concurrency, and history retention
and flush settings. Collections are defensively copied.

## 8. Statistics And Health APIs

```java
public interface StatisticsReader {
    StatisticsSnapshot snapshot(StatisticsQuery query);
}

public record StatisticsQuery(
        Instant from, Instant to, String tenantId, String volumeId,
        String watcherId, String operation) {}

public enum HealthStatus { UP, DEGRADED, DOWN }
public record ComponentHealth(HealthStatus status, String summary,
                              Instant checkedAt) {}
public record RuntimeHealth(HealthStatus status,
                            Map<String, ComponentHealth> components) {}
```

Snapshots include at least written files/bytes and MiB/s, reads, claims,
completions, SQLite persistence operations, watcher imports, and import bytes.
Returned maps and lists are immutable snapshots.

## 9. Public SPI

Only replacement boundaries with practical value are public in 1.0:

```java
public interface StorageVolume extends AutoCloseable {
    String id();
    Path mountPath();
    boolean healthy();
    long totalCapacity();
    long availableCapacity();
    Path buildPath(String tenantId, String fileKey, String extension);
    long write(Path target, InputStream content);
    InputStream read(Path path);
    void delete(Path path);
    void move(Path source, Path target);
}

public interface StorageVolumeProvider {
    StorageVolume create(VolumeConfiguration configuration);
}

public interface JournalCodec {
    JournalFormat format();
    byte[] encode(QueueEventRecord event);
    QueueEventRecord decode(byte[] payload);
}
```

`MetadataProjectionStore` and `QueueEventJournal` are advanced SPIs marked
with Stow's `@ExperimentalApi`. They may change during a deprecation period in
the 1.x line; ordinary callers do not need to implement them.

## 10. Configuration And Defaults

`StowConfiguration` is an aggregate record created by
`StowConfiguration.builder()`. Every path becomes an absolute normalized path
during `build()`.

| Group | Property | Default |
| --- | --- | --- |
| paths | `metadataDirectory` | `./stow-metadata` |
| paths | `quotaDirectory` | `./stow-quota` |
| paths | `queueDirectory` | `./stow-queue` |
| paths | `watcherDirectory` | `./stow-watchers` |
| tenant | `autoCreateTenants` | `false` |
| tenant | `defaultQuota` | `0` (unlimited) |
| metadata | `backgroundPersistence` | `true` |
| metadata | `maxQueueSize` | `100000` |
| metadata | `drainBatchSize` | `2000` |
| metadata | `softMergeThresholdPercent` | `90` |
| metadata | `startupLoadBatchSize` | `2000` |
| metadata | `shutdownDrainTimeout` | `30s` |
| metadata | `persistenceInterval` | `2s` |
| storage | `completionGuardStripes` | `256` |
| storage | `emptyQueueReclaimBatchSize` | `32` |
| storage | `backgroundReclaimBatchSize` | `8` |
| storage | `reclaimCooldown` | `30s` |
| storage | `backgroundReclaimEnabled` | `true` |
| sqlite | `journalMode` | `WAL` |
| sqlite | `synchronousMode` | `NORMAL` |
| sqlite | `cacheSizeKb` | `-4000` |
| sqlite | `busyTimeout` | `5s` |
| sqlite | `checkpointAfterBatch` | `false` |
| retry | `maxRetryCount` | `3` |
| retry | `initialDelay` | `5s` |
| retry | `exponentialBackoff` | `true` |
| retry | `maxDelay` | `5m` |
| journal | `enabled` | `true` (cannot be disabled) |
| journal | `projectionEnabled` | `true` |
| journal | `format` | `BINARY_V1` |
| journal | `ackMode` | `DURABLE` |
| journal | `stateFlushDebounce` | `1s` |
| journal | `linger` | `1ms` |
| journal | `maxBatchRecords` | `16` |
| journal | `maxBatchBytes` | `262144` |
| journal | `writerIdleTimeout` | `30s` |
| journal | `asyncQueueCapacityPerTenant` | `8192` |
| journal | `balancedFlushWindow` | `5ms` |
| projection | `maxRecordsPerTenantCycle` | `64` |
| projection | `maxTenantsPerCycle` | `8` |
| projection | `busyCycleDelay` | `500ms` |
| projection | `idleCycleDelay` | `5s` |
| projection | `cycleTimeBudget` | `2s` |
| snapshot | `enabled` | `true` |
| snapshot | `interval` | `15m` |
| snapshot | `minimumProgressBytes` | `1048576` |
| compaction | `enabled` | `true` |
| compaction | `minimumProcessedBytes` | `4194304` |
| cleanup | `enabled` | `true` |
| cleanup | `interval` | `1h` |
| cleanup | `initialDelay` | `1m` |
| cleanup | `processingTimeout` | `30m` |
| cleanup | `completedRetention` | `0s` |
| cleanup | `failedRetention` | `3d` |
| cleanup | `permanentlyFailedDisposition` | `MOVE_TO_DEAD_LETTER` |
| cleanup | `batchSizePerTenant` | `500` |
| orphanRecovery | `enabled` | `false` |
| orphanRecovery | `runOnStartup` | `false` |
| orphanRecovery | `interval` | `6h` |
| statistics | `enabled` | `false` |
| statistics | `windowSize` | `5m` |
| statistics | `retention` | `1h` |
| statistics | `maxSeries` | `16384` |

Volumes require unique IDs, safe mount paths, sharding depth `0..3`, a
positive buffer size, and default `forceFlushAfterWrite=false`. Watchers are
empty by default; required fields are `watcher-id`, `tenant-id`, and
`watch-path`. Their defaults are enabled, non-recursive, `[*]`, `KEEP`, a 5s
poll interval, zero minimum age, a 100ms stability interval, two stability
checks, one concurrent import, 7d history retention, and a 1s history flush.

All capacities, counts, durations, and percentages are validated during
`build()`. The journal cannot be disabled without an explicit test-only switch;
production configuration has no legacy non-journal mode.

## 11. Spring Boot Mapping And Dependency Injection

The starter uses the `stow.*` prefix and maps kebab-case properties to the
same core configuration:

```yaml
stow:
  paths:
    metadata-directory: ./stow-metadata
    queue-directory: ./stow-queue
  journal:
    ack-mode: durable
  volumes:
    - id: volume-1
      mount-path: ./storage/volume-1
      sharding-depth: 2
      force-flush-after-write: true
```

The starter creates one `StowRuntime`, manages its `SmartLifecycle`, and
exposes `StoragePool`, `TenantManager`, quota managers, maintenance services,
watcher services, and `StatisticsReader` as beans. Applications may use
constructor injection:

```java
@Service
public final class DocumentService {
    private final StoragePool storagePool;
    private final TenantManager tenantManager;

    public DocumentService(StoragePool storagePool, TenantManager tenantManager) {
        this.storagePool = storagePool;
        this.tenantManager = tenantManager;
    }
}
```

The starter initializes the runtime before dependent beans are created and
owns startup and shutdown. Application code must not call `start()` or
`close()` on an injected runtime. Do not call `Stow.open(...)` or create a
second runtime from a Spring-managed service; inject the single Spring-managed
`StowRuntime` or one of its public service beans. The core remains independent
of Spring and does not expose internal implementation beans.

## 12. Logging Contract

`stow-core` depends only on `org.slf4j:slf4j-api:2.0.17`. It does not include
Logback, Log4j2, a JUL bridge, or any binding/provider. The host application
must select exactly one SLF4J 2 provider. With only the API present, SLF4J's
standard NOP-provider warning is expected and Stow remains functional.

For example, an application may choose Logback:

```xml
<dependency>
  <groupId>org.slf4j</groupId>
  <artifactId>slf4j-api</artifactId>
  <version>2.0.17</version>
</dependency>
<dependency>
  <groupId>ch.qos.logback</groupId>
  <artifactId>logback-classic</artifactId>
  <version>1.5.18</version>
</dependency>
```

The `slf4j1-compat` profile validates a legacy host with API `1.7.36` and a
test-only simple binding; it is never packaged into Stow. All logs use
parameterized placeholders and avoid file content, full physical paths, and
original filenames by default. `fileKey`, tenant, and path values are limited
to necessary diagnostic events and remain controlled by host log levels.

## 13. Exception Contract

All domain exceptions extend unchecked `StowException`. Fixed subclasses are
`InvalidConfigurationException`, `RuntimeNotReadyException`,
`RuntimeDirectoryLockedException`, `TenantNotFoundException`,
`TenantDisabledException`, `TenantQuotaExceededException`,
`DirectoryQuotaExceededException`, `InsufficientStorageException`,
`StorageVolumeUnavailableException`, `StoredFileNotFoundException`,
`FileAlreadyProcessingException`, `LeaseMismatchException`,
`PhysicalFileMissingException`, `JournalCorruptionException`,
`ProjectionException`, `DatabaseRecoveryException`, and
`StowInterruptedException`.

Exceptions carry stable error codes; messages are diagnostic text, not a machine
protocol. Interrupted operations restore the thread interrupt flag. Public
APIs do not swallow failures or use `null` to represent failure.
