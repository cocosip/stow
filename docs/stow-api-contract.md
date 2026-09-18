# Stow 1.0 公共 API 与配置契约

## 1. 兼容性范围

- Maven groupId：`io.github.cocosip`
- Java 根包：`io.github.cocosip.stow`
- Java：OpenJDK 21，`--release 21`
- 核心制品：`stow-core`
- 框架适配：`stow-spring-boot-starter`
- 核心 API：同步阻塞、线程安全、虚拟线程友好
- 时间：`java.time.Instant` 与 `java.time.Duration`
- 可空查询结果：`Optional<T>`；集合永不返回 `null`
- 日志：仅依赖 `slf4j-api:1.7.36`，不携带 Provider

`io.github.cocosip.stow.internal` 下的类型不属于兼容承诺。公开 API 只能暴露根包、`api`、`model`、`config`、`exception` 和明确公开的 `spi` 类型。

## 2. 运行时入口

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

`Stow.builder().build()` 返回 `NEW` 状态且不创建线程；`start()` 完成恢复后进入 `RUNNING`。`Stow.open` 等价于 build + start。`start()` 只允许从 `NEW` 调用；重复调用抛出 `IllegalStateException`。`close()` 在任何状态下幂等。

`StowBuilder` 只允许覆盖完整配置、`Clock`、工作执行器、调度器和公开 SPI。未提供的资源由 runtime 创建并拥有；调用方提供的资源由调用方关闭。

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

`InputStream` 重载只尝试一个卷且不关闭调用方流。`ContentSource` 重载在 `repeatable=true` 时可以为每次卷重试重新调用 `openStream()`，并负责关闭每次打开的流；`repeatable=false` 时同样只尝试一个卷。内置工厂提供 path、byte array 和单次 InputStream 三种来源。

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

`WriteOptions` 的两个字段允许为 `null`，进入核心后分别规范为无原始文件名和根逻辑目录 `/`。其他 record 的必填字段在紧凑构造器中校验。错误信息最大 4096 个字符，原始文件名最大 255 个字符，扩展名最大 32 个字符。

`read` 返回的 `InputStream` 由调用方关闭。跨租户查询返回空，跨租户读取抛出 `StoredFileNotFoundException`，不泄露文件存在性。

## 4. 状态与事件

```java
public enum FileProcessingStatus {
    PENDING,
    PROCESSING,
    COMPLETED,
    FAILED,
    PERMANENTLY_FAILED,
    DELETE_REQUESTED,
    DELETE_SUCCEEDED,
    DEAD_LETTERED
}

public enum QueueEventType {
    ACCEPTED,
    PROCESSING_STARTED,
    PROCESSING_FAILED,
    PROCESSING_COMPLETED,
    DELETE_REQUESTED,
    DELETE_SUCCEEDED,
    PROCESSING_TIMED_OUT,
    DEAD_LETTERED
}
```

`QueueEventRecord` 是不可变 record，包含 `schemaVersion`、`eventId`、`tenantId`、`fileKey`、`eventType`、`occurredAt`、`sequenceNumber`、`volumeId`、`physicalPath`、`logicalDirectory`、`fileSize`、`status`、`leaseId`、`processingStartedAt`、`retryCount`、`availableAt`、`errorMessage`、`originalFileName` 和 `fileExtension`。CRC 位于 journal frame，不放进业务 record。

## 5. 租户与配额 API

```java
public interface TenantManager {
    TenantContext get(String tenantId);
    Optional<TenantContext> find(String tenantId);
    List<TenantContext> list();
    TenantContext create(String tenantId);
    void enable(String tenantId);
    void disable(String tenantId);
}

public record TenantContext(String tenantId, TenantStatus status, Instant createdAt, Instant updatedAt) {}
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
        String tenantId,
        String logicalDirectory,
        long currentCount,
        long maxFiles,
        boolean enabled) {}
```

配额值 0 表示无限制。所有计数使用 `long`，不得因 Java `int` 溢出。默认租户配额只在创建租户时复制，修改配置不改变已存在租户。

## 6. 维护与投影 API

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

维护结果 record 必须包含开始/结束时间、扫描数量、成功数量、跳过数量、失败数量、释放字节数和受影响租户数。单项错误以不可变错误摘要列表返回，同时记录日志；事实损坏或无法继续的错误仍抛异常。

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

`WatcherConfiguration` 包含 watcher ID、tenant ID、多租户模式、自动创建租户目录、watch path、启用状态、递归、glob 列表、后置动作、移动目录、轮询周期、最大文件大小、最小年龄、稳定性检查、并发导入、历史裁剪和历史刷写设置。所有集合创建防御性副本。

## 8. 统计与健康 API

```java
public interface StatisticsReader {
    StatisticsSnapshot snapshot(StatisticsQuery query);
}

public record StatisticsQuery(
        Instant from,
        Instant to,
        String tenantId,
        String volumeId,
        String watcherId,
        String operation) {}

public enum HealthStatus { UP, DEGRADED, DOWN }
public record ComponentHealth(HealthStatus status, String summary, Instant checkedAt) {}
public record RuntimeHealth(HealthStatus status, Map<String, ComponentHealth> components) {}
```

统计快照至少包含写文件数、写字节、MiB/s、读取数、领取数、完成数、SQLite 持久化操作数、watcher 导入数和导入字节。返回 Map/List 均为不可变快照。

## 9. 公开 SPI

1.0 只公开确有替换价值的边界：

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

`MetadataProjectionStore` 与 `QueueEventJournal` 为高级 SPI，标记 Stow 自己的 `@ExperimentalApi` 注解，并在 1.x 内允许经弃用周期调整；默认调用方不需要实现它们。

## 10. 配置契约与默认值

`StowConfiguration` 是聚合 record，由 `StowConfiguration.builder()` 创建。所有路径在 build 时转为绝对规范路径。

| 分组 | 配置 | 默认值 |
| --- | --- | --- |
| paths | metadataDirectory | `./stow-metadata` |
| paths | quotaDirectory | `./stow-quota` |
| paths | queueDirectory | `./stow-queue` |
| paths | watcherDirectory | `./stow-watchers` |
| tenant | autoCreateTenants | `false` |
| tenant | defaultQuota | `0` |
| metadata | backgroundPersistence | `true` |
| metadata | maxQueueSize | `100000` |
| metadata | drainBatchSize | `2000` |
| metadata | softMergeThresholdPercent | `90` |
| metadata | startupLoadBatchSize | `2000` |
| metadata | shutdownDrainTimeout | `30s` |
| metadata | persistenceInterval | `2s` |
| storage | completionGuardStripes | `256` |
| storage | emptyQueueReclaimBatchSize | `32` |
| storage | backgroundReclaimBatchSize | `8` |
| storage | reclaimCooldown | `30s` |
| storage | backgroundReclaimEnabled | `true` |
| sqlite | journalMode | `WAL` |
| sqlite | synchronousMode | `NORMAL` |
| sqlite | cacheSizeKb | `-4000` |
| sqlite | busyTimeout | `5s` |
| sqlite | checkpointAfterBatch | `false` |
| retry | maxRetryCount | `3` |
| retry | initialDelay | `5s` |
| retry | exponentialBackoff | `true` |
| retry | maxDelay | `5m` |
| journal | enabled | `true` |
| journal | projectionEnabled | `true` |
| journal | format | `BINARY_V1` |
| journal | ackMode | `DURABLE` |
| journal | stateFlushDebounce | `1s` |
| journal | linger | `1ms` |
| journal | maxBatchRecords | `16` |
| journal | maxBatchBytes | `262144` |
| journal | writerIdleTimeout | `30s` |
| journal | asyncQueueCapacityPerTenant | `8192` |
| journal | balancedFlushWindow | `5ms` |
| projection | maxRecordsPerTenantCycle | `64` |
| projection | maxTenantsPerCycle | `8` |
| projection | busyCycleDelay | `500ms` |
| projection | idleCycleDelay | `5s` |
| projection | cycleTimeBudget | `2s` |
| snapshot | enabled | `true` |
| snapshot | interval | `15m` |
| snapshot | minimumProgressBytes | `1048576` |
| compaction | enabled | `true` |
| compaction | minimumProcessedBytes | `4194304` |
| cleanup | enabled | `true` |
| cleanup | interval | `1h` |
| cleanup | initialDelay | `1m` |
| cleanup | processingTimeout | `30m` |
| cleanup | completedRetention | `0s` |
| cleanup | failedRetention | `3d` |
| cleanup | permanentlyFailedDisposition | `MOVE_TO_DEAD_LETTER` |
| cleanup | batchSizePerTenant | `500` |
| orphanRecovery | enabled | `false` |
| orphanRecovery | runOnStartup | `false` |
| orphanRecovery | interval | `6h` |
| statistics | enabled | `false` |
| statistics | windowSize | `5m` |
| statistics | retention | `1h` |
| statistics | maxSeries | `16384` |

所有容量、数量、时长和百分比在 build 时验证。journal 不能在没有显式测试开关的情况下关闭；生产配置不公开 legacy non-journal 模式。

## 11. Spring Boot 映射

Starter 使用 `stow.*` 前缀，把 kebab-case 属性转换为上述核心配置，例如：

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

Starter 只公开一个 `StowRuntime` Bean，并从它转发公开服务 Bean。不得让 Spring 容器单独拥有核心内部组件。

## 12. 异常契约

所有领域异常继承非受检 `StowException`。固定子类包括 `InvalidConfigurationException`、`RuntimeNotReadyException`、`RuntimeDirectoryLockedException`、`TenantNotFoundException`、`TenantDisabledException`、`TenantQuotaExceededException`、`DirectoryQuotaExceededException`、`InsufficientStorageException`、`StorageVolumeUnavailableException`、`StoredFileNotFoundException`、`FileAlreadyProcessingException`、`LeaseMismatchException`、`PhysicalFileMissingException`、`JournalCorruptionException`、`ProjectionException`、`DatabaseRecoveryException` 和 `StowInterruptedException`。

异常携带稳定 error code；消息用于诊断但不是机器协议。中断时必须恢复线程中断标志。公开 API 不吞异常，不使用 `null` 表示失败。
