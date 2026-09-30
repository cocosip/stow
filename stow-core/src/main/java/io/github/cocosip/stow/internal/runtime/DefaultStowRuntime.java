package io.github.cocosip.stow.internal.runtime;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import io.github.cocosip.stow.RuntimeState;
import io.github.cocosip.stow.StowRuntime;
import io.github.cocosip.stow.api.DirectoryQuotaManager;
import io.github.cocosip.stow.api.FileWatcherAutoManager;
import io.github.cocosip.stow.api.FileWatcherManager;
import io.github.cocosip.stow.api.FileWatcherOptionsManager;
import io.github.cocosip.stow.api.QueueProjectionMaintenance;
import io.github.cocosip.stow.api.StatisticsReader;
import io.github.cocosip.stow.api.StorageMaintenance;
import io.github.cocosip.stow.api.StoragePool;
import io.github.cocosip.stow.api.TenantManager;
import io.github.cocosip.stow.api.TenantQuotaManager;
import io.github.cocosip.stow.config.StowConfiguration;
import io.github.cocosip.stow.exception.RuntimeNotReadyException;
import io.github.cocosip.stow.exception.StowInterruptedException;
import io.github.cocosip.stow.internal.cleanup.DefaultStorageMaintenance;
import io.github.cocosip.stow.internal.filesystem.DefaultStorageVolumeProvider;
import io.github.cocosip.stow.internal.journal.BinaryV1JournalCodec;
import io.github.cocosip.stow.internal.journal.FileQueueEventJournal;
import io.github.cocosip.stow.internal.journal.JsonLinesJournalCodec;
import io.github.cocosip.stow.internal.journal.SequencedJournalAppender;
import io.github.cocosip.stow.internal.projection.ProjectionCursorStore;
import io.github.cocosip.stow.internal.projection.ProjectionMaintenanceService;
import io.github.cocosip.stow.internal.projection.ProjectionSnapshotStore;
import io.github.cocosip.stow.internal.projection.QueueEventReducer;
import io.github.cocosip.stow.internal.projection.QueueProjectionService;
import io.github.cocosip.stow.internal.projection.SqliteMetadataProjectionStore;
import io.github.cocosip.stow.internal.quota.DefaultDirectoryQuotaManager;
import io.github.cocosip.stow.internal.quota.DefaultTenantQuotaManager;
import io.github.cocosip.stow.internal.quota.SqliteQuotaRepository;
import io.github.cocosip.stow.internal.recovery.DatabaseHealthService;
import io.github.cocosip.stow.internal.recovery.DatabaseRecoveryService;
import io.github.cocosip.stow.internal.scheduler.DefaultStoragePool;
import io.github.cocosip.stow.internal.scheduler.ProcessingTimeoutRecovery;
import io.github.cocosip.stow.internal.scheduler.TerminalLeaseIndex;
import io.github.cocosip.stow.internal.statistics.DefaultStatisticsReader;
import io.github.cocosip.stow.internal.tenant.DefaultTenantManager;
import io.github.cocosip.stow.internal.tenant.JsonTenantRepository;
import io.github.cocosip.stow.internal.watcher.DefaultFileWatcherAutoManager;
import io.github.cocosip.stow.internal.watcher.DefaultFileWatcherManager;
import io.github.cocosip.stow.internal.watcher.SourceCleanupStore;
import io.github.cocosip.stow.internal.watcher.SourceCleanupWorker;
import io.github.cocosip.stow.model.CleanupStatistics;
import io.github.cocosip.stow.model.ComponentHealth;
import io.github.cocosip.stow.model.DatabaseHealthReport;
import io.github.cocosip.stow.model.HealthStatus;
import io.github.cocosip.stow.model.RuntimeHealth;
import io.github.cocosip.stow.spi.JournalCodec;
import io.github.cocosip.stow.spi.QueueEventJournal;
import io.github.cocosip.stow.spi.StorageVolume;
import io.github.cocosip.stow.spi.StorageVolumeProvider;
import java.nio.file.Path;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

@SuppressFBWarnings(
        value = "EI_EXPOSE_REP",
        justification = "The runtime intentionally exposes its public service facades while owning their lifecycle.")
public final class DefaultStowRuntime implements StowRuntime {

    private final StowConfiguration configuration;
    private final Clock clock;
    private final ExecutorService suppliedWorkerExecutor;
    private final ScheduledExecutorService suppliedScheduler;
    private final StorageVolumeProvider storageVolumeProvider;
    private final JournalCodec journalCodec;
    private final List<ManagedBackgroundService> backgroundServices;
    private final DefaultRuntimeHealth runtimeHealth;
    private final AtomicReference<RuntimeState> state = new AtomicReference<>(RuntimeState.NEW);
    private final RuntimeQuotaOperationAdmission quotaOperationAdmission = new RuntimeQuotaOperationAdmission(state);
    private final Deque<AutoCloseable> ownedResources = new ArrayDeque<>();

    private ExecutorService workerExecutor;
    private ScheduledExecutorService scheduler;
    private TenantManager tenantManager;
    private SqliteQuotaRepository quotaRepository;
    private QueueEventJournal eventJournal;
    private SqliteMetadataProjectionStore metadataProjection;
    private QueueProjectionService projectionService;
    private ProjectionMaintenanceService projectionMaintenanceService;
    private DefaultStoragePool storagePoolService;
    private DefaultStorageMaintenance storageMaintenanceService;
    private DefaultFileWatcherManager watcherManagerService;
    private DefaultFileWatcherAutoManager watcherAutoManagerService;
    private SourceCleanupStore sourceCleanupStore;
    private SourceCleanupWorker sourceCleanupWorker;
    private DefaultStatisticsReader statisticsReaderService;
    private List<StorageVolume> storageVolumes = List.of();
    private final AtomicInteger projectionRotation = new AtomicInteger();
    private final Map<String, Instant> lastSnapshotAt = new ConcurrentHashMap<>();
    private volatile Instant lastBackgroundReclaimAt;
    private volatile Instant lastOptimizeAt;
    private volatile Instant lastStatisticsOutputAt;

    DefaultStowRuntime(
            StowConfiguration configuration,
            Clock clock,
            ExecutorService workerExecutor,
            ScheduledExecutorService scheduler,
            List<ManagedBackgroundService> backgroundServices) {
        this(configuration, clock, workerExecutor, scheduler, null, null, backgroundServices);
    }

    DefaultStowRuntime(
            StowConfiguration configuration,
            Clock clock,
            ExecutorService workerExecutor,
            ScheduledExecutorService scheduler,
            StorageVolumeProvider storageVolumeProvider,
            JournalCodec journalCodec,
            List<ManagedBackgroundService> backgroundServices) {
        this.configuration = Objects.requireNonNull(configuration, "configuration");
        this.clock = Objects.requireNonNull(clock, "clock");
        suppliedWorkerExecutor = workerExecutor;
        suppliedScheduler = scheduler;
        this.storageVolumeProvider = storageVolumeProvider;
        this.journalCodec = journalCodec;
        this.backgroundServices = List.copyOf(backgroundServices);
        runtimeHealth = new DefaultRuntimeHealth(clock);
    }

    @Override
    public RuntimeState state() {
        return state.get();
    }

    @Override
    public synchronized void start() {
        if (!state.compareAndSet(RuntimeState.NEW, RuntimeState.STARTING)) {
            throw new IllegalStateException("Stow runtime can only be started from NEW state");
        }

        try {
            ownedResources.push(RuntimeDirectoryLock.acquire(configuration.paths()));
            initializeExecutors();
            initializeTenantManager();
            initializeQuotaManagers();
            initializeStorageServices();
            BackgroundServiceCoordinator coordinator = initializeBackgroundServices();
            ownedResources.push(coordinator);
            coordinator.start();
            state.set(RuntimeState.RUNNING);
        } catch (RuntimeException | Error failure) {
            state.set(RuntimeState.FAILED);
            RuntimeException closeFailure = closeOwnedResources();
            if (closeFailure != null) {
                failure.addSuppressed(closeFailure);
            }
            throw failure;
        }
    }

    @Override
    public synchronized void close() {
        RuntimeState current = state.get();
        if (current == RuntimeState.TERMINATED) {
            return;
        }
        AtomicReference<RuntimeException> closeFailure = new AtomicReference<>();
        quotaOperationAdmission.closeAdmission(() -> state.set(RuntimeState.STOPPING), () -> {
            closeFailure.set(closeOwnedResources());
            state.set(RuntimeState.TERMINATED);
        });
        RuntimeException failure = closeFailure.get();
        if (failure != null) {
            throw failure;
        }
    }

    @Override
    public StoragePool storagePool() {
        ensureRunning();
        if (storagePoolService == null) return unavailableService("StoragePool");
        return storagePoolService;
    }

    @Override
    public TenantManager tenantManager() {
        ensureRunning();
        return tenantManager;
    }

    @Override
    public TenantQuotaManager tenantQuotaManager() {
        ensureRunning();
        return new DefaultTenantQuotaManager(quotaRepository, quotaOperationAdmission);
    }

    @Override
    public DirectoryQuotaManager directoryQuotaManager() {
        ensureRunning();
        return new DefaultDirectoryQuotaManager(quotaRepository, quotaOperationAdmission);
    }

    @Override
    public StorageMaintenance maintenance() {
        ensureRunning();
        if (storageMaintenanceService == null) return unavailableService("StorageMaintenance");
        return storageMaintenanceService;
    }

    @Override
    public QueueProjectionMaintenance projectionMaintenance() {
        ensureRunning();
        if (projectionMaintenanceService == null) return unavailableService("QueueProjectionMaintenance");
        return projectionMaintenanceService;
    }

    @Override
    public FileWatcherManager fileWatcherManager() {
        ensureRunning();
        if (watcherManagerService == null) return unavailableService("FileWatcherManager");
        return watcherManagerService;
    }

    @Override
    public FileWatcherOptionsManager fileWatcherOptionsManager() {
        ensureRunning();
        if (watcherManagerService == null) return unavailableService("FileWatcherOptionsManager");
        return watcherManagerService.options();
    }

    @Override
    public FileWatcherAutoManager fileWatcherAutoManager() {
        ensureRunning();
        if (watcherAutoManagerService == null) return unavailableService("FileWatcherAutoManager");
        return watcherAutoManagerService;
    }

    @Override
    public StatisticsReader statisticsReader() {
        ensureRunning();
        return statisticsReaderService;
    }

    @Override
    public RuntimeHealth health() {
        for (StorageVolume volume : storageVolumes) {
            String component = "volume:" + volume.id();
            try {
                boolean healthy = volume.healthy();
                long available = volume.availableCapacity();
                runtimeHealth.update(
                        component,
                        healthy ? HealthStatus.UP : HealthStatus.DOWN,
                        healthy ? "availableBytes=" + available : "unhealthy");
            } catch (RuntimeException failure) {
                runtimeHealth.update(component, HealthStatus.DOWN, message(failure));
            }
        }
        updateJournalHealth();
        updateSqliteHealth();
        return runtimeHealth.snapshot(state.get());
    }

    /**
     * The journal component reports the tenant-level state: a tenant latched DOWN by
     * mid-file corruption or a dead writer marks the component DOWN (register W18).
     */
    private void updateJournalHealth() {
        if (eventJournal == null) return;
        long downTenants = 0;
        long totalTenants = 0;
        String detail = null;
        for (String tenantId : eventJournal.tenantIds().stream().sorted().toList()) {
            totalTenants++;
            try {
                eventJournal.readBatch(tenantId, eventJournal.tailOffset(tenantId), 1);
            } catch (RuntimeException failure) {
                downTenants++;
                detail = tenantId + ": " + message(failure);
            }
        }
        if (totalTenants == 0) {
            runtimeHealth.update("journal", HealthStatus.UP, "no tenant journals");
        } else if (downTenants == 0) {
            runtimeHealth.update("journal", HealthStatus.UP, totalTenants + " tenants up");
        } else {
            runtimeHealth.update(
                    "journal",
                    HealthStatus.DOWN,
                    downTenants + "/" + totalTenants + " tenants down" + (detail == null ? "" : "; last: " + detail));
        }
    }

    /** The sqlite component aggregates the database quick-check report (register W18). */
    private void updateSqliteHealth() {
        if (storageMaintenanceService == null) return;
        try {
            io.github.cocosip.stow.model.DatabaseHealthReport report = storageMaintenanceService.checkDatabases();
            long down = report.databases().values().stream()
                    .filter(component -> component.status() == HealthStatus.DOWN)
                    .count();
            long degraded = report.databases().values().stream()
                    .filter(component -> component.status() == HealthStatus.DEGRADED)
                    .count();
            if (down > 0) {
                runtimeHealth.update("sqlite", HealthStatus.DOWN, down + " database(s) failed quick_check");
            } else if (degraded > 0) {
                runtimeHealth.update("sqlite", HealthStatus.DEGRADED, degraded + " database(s) busy or locked");
            } else {
                runtimeHealth.update(
                        "sqlite", HealthStatus.UP, report.databases().size() + " database(s) ok");
            }
        } catch (RuntimeException failure) {
            runtimeHealth.update("sqlite", HealthStatus.DOWN, message(failure));
        }
    }

    Optional<StorageVolumeProvider> storageVolumeProvider() {
        return Optional.ofNullable(storageVolumeProvider);
    }

    Optional<JournalCodec> journalCodec() {
        return Optional.ofNullable(journalCodec);
    }

    ExecutorService workerExecutor() {
        ensureRunning();
        return workerExecutor;
    }

    ScheduledExecutorService scheduler() {
        ensureRunning();
        return scheduler;
    }

    private void initializeExecutors() {
        if (suppliedWorkerExecutor == null) {
            workerExecutor = Executors.newThreadPerTaskExecutor(
                    Thread.ofVirtual().name("stow-worker-", 0).factory());
            ownedResources.push(workerExecutor::shutdown);
        } else {
            workerExecutor = suppliedWorkerExecutor;
        }

        if (suppliedScheduler == null) {
            scheduler = Executors.newSingleThreadScheduledExecutor(
                    Thread.ofPlatform().daemon(true).name("stow-scheduler-", 0).factory());
            ownedResources.push(scheduler::shutdown);
        } else {
            scheduler = suppliedScheduler;
        }
    }

    private void initializeTenantManager() {
        var tenantConfiguration = configuration.tenant();
        tenantManager = new DefaultTenantManager(
                new JsonTenantRepository(configuration.paths().metadataDirectory()),
                clock,
                tenantConfiguration.autoCreateTenants(),
                tenantConfiguration.defaultQuota());
        tenantConfiguration.preconfiguredTenants().forEach(tenantManager::create);
    }

    private void initializeQuotaManagers() {
        DefaultTenantManager tenants = (DefaultTenantManager) tenantManager;
        quotaRepository = new SqliteQuotaRepository(
                configuration.paths().quotaDirectory(), configuration.sqlite(), clock, tenants::quotaLimit);
    }

    private void initializeStorageServices() {
        StorageVolumeProvider provider =
                storageVolumeProvider == null ? new DefaultStorageVolumeProvider() : storageVolumeProvider;
        storageVolumes = configuration.volumes().stream()
                .map(configuration -> provider.create(configuration))
                .toList();
        if (!storageVolumes.isEmpty()) {
            ownedResources.push(() -> closeVolumes(storageVolumes));
            gateVolumeMounts();
        }
        // Attach provisioning roots so tenants created from now on get their
        // directories pre-created (Locus storage-path provisioning). The runtime
        // always constructs the concrete manager, so a plain cast is safe.
        List<Path> provisioningRoots = new java.util.ArrayList<>();
        provisioningRoots.add(configuration.paths().metadataDirectory());
        provisioningRoots.add(configuration.paths().quotaDirectory());
        provisioningRoots.add(configuration.paths().queueDirectory());
        for (StorageVolume volume : storageVolumes) provisioningRoots.add(volume.mountPath());
        ((DefaultTenantManager) tenantManager).provisionUnder(provisioningRoots);

        JournalCodec codec = journalCodec == null ? defaultJournalCodec() : journalCodec;
        eventJournal =
                new FileQueueEventJournal(configuration.paths().queueDirectory(), configuration.journal(), codec);
        ownedResources.push(eventJournal);
        SequencedJournalAppender appender = new SequencedJournalAppender(eventJournal);
        statisticsReaderService = new DefaultStatisticsReader(configuration.statistics(), clock);

        metadataProjection = new SqliteMetadataProjectionStore(
                configuration.paths().metadataDirectory(), configuration.sqlite(), clock);
        QueueEventReducer reducer =
                new QueueEventReducer(metadataProjection, quotaRepository, statisticsReaderService.recorder());
        ProjectionCursorStore cursors =
                new ProjectionCursorStore(configuration.paths().metadataDirectory(), clock);
        ProjectionSnapshotStore snapshots =
                new ProjectionSnapshotStore(configuration.paths().metadataDirectory());
        TerminalLeaseIndex terminalLeases = new TerminalLeaseIndex();
        rebuildTerminalLeaseIndex(terminalLeases);
        projectionService = new QueueProjectionService(
                eventJournal,
                reducer,
                cursors,
                clock,
                failure -> runtimeHealth.update("projection", HealthStatus.DEGRADED, message(failure)),
                terminalLeases::record);
        projectionMaintenanceService = new ProjectionMaintenanceService(
                eventJournal, projectionService, cursors, snapshots, reducer, metadataProjection, quotaRepository);
        projectionMaintenanceService.manualReplayBatchSize(
                configuration.projection().manualReplayBatchSize());

        if (!storageVolumes.isEmpty()) {
            storagePoolService = new DefaultStoragePool(
                    DefaultStoragePool.tenantManager(tenantManager),
                    quotaRepository,
                    metadataProjection,
                    projectionService,
                    eventJournal,
                    storageVolumes,
                    clock,
                    configuration.retry(),
                    appender,
                    statisticsReaderService.recorder(),
                    terminalLeases,
                    new DefaultStoragePool.PoolSettings(
                            configuration.storage().emptyQueueReclaimBatchSize(),
                            configuration.cleanup().processingTimeout(),
                            configuration.storage().completionGuardStripes()));
            ProcessingTimeoutRecovery timeoutRecovery = new ProcessingTimeoutRecovery(
                    storagePoolService, configuration.cleanup().processingTimeout());
            storageMaintenanceService = new DefaultStorageMaintenance(
                    eventJournal,
                    metadataProjection,
                    quotaRepository,
                    projectionService,
                    storageVolumes,
                    configuration.paths().metadataDirectory(),
                    configuration.paths().quotaDirectory(),
                    configuration.sqlite(),
                    clock,
                    configuration.cleanup(),
                    tenantId -> ((DefaultTenantManager) tenantManager).quotaLimit(tenantId),
                    timeoutRecovery,
                    appender);
            sourceCleanupStore = new SourceCleanupStore(configuration.sourceCleanup(), configuration.sqlite(), clock);
            watcherManagerService = new DefaultFileWatcherManager(
                    configuration.paths().watcherDirectory(),
                    storagePoolService,
                    tenantManager,
                    clock,
                    statisticsReaderService.recorder(),
                    result -> runtimeHealth.update(
                            "watcher",
                            result.failedCount() == 0 ? HealthStatus.UP : HealthStatus.DEGRADED,
                            result.failedCount() == 0
                                    ? "running"
                                    : result.errors().get(0).summary()),
                    configuration.sourceCleanup(),
                    sourceCleanupStore);
            watcherAutoManagerService = new DefaultFileWatcherAutoManager(
                    configuration.paths().watcherDirectory(), watcherManagerService, tenantManager, clock);
            for (var watcher : configuration.watchers()) {
                if (watcherManagerService.find(watcher.watcherId()).isPresent()) {
                    watcherManagerService.update(watcher);
                } else {
                    watcherManagerService.register(watcher);
                }
            }
            sourceCleanupWorker = new SourceCleanupWorker(
                    configuration.sourceCleanup(),
                    sourceCleanupStore,
                    watcherManagerService.options(),
                    watcherManagerService,
                    clock,
                    workerExecutor,
                    scheduler);
        }
        recoverDatabasesAtStartup();
        replayJournal(cursors);
        reconcileQuotaReservations();
        runtimeHealth.update("projection", HealthStatus.UP, "ready");
    }

    /**
     * Checks every projected database at startup and rebuilds damaged ones from the
     * snapshot plus journal, matching the original's startup self-healing behavior.
     * Connections are per-operation, so the corrupt files can be moved aside before
     * any live operation touches them.
     */
    private void recoverDatabasesAtStartup() {
        DatabaseHealthService health = new DatabaseHealthService(
                configuration.paths().metadataDirectory(), configuration.paths().quotaDirectory(), clock);
        DatabaseHealthReport report = health.checkDatabases();
        if (report.databases().values().stream().allMatch(c -> c.status() != HealthStatus.DOWN)) return;
        DatabaseRecoveryService recovery = new DatabaseRecoveryService(
                configuration.paths().metadataDirectory(),
                configuration.paths().quotaDirectory(),
                eventJournal,
                configuration.sqlite(),
                clock,
                tenantId -> ((DefaultTenantManager) tenantManager).quotaLimit(tenantId));
        for (Map.Entry<String, ComponentHealth> entry : report.databases().entrySet()) {
            if (entry.getValue().status() != HealthStatus.DOWN) continue;
            int separator = entry.getKey().indexOf('/');
            if (separator < 0) continue;
            String database = entry.getKey().substring(0, separator);
            String tenantId = entry.getKey().substring(separator + 1);
            try {
                // Metadata rebuild replays the snapshot plus journal into a fresh
                // database after backing the corrupt file up; the quota rebuild then
                // recomputes counts from the recovered metadata.
                if (database.equals("metadata")) {
                    recovery.rebuildMetadata(tenantId);
                    recovery.rebuildQuota(tenantId);
                } else if (database.equals("quota")) {
                    recovery.rebuildQuota(tenantId);
                }
            } catch (RuntimeException failure) {
                throw new IllegalStateException(
                        "Unable to recover damaged " + database + " database for tenant " + tenantId, failure);
            }
        }
    }

    private void reconcileQuotaReservations() {
        // Union of journal tenants and quota-database tenants: a crash between the quota
        // reservation and the journal append leaves a tenant that the journal never saw.
        java.util.Set<String> tenantIds = new java.util.HashSet<>(eventJournal.tenantIds());
        Path quotaRoot = configuration.paths().quotaDirectory();
        if (java.nio.file.Files.isDirectory(quotaRoot)) {
            try (var directories = java.nio.file.Files.list(quotaRoot)) {
                directories
                        .filter(java.nio.file.Files::isDirectory)
                        .map(path -> path.getFileName().toString())
                        .filter(name -> name.matches("[A-Za-z0-9._-]{1,128}"))
                        .forEach(tenantIds::add);
            } catch (java.io.IOException exception) {
                throw new IllegalStateException("Unable to enumerate quota tenants for reconciliation", exception);
            }
        }
        for (String tenantId : tenantIds) {
            java.util.Set<String> activeFileKeys = new java.util.HashSet<>();
            for (SqliteMetadataProjectionStore.FileRow row : metadataProjection.activeFiles(tenantId)) {
                activeFileKeys.add(row.fileKey());
            }
            quotaRepository.reconcileReservations(tenantId, activeFileKeys);
        }
    }

    /**
     * Automatic snapshot and compaction for a caught-up tenant, honoring the configured
     * byte thresholds; the journal would otherwise grow without bound.
     */
    private void maybeSnapshotAndCompact(String tenantId) {
        try {
            var configuration = this.configuration;
            long processedBytes = eventJournal.tailOffset(tenantId) - eventJournal.baseOffset(tenantId);
            if (processedBytes <= 0) return;
            var state = projectionMaintenanceService.state(tenantId);
            if (state.projectedOffset() != state.tailOffset()) return;
            Instant now = clock.instant();
            Instant previous = lastSnapshotAt.get(tenantId);
            if (configuration.snapshot().enabled()
                    && processedBytes >= configuration.snapshot().minimumProgressBytes()
                    && (previous == null
                            || !now.isBefore(
                                    previous.plus(configuration.snapshot().interval())))) {
                projectionMaintenanceService.snapshot(tenantId);
                lastSnapshotAt.put(tenantId, now);
            }
            if (configuration.compaction().enabled()
                    && processedBytes >= configuration.compaction().minimumProcessedBytes()) {
                projectionMaintenanceService.compact(tenantId);
                lastSnapshotAt.put(tenantId, now);
            }
        } catch (RuntimeException failure) {
            runtimeHealth.update("projection", HealthStatus.DEGRADED, message(failure));
        }
    }

    private void rebuildTerminalLeaseIndex(TerminalLeaseIndex terminalLeases) {
        for (String tenantId : eventJournal.tenantIds()) {
            long cursor = eventJournal.baseOffset(tenantId);
            long tail = eventJournal.tailOffset(tenantId);
            while (cursor < tail) {
                var batch = eventJournal.readBatch(tenantId, cursor, 512);
                batch.events().forEach(terminalLeases::record);
                if (batch.events().isEmpty() || batch.nextOffset() <= cursor) break;
                cursor = batch.nextOffset();
            }
        }
    }

    private BackgroundServiceCoordinator initializeBackgroundServices() {
        List<BackgroundServiceCoordinator.Service> services = new ArrayList<>();
        services.add(new ProjectionBackgroundService());
        if (storageMaintenanceService != null && configuration.cleanup().enabled()) {
            services.add(BackgroundServiceCoordinator.periodic(
                    "cleanup",
                    scheduler,
                    monitored("cleanup", this::runCleanupCycle),
                    configuration.cleanup().initialDelay(),
                    configuration.cleanup().interval()));
        }
        if (storageMaintenanceService != null && configuration.orphanRecovery().enabled()) {
            Duration initialDelay = configuration.orphanRecovery().runOnStartup()
                    ? Duration.ZERO
                    : configuration.orphanRecovery().interval();
            services.add(BackgroundServiceCoordinator.periodic(
                    "orphan-recovery",
                    scheduler,
                    monitored("orphan-recovery", storageMaintenanceService::recoverAllOrphans),
                    initialDelay,
                    configuration.orphanRecovery().interval()));
        }
        if (sourceCleanupWorker != null) {
            services.add(new ManagedBackgroundService() {
                @Override
                public void start() {
                    sourceCleanupWorker.start();
                }

                @Override
                public void close() {
                    sourceCleanupWorker.close();
                }
            });
        }
        if (watcherManagerService != null) {
            services.add(new ManagedBackgroundService() {
                @Override
                public void start() {
                    watcherManagerService.start();
                    runtimeHealth.update("watcher", HealthStatus.UP, "running");
                }

                @Override
                public void close() {
                    watcherManagerService.close();
                }
            });
        }
        services.addAll(backgroundServices);
        return new BackgroundServiceCoordinator(services);
    }

    private void runCleanupCycle() {
        if (configuration.storage().backgroundReclaimEnabled()) {
            Instant now = clock.instant();
            Duration cooldown = configuration.storage().reclaimCooldown();
            Instant previous = lastBackgroundReclaimAt;
            if (previous == null || !now.isBefore(previous.plus(cooldown))) {
                storagePoolService.recoverTimedOut(
                        configuration.cleanup().processingTimeout(),
                        configuration.storage().backgroundReclaimBatchSize());
                lastBackgroundReclaimAt = now;
            }
        }
        storageMaintenanceService.cleanupCompleted(configuration.cleanup().completedRetention());
        storageMaintenanceService.cleanupPermanentlyFailed(
                configuration.cleanup().failedRetention());
        // Housekeeping the original schedules alongside the cleanup cycle. Write-path
        // temporary files are intentionally NOT swept here (they belong to in-flight
        // writes; startup owns their removal).
        silentMaintenance(storageMaintenanceService::cleanupInvalidDatabaseBackups);
        silentMaintenance(storageMaintenanceService::cleanupEmptyDirectories);
        silentMaintenance(storageMaintenanceService::cleanupOrphanedMetadata);
        // VACUUM takes the database write lock, so it runs daily like the original's
        // scheduled maintenance instead of on every cycle.
        Instant now = clock.instant();
        Instant previousOptimize = lastOptimizeAt;
        if (previousOptimize == null || !now.isBefore(previousOptimize.plus(Duration.ofHours(24)))) {
            lastOptimizeAt = now;
            try {
                storageMaintenanceService.optimizeDatabases();
            } catch (RuntimeException ignored) {
                // VACUUM fails benignly under concurrent writers; the next day retries.
            }
        }
        outputStatisticsIfNeeded(now);
    }

    /**
     * Optional periodic statistics output (Locus LocusStatisticsOutputOptions, logging
     * sink): every configured interval a snapshot over the query window is written to
     * the stow.statistics logger.
     */
    private void outputStatisticsIfNeeded(Instant now) {
        io.github.cocosip.stow.config.StatisticsConfiguration statistics = configuration.statistics();
        if (!statistics.enabled() || !statistics.outputEnabled()) return;
        Instant previous = lastStatisticsOutputAt;
        if (previous != null && now.isBefore(previous.plus(statistics.outputInterval()))) return;
        lastStatisticsOutputAt = now;
        try {
            Instant to = now;
            Instant from = to.minus(statistics.outputQueryWindow());
            io.github.cocosip.stow.model.StatisticsSnapshot snapshot = statisticsReaderService.snapshot(
                    new io.github.cocosip.stow.model.StatisticsQuery(from, to, null, null, null, null));
            STATISTICS_LOGGER.log(
                    System.Logger.Level.INFO,
                    "statistics window from={0} to={1}: writtenFiles={2} writtenBytes={3} reads={4} claims={5} "
                            + "completed={6} sqlitePersistence={7} watcherImports={8} watcherBytes={9} series={10}",
                    from,
                    to,
                    snapshot.writtenFileCount(),
                    snapshot.writtenBytes(),
                    snapshot.readCount(),
                    snapshot.claimCount(),
                    snapshot.completedCount(),
                    snapshot.sqlitePersistenceOperationCount(),
                    snapshot.watcherImportedCount(),
                    snapshot.watcherImportedBytes(),
                    snapshot.series());
        } catch (RuntimeException ignored) {
            // A statistics output failure must never disturb the cleanup cycle.
        }
    }

    private static final System.Logger STATISTICS_LOGGER = System.getLogger("stow.statistics");

    private void silentMaintenance(java.util.function.Supplier<CleanupStatistics> action) {
        try {
            action.get();
        } catch (RuntimeException failure) {
            runtimeHealth.update("cleanup", HealthStatus.DEGRADED, message(failure));
        }
    }

    private Runnable monitored(String component, Runnable action) {
        return () -> {
            try {
                action.run();
                runtimeHealth.update(component, HealthStatus.UP, "ready");
            } catch (RuntimeException failure) {
                runtimeHealth.update(component, HealthStatus.DEGRADED, message(failure));
            }
        };
    }

    private void replayJournal(ProjectionCursorStore cursors) {
        for (String tenantId : eventJournal.tenantIds()) {
            projectionService.projectTenantUntilCaughtUp(
                    tenantId, configuration.projection().maxRecordsPerTenantCycle());
        }
    }

    private JournalCodec defaultJournalCodec() {
        return configuration.journal().format() == io.github.cocosip.stow.config.JournalFormat.JSON_LINES_V1
                ? new JsonLinesJournalCodec()
                : new BinaryV1JournalCodec();
    }

    private static void closeVolumes(List<StorageVolume> volumes) {
        RuntimeException failure = null;
        for (StorageVolume volume : volumes) {
            try {
                volume.close();
            } catch (RuntimeException exception) {
                if (failure == null) failure = exception;
                else failure.addSuppressed(exception);
            }
        }
        if (failure != null) throw failure;
    }

    /**
     * Mount-time gate (Locus AddVolumeAsync): a volume joins the runtime only after two
     * consecutive forced health probes pass, so a missing or read-only mount fails
     * startup instead of silently receiving no writes.
     */
    private void gateVolumeMounts() {
        for (StorageVolume volume : storageVolumes) {
            int consecutive = 0;
            RuntimeException lastFailure = null;
            for (int attempt = 0; attempt < 3 && consecutive < 2; attempt++) {
                try {
                    consecutive = volume.probeHealth() ? consecutive + 1 : 0;
                } catch (RuntimeException exception) {
                    consecutive = 0;
                    lastFailure = exception;
                }
            }
            if (consecutive < 2) {
                throw new io.github.cocosip.stow.exception.StorageVolumeUnavailableException(
                        "Storage volume " + volume.id() + " is not healthy at mount time", lastFailure);
            }
        }
    }

    private RuntimeException closeOwnedResources() {
        RuntimeException failure = null;
        while (!ownedResources.isEmpty()) {
            try {
                ownedResources.pop().close();
            } catch (Exception exception) {
                RuntimeException runtimeFailure = exception instanceof RuntimeException runtimeException
                        ? runtimeException
                        : new IllegalStateException("Unable to close runtime resource", exception);
                if (failure == null) {
                    failure = runtimeFailure;
                } else {
                    failure.addSuppressed(runtimeFailure);
                }
            }
        }
        return failure;
    }

    private <T> T unavailableService(String serviceName) {
        ensureRunning();
        throw new RuntimeNotReadyException(serviceName + " is not available in the current implementation stage");
    }

    private void ensureRunning() {
        if (state.get() != RuntimeState.RUNNING) {
            throw new RuntimeNotReadyException("Stow runtime is not running");
        }
    }

    private static String message(RuntimeException failure) {
        return failure.getMessage() == null ? failure.getClass().getSimpleName() : failure.getMessage();
    }

    private final class ProjectionBackgroundService implements BackgroundServiceCoordinator.Service {
        private final AtomicBoolean running = new AtomicBoolean();
        private ScheduledFuture<?> future;
        private Thread runningThread;

        @Override
        public synchronized void start() {
            if (!running.compareAndSet(false, true)) return;
            schedule(Duration.ZERO);
        }

        @Override
        public synchronized void close() {
            running.set(false);
            if (future != null) future.cancel(false);
            while (runningThread != null && runningThread != Thread.currentThread()) {
                try {
                    wait();
                } catch (InterruptedException exception) {
                    Thread.currentThread().interrupt();
                    throw new StowInterruptedException("Interrupted while stopping background projection", exception);
                }
            }
        }

        private void runCycle() {
            synchronized (this) {
                if (!running.get()) return;
                runningThread = Thread.currentThread();
            }
            boolean busy = false;
            try {
                long deadline = System.nanoTime()
                        + configuration.projection().cycleTimeBudget().toNanos();
                int tenants = 0;
                List<String> tenantIds =
                        eventJournal.tenantIds().stream().sorted().toList();
                int limit = configuration.projection().maxTenantsPerCycle();
                // Rotate the start index so every tenant is eventually served even when
                // the per-cycle budget is smaller than the tenant count.
                int start =
                        tenantIds.isEmpty() ? 0 : Math.floorMod(projectionRotation.getAndAdd(limit), tenantIds.size());
                for (int index = 0; index < tenantIds.size() && tenants < limit; index++) {
                    if (System.nanoTime() >= deadline) break;
                    String tenantId = tenantIds.get((start + index) % tenantIds.size());
                    boolean tenantBusy = projectionService.projectTenant(
                            tenantId, configuration.projection().maxRecordsPerTenantCycle());
                    busy |= tenantBusy;
                    tenants++;
                    if (!tenantBusy) maybeSnapshotAndCompact(tenantId);
                }
                runtimeHealth.update("projection", HealthStatus.UP, "ready");
            } catch (RuntimeException failure) {
                runtimeHealth.update("projection", HealthStatus.DEGRADED, message(failure));
            } finally {
                synchronized (this) {
                    runningThread = null;
                    notifyAll();
                    if (running.get()) {
                        schedule(
                                busy
                                        ? configuration.projection().busyCycleDelay()
                                        : configuration.projection().idleCycleDelay());
                    }
                }
            }
        }

        private synchronized void schedule(Duration delay) {
            if (!running.get()) return;
            future = scheduler.schedule(this::runCycle, Math.max(1, delay.toMillis()), TimeUnit.MILLISECONDS);
        }
    }
}
