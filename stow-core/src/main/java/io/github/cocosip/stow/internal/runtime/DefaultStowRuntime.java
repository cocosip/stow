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
import io.github.cocosip.stow.internal.projection.ActiveFileCache;
import io.github.cocosip.stow.internal.projection.ProjectionCursorStore;
import io.github.cocosip.stow.internal.projection.ProjectionMaintenanceService;
import io.github.cocosip.stow.internal.projection.ProjectionSnapshotStore;
import io.github.cocosip.stow.internal.projection.QueueEventReducer;
import io.github.cocosip.stow.internal.projection.QueueProjectionService;
import io.github.cocosip.stow.internal.projection.SqliteMetadataProjectionStore;
import io.github.cocosip.stow.internal.quota.DefaultDirectoryQuotaManager;
import io.github.cocosip.stow.internal.quota.DefaultTenantQuotaManager;
import io.github.cocosip.stow.internal.quota.SqliteQuotaRepository;
import io.github.cocosip.stow.internal.scheduler.DefaultStoragePool;
import io.github.cocosip.stow.internal.scheduler.ProcessingTimeoutRecovery;
import io.github.cocosip.stow.internal.scheduler.TerminalLeaseIndex;
import io.github.cocosip.stow.internal.statistics.DefaultStatisticsReader;
import io.github.cocosip.stow.internal.tenant.DefaultTenantManager;
import io.github.cocosip.stow.internal.tenant.JsonTenantRepository;
import io.github.cocosip.stow.internal.watcher.DefaultFileWatcherAutoManager;
import io.github.cocosip.stow.internal.watcher.DefaultFileWatcherManager;
import io.github.cocosip.stow.model.HealthStatus;
import io.github.cocosip.stow.model.RuntimeHealth;
import io.github.cocosip.stow.spi.JournalCodec;
import io.github.cocosip.stow.spi.QueueEventJournal;
import io.github.cocosip.stow.spi.StorageVolume;
import io.github.cocosip.stow.spi.StorageVolumeProvider;
import java.time.Clock;
import java.time.Duration;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
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
    private DefaultStatisticsReader statisticsReaderService;
    private List<StorageVolume> storageVolumes = List.of();

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
        return runtimeHealth.snapshot(state.get());
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
        }

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
        ActiveFileCache activeCache = new ActiveFileCache(metadataProjection);
        TerminalLeaseIndex terminalLeases = new TerminalLeaseIndex();
        rebuildTerminalLeaseIndex(terminalLeases);
        projectionService = new QueueProjectionService(
                eventJournal,
                reducer,
                cursors,
                clock,
                activeCache,
                failure -> runtimeHealth.update("projection", HealthStatus.DEGRADED, message(failure)),
                terminalLeases::record);
        projectionMaintenanceService =
                new ProjectionMaintenanceService(eventJournal, projectionService, cursors, snapshots, reducer);

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
                    terminalLeases);
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
                                    : result.errors().get(0).summary()));
            watcherAutoManagerService = new DefaultFileWatcherAutoManager(
                    configuration.paths().watcherDirectory(), watcherManagerService, tenantManager, clock);
            for (var watcher : configuration.watchers()) {
                if (watcherManagerService.find(watcher.watcherId()).isPresent()) {
                    watcherManagerService.update(watcher);
                } else {
                    watcherManagerService.register(watcher);
                }
            }
        }
        replayJournal(cursors);
        runtimeHealth.update("projection", HealthStatus.UP, "ready");
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
        storageMaintenanceService.reclaimTimedOutProcessing(
                configuration.cleanup().processingTimeout());
        storageMaintenanceService.cleanupCompleted(configuration.cleanup().completedRetention());
        storageMaintenanceService.cleanupPermanentlyFailed(
                configuration.cleanup().failedRetention());
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
            } catch (Exception exception) {
                RuntimeException current = exception instanceof RuntimeException runtime
                        ? runtime
                        : new IllegalStateException("Unable to close storage volume", exception);
                if (failure == null) failure = current;
                else failure.addSuppressed(current);
            }
        }
        if (failure != null) throw failure;
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
                for (String tenantId :
                        eventJournal.tenantIds().stream().sorted().toList()) {
                    if (tenants++ >= configuration.projection().maxTenantsPerCycle() || System.nanoTime() >= deadline)
                        break;
                    busy |= projectionService.projectTenant(
                            tenantId, configuration.projection().maxRecordsPerTenantCycle());
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
