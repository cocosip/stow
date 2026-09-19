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
import io.github.cocosip.stow.internal.cleanup.DefaultStorageMaintenance;
import io.github.cocosip.stow.internal.filesystem.DefaultStorageVolumeProvider;
import io.github.cocosip.stow.internal.journal.BinaryV1JournalCodec;
import io.github.cocosip.stow.internal.journal.FileQueueEventJournal;
import io.github.cocosip.stow.internal.journal.JsonLinesJournalCodec;
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
import io.github.cocosip.stow.internal.statistics.DefaultStatisticsReader;
import io.github.cocosip.stow.internal.tenant.DefaultTenantManager;
import io.github.cocosip.stow.internal.tenant.JsonTenantRepository;
import io.github.cocosip.stow.internal.watcher.DefaultFileWatcherAutoManager;
import io.github.cocosip.stow.internal.watcher.DefaultFileWatcherManager;
import io.github.cocosip.stow.model.ComponentHealth;
import io.github.cocosip.stow.model.HealthStatus;
import io.github.cocosip.stow.model.RuntimeHealth;
import io.github.cocosip.stow.spi.JournalCodec;
import io.github.cocosip.stow.spi.QueueEventJournal;
import io.github.cocosip.stow.spi.StorageVolume;
import io.github.cocosip.stow.spi.StorageVolumeProvider;
import java.time.Clock;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
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
            for (ManagedBackgroundService service : backgroundServices) {
                ownedResources.push(service);
                service.start();
            }
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
        RuntimeState current = state.get();
        HealthStatus status =
                switch (current) {
                    case RUNNING -> HealthStatus.UP;
                    case NEW, STARTING, STOPPING -> HealthStatus.DEGRADED;
                    case TERMINATED, FAILED -> HealthStatus.DOWN;
                };
        ComponentHealth runtime = new ComponentHealth(status, current.name().toLowerCase(), clock.instant());
        return new RuntimeHealth(status, Map.of("runtime", runtime));
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

        metadataProjection = new SqliteMetadataProjectionStore(
                configuration.paths().metadataDirectory(), configuration.sqlite(), clock);
        QueueEventReducer reducer = new QueueEventReducer(metadataProjection, quotaRepository);
        ProjectionCursorStore cursors =
                new ProjectionCursorStore(configuration.paths().metadataDirectory(), clock);
        ProjectionSnapshotStore snapshots =
                new ProjectionSnapshotStore(configuration.paths().metadataDirectory());
        ActiveFileCache activeCache = new ActiveFileCache(metadataProjection);
        projectionService = new QueueProjectionService(eventJournal, reducer, cursors, clock, activeCache);
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
                    configuration.retry());
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
                    tenantId -> ((DefaultTenantManager) tenantManager).quotaLimit(tenantId));
            watcherManagerService = new DefaultFileWatcherManager(
                    configuration.paths().watcherDirectory(), storagePoolService, tenantManager, clock);
            watcherAutoManagerService = new DefaultFileWatcherAutoManager(
                    configuration.paths().watcherDirectory(), watcherManagerService, tenantManager, clock);
        }
        statisticsReaderService = new DefaultStatisticsReader(configuration.statistics(), clock);
        replayJournal(cursors);
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
}
