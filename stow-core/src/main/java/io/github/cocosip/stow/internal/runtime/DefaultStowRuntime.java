package io.github.cocosip.stow.internal.runtime;

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
import io.github.cocosip.stow.internal.tenant.DefaultTenantManager;
import io.github.cocosip.stow.internal.tenant.JsonTenantRepository;
import io.github.cocosip.stow.model.ComponentHealth;
import io.github.cocosip.stow.model.HealthStatus;
import io.github.cocosip.stow.model.RuntimeHealth;
import io.github.cocosip.stow.spi.JournalCodec;
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

public final class DefaultStowRuntime implements StowRuntime {

    private final StowConfiguration configuration;
    private final Clock clock;
    private final ExecutorService suppliedWorkerExecutor;
    private final ScheduledExecutorService suppliedScheduler;
    private final StorageVolumeProvider storageVolumeProvider;
    private final JournalCodec journalCodec;
    private final List<ManagedBackgroundService> backgroundServices;
    private final AtomicReference<RuntimeState> state = new AtomicReference<>(RuntimeState.NEW);
    private final Deque<AutoCloseable> ownedResources = new ArrayDeque<>();

    private ExecutorService workerExecutor;
    private ScheduledExecutorService scheduler;
    private TenantManager tenantManager;

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
        state.set(RuntimeState.STOPPING);
        RuntimeException failure = closeOwnedResources();
        state.set(RuntimeState.TERMINATED);
        if (failure != null) {
            throw failure;
        }
    }

    @Override
    public StoragePool storagePool() {
        return unavailableService("StoragePool");
    }

    @Override
    public TenantManager tenantManager() {
        ensureRunning();
        return tenantManager;
    }

    @Override
    public TenantQuotaManager tenantQuotaManager() {
        return unavailableService("TenantQuotaManager");
    }

    @Override
    public DirectoryQuotaManager directoryQuotaManager() {
        return unavailableService("DirectoryQuotaManager");
    }

    @Override
    public StorageMaintenance maintenance() {
        return unavailableService("StorageMaintenance");
    }

    @Override
    public QueueProjectionMaintenance projectionMaintenance() {
        return unavailableService("QueueProjectionMaintenance");
    }

    @Override
    public FileWatcherManager fileWatcherManager() {
        return unavailableService("FileWatcherManager");
    }

    @Override
    public FileWatcherOptionsManager fileWatcherOptionsManager() {
        return unavailableService("FileWatcherOptionsManager");
    }

    @Override
    public FileWatcherAutoManager fileWatcherAutoManager() {
        return unavailableService("FileWatcherAutoManager");
    }

    @Override
    public StatisticsReader statisticsReader() {
        return unavailableService("StatisticsReader");
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
