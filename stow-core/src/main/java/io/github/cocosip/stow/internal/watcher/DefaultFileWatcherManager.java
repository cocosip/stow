package io.github.cocosip.stow.internal.watcher;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import io.github.cocosip.stow.api.FileWatcherManager;
import io.github.cocosip.stow.api.FileWatcherOptionsManager;
import io.github.cocosip.stow.api.StoragePool;
import io.github.cocosip.stow.api.TenantManager;
import io.github.cocosip.stow.config.SourceCleanupConfiguration;
import io.github.cocosip.stow.exception.StowInterruptedException;
import io.github.cocosip.stow.internal.statistics.NoopStatisticsRecorder;
import io.github.cocosip.stow.internal.statistics.StatisticsRecorder;
import io.github.cocosip.stow.model.WatcherConfiguration;
import io.github.cocosip.stow.model.WatcherOptions;
import io.github.cocosip.stow.model.WatcherScanResult;
import java.nio.file.Path;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Public watcher configuration facade backed by durable state. */
public final class DefaultFileWatcherManager implements FileWatcherManager, AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(DefaultFileWatcherManager.class);

    private final WatcherConfigurationStore store;
    private final WatcherScanner scanner;
    private final DefaultOptionsManager options;
    private final Consumer<WatcherScanResult> scanObserver;
    private final Clock clock;
    private final ExecutorService scanExecutor = Executors.newVirtualThreadPerTaskExecutor();
    private final ConcurrentHashMap<String, Instant> nextDueByWatcherId = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, CompletableFuture<WatcherScanResult>> activeScans =
            new ConcurrentHashMap<>();
    private final Object admissionLock = new Object();
    private final AtomicBoolean running = new AtomicBoolean();
    private int admittedScans;
    private boolean closed;
    private ScheduledExecutorService scheduler;
    private ScheduledFuture<?> pollFuture;
    private long pollGeneration;

    public DefaultFileWatcherManager(
            Path watcherDirectory, StoragePool storagePool, TenantManager tenants, Clock clock) {
        this(watcherDirectory, storagePool, tenants, clock, new NoopStatisticsRecorder(clock));
    }

    public DefaultFileWatcherManager(
            Path watcherDirectory,
            StoragePool storagePool,
            TenantManager tenants,
            Clock clock,
            StatisticsRecorder statistics) {
        this(watcherDirectory, storagePool, tenants, clock, statistics, ignored -> {});
    }

    public DefaultFileWatcherManager(
            Path watcherDirectory,
            StoragePool storagePool,
            TenantManager tenants,
            Clock clock,
            StatisticsRecorder statistics,
            Consumer<WatcherScanResult> scanObserver) {
        this(watcherDirectory, storagePool, tenants, clock, statistics, scanObserver, null, null);
    }

    public DefaultFileWatcherManager(
            Path watcherDirectory,
            StoragePool storagePool,
            TenantManager tenants,
            Clock clock,
            StatisticsRecorder statistics,
            Consumer<WatcherScanResult> scanObserver,
            SourceCleanupConfiguration cleanupConfiguration,
            SourceCleanupStore cleanupStore) {
        this.clock = Objects.requireNonNull(clock, "clock");
        store = new WatcherConfigurationStore(watcherDirectory);
        options = new DefaultOptionsManager(store);
        ImportedFileHistory history = new ImportedFileHistory(watcherDirectory.resolve("history"), clock);
        scanner = cleanupStore == null
                ? new WatcherScanner(storagePool, tenants, history, clock, statistics)
                : new WatcherScanner(
                        storagePool,
                        tenants,
                        history,
                        clock,
                        statistics,
                        Objects.requireNonNull(cleanupConfiguration, "cleanupConfiguration"),
                        cleanupStore,
                        new SourceCleanupGate(cleanupConfiguration, options, this));
        this.scanObserver = Objects.requireNonNull(scanObserver, "scanObserver");
    }

    public DefaultFileWatcherManager(WatcherConfigurationStore store, WatcherScanner scanner) {
        this(store, scanner, Clock.systemUTC());
    }

    DefaultFileWatcherManager(WatcherConfigurationStore store, WatcherScanner scanner, Clock clock) {
        this.store = Objects.requireNonNull(store, "store");
        this.scanner = Objects.requireNonNull(scanner, "scanner");
        this.clock = Objects.requireNonNull(clock, "clock");
        options = new DefaultOptionsManager(store);
        scanObserver = ignored -> {};
    }

    @Override
    public WatcherConfiguration register(WatcherConfiguration configuration) {
        Objects.requireNonNull(configuration, "configuration");
        if (store.find(configuration.watcherId()).isPresent()) {
            throw new IllegalArgumentException("Watcher already exists: " + configuration.watcherId());
        }
        WatcherConfiguration saved = store.save(configuration);
        nextDueByWatcherId.remove(configuration.watcherId());
        rescheduleNow();
        return saved;
    }

    @Override
    public WatcherConfiguration update(WatcherConfiguration configuration) {
        Objects.requireNonNull(configuration, "configuration");
        if (store.find(configuration.watcherId()).isEmpty()) {
            throw new IllegalArgumentException("Watcher does not exist: " + configuration.watcherId());
        }
        WatcherConfiguration saved = store.save(configuration);
        nextDueByWatcherId.remove(configuration.watcherId());
        rescheduleNow();
        return saved;
    }

    @Override
    public void remove(String watcherId) {
        requireExisting(watcherId);
        store.remove(watcherId);
        nextDueByWatcherId.remove(watcherId);
        rescheduleNow();
    }

    @Override
    public void enable(String watcherId) {
        update(enabled(requireExisting(watcherId), true));
    }

    @Override
    public void disable(String watcherId) {
        update(enabled(requireExisting(watcherId), false));
    }

    @Override
    public Optional<WatcherConfiguration> find(String watcherId) {
        return store.find(watcherId);
    }

    @Override
    public List<WatcherConfiguration> list() {
        return store.list();
    }

    @Override
    public List<WatcherConfiguration> listForTenant(String tenantId) {
        return store.list().stream()
                .filter(configuration -> Objects.equals(configuration.tenantId(), tenantId))
                .toList();
    }

    @Override
    public WatcherScanResult scanNow(String watcherId) {
        return executeScan(requireExisting(watcherId), false);
    }

    public FileWatcherOptionsManager options() {
        return options;
    }

    /** Starts the polling loop; lifecycle owners may call this after runtime recovery is complete. */
    public synchronized void start() {
        synchronized (admissionLock) {
            if (closed) throw new IllegalStateException("File watcher manager is closed");
        }
        if (!running.compareAndSet(false, true)) return;
        scheduler = Executors.newSingleThreadScheduledExecutor(Thread.ofPlatform()
                .daemon(true)
                .name("stow-watcher-scheduler-", 0)
                .factory());
        schedulePoll(Duration.ZERO);
    }

    @SuppressFBWarnings(
            value = "NN_NAKED_NOTIFY",
            justification = "The notification follows the running-state transition stored in AtomicBoolean.")
    public void stop() {
        ScheduledExecutorService stoppedScheduler;
        synchronized (this) {
            running.set(false);
            pollGeneration++;
            if (pollFuture != null) {
                pollFuture.cancel(false);
                pollFuture = null;
            }
            stoppedScheduler = scheduler;
            scheduler = null;
            if (stoppedScheduler != null) stoppedScheduler.shutdownNow();
        }
        synchronized (admissionLock) {
            admissionLock.notifyAll();
        }
        awaitActiveScans();
        awaitScheduler(stoppedScheduler);
    }

    WatcherConfigurationStore store() {
        return store;
    }

    @Override
    public void close() {
        synchronized (admissionLock) {
            closed = true;
            admissionLock.notifyAll();
        }
        stop();
        scanExecutor.shutdown();
        try {
            while (!scanExecutor.awaitTermination(1, TimeUnit.DAYS)) {
                // Continue waiting without interrupting admitted scans.
            }
        } catch (InterruptedException exception) {
            Thread.currentThread().interrupt();
            throw new StowInterruptedException("Interrupted while stopping file watcher scans", exception);
        }
    }

    private void poll(long generation) {
        try {
            if (running.get()) pollOnce(true);
        } catch (RuntimeException exception) {
            LOG.warn("Watcher polling round failed; continuing with next round", exception);
        } finally {
            synchronized (this) {
                if (running.get() && generation == pollGeneration) schedulePoll(nextPollDelay());
            }
        }
    }

    private synchronized void schedulePoll(Duration delay) {
        if (!running.get() || scheduler == null) return;
        long generation = ++pollGeneration;
        pollFuture = scheduler.schedule(() -> poll(generation), Math.max(0, delay.toMillis()), TimeUnit.MILLISECONDS);
    }

    private synchronized void rescheduleNow() {
        if (!running.get() || scheduler == null) return;
        pollGeneration++;
        if (pollFuture != null) pollFuture.cancel(false);
        schedulePoll(Duration.ZERO);
    }

    private Duration nextPollDelay() {
        WatcherOptions current = options.get();
        Duration delay = normalized(current.scanInterval());
        if (!current.enabled()) return delay;
        Instant now = clock.instant();
        for (WatcherConfiguration configuration : list()) {
            if (!configuration.enabled()) continue;
            Duration candidate;
            Instant due = nextDueByWatcherId.get(configuration.watcherId());
            if (activeScans.containsKey(configuration.watcherId()) || Instant.MAX.equals(due)) {
                candidate = normalized(configuration.pollInterval());
            } else if (due == null || !due.isAfter(now)) {
                candidate = Duration.ofMillis(1);
            } else {
                candidate = Duration.between(now, due);
            }
            if (candidate.compareTo(delay) < 0) delay = candidate;
        }
        return normalized(delay);
    }

    void pollOnce() {
        pollOnce(false);
    }

    private void pollOnce(boolean background) {
        if (!options.get().enabled()) return;
        List<WatcherConfiguration> enabled =
                list().stream().filter(WatcherConfiguration::enabled).toList();
        if (enabled.isEmpty()) return;
        Instant now = clock.instant();
        for (WatcherConfiguration configuration : enabled) {
            if (background && !running.get()) return;
            if (activeScans.containsKey(configuration.watcherId())) continue;
            Instant due = nextDueByWatcherId.get(configuration.watcherId());
            if (due != null && due.isAfter(now)) continue;
            nextDueByWatcherId.put(configuration.watcherId(), Instant.MAX);
            scanExecutor.submit(() -> {
                try {
                    executeScan(configuration, background);
                } catch (RuntimeException ignored) {
                    // A single watcher failure is isolated from the polling coordinator.
                }
            });
        }
    }

    private WatcherScanResult executeScan(WatcherConfiguration configuration, boolean background) {
        String watcherId = configuration.watcherId();
        CompletableFuture<WatcherScanResult> current = new CompletableFuture<>();
        CompletableFuture<WatcherScanResult> existing = activeScans.putIfAbsent(watcherId, current);
        if (existing != null) return join(existing);

        boolean admitted = false;
        try {
            admitted = acquireScan(background);
            if (!admitted) throw new IllegalStateException("File watcher scan admission is closed");
            WatcherScanResult result = scanner.scan(configuration);
            scanObserver.accept(result);
            current.complete(result);
            return result;
        } catch (RuntimeException | Error failure) {
            current.completeExceptionally(failure);
            throw failure;
        } finally {
            if (admitted) releaseScan();
            activeScans.remove(watcherId, current);
            WatcherConfiguration latest = store.find(watcherId).orElse(configuration);
            nextDueByWatcherId.put(watcherId, clock.instant().plus(normalized(latest.pollInterval())));
        }
    }

    private boolean acquireScan(boolean background) {
        synchronized (admissionLock) {
            while (!closed
                    && (!background || running.get())
                    && admittedScans >= options.get().maxParallelScans()) {
                try {
                    admissionLock.wait();
                } catch (InterruptedException exception) {
                    Thread.currentThread().interrupt();
                    throw new StowInterruptedException("Interrupted while waiting for a watcher scan slot", exception);
                }
            }
            if (closed || (background && !running.get())) return false;
            admittedScans++;
            return true;
        }
    }

    private void releaseScan() {
        synchronized (admissionLock) {
            admittedScans--;
            admissionLock.notifyAll();
        }
    }

    private void awaitActiveScans() {
        synchronized (admissionLock) {
            while (admittedScans > 0) {
                try {
                    admissionLock.wait();
                } catch (InterruptedException exception) {
                    Thread.currentThread().interrupt();
                    throw new StowInterruptedException("Interrupted while stopping file watcher scans", exception);
                }
            }
        }
    }

    private static void awaitScheduler(ScheduledExecutorService stoppedScheduler) {
        if (stoppedScheduler == null) return;
        try {
            while (!stoppedScheduler.awaitTermination(1, TimeUnit.DAYS)) {
                // Continue waiting for an in-progress polling round.
            }
        } catch (InterruptedException exception) {
            Thread.currentThread().interrupt();
            throw new StowInterruptedException("Interrupted while stopping file watcher polling", exception);
        }
    }

    private static WatcherScanResult join(CompletableFuture<WatcherScanResult> scan) {
        try {
            return scan.join();
        } catch (CompletionException exception) {
            if (exception.getCause() instanceof RuntimeException runtime) throw runtime;
            if (exception.getCause() instanceof Error error) throw error;
            throw exception;
        }
    }

    private static Duration normalized(Duration interval) {
        return interval.isZero() ? Duration.ofMillis(1) : interval;
    }

    private WatcherConfiguration requireExisting(String watcherId) {
        return store.find(Objects.requireNonNull(watcherId, "watcherId"))
                .orElseThrow(() -> new IllegalArgumentException("Watcher does not exist: " + watcherId));
    }

    private static WatcherConfiguration enabled(WatcherConfiguration configuration, boolean enabled) {
        return new WatcherConfiguration(
                configuration.watcherId(),
                configuration.tenantId(),
                configuration.tenantMode(),
                configuration.autoCreateTenantDirectories(),
                configuration.watchPath(),
                enabled,
                configuration.recursive(),
                configuration.globs(),
                configuration.postImportAction(),
                configuration.moveDirectory(),
                configuration.pollInterval(),
                configuration.maxFileSize(),
                configuration.minimumFileAge(),
                configuration.stabilityCheckInterval(),
                configuration.stabilityCheckCount(),
                configuration.concurrentImports(),
                configuration.historyRetention(),
                configuration.historyFlushInterval(),
                configuration.sourceCleanupFailureDirectory(),
                configuration.maxPostImportActionAttempts(),
                configuration.postImportRetryInitialDelay(),
                configuration.postImportRetryMaxDelay());
    }

    private final class DefaultOptionsManager implements FileWatcherOptionsManager {
        private final WatcherConfigurationStore store;

        private DefaultOptionsManager(WatcherConfigurationStore store) {
            this.store = store;
        }

        @Override
        public WatcherOptions get() {
            return store.readOptions();
        }

        @Override
        @SuppressFBWarnings(
                value = "NN_NAKED_NOTIFY",
                justification = "The notification follows a durable watcher-options update outside this monitor.")
        public void update(WatcherOptions options) {
            store.writeOptions(options);
            synchronized (admissionLock) {
                admissionLock.notifyAll();
            }
            rescheduleNow();
        }

        @Override
        public void enable() {
            update(copy(get(), true));
        }

        @Override
        public void disable() {
            update(copy(get(), false));
        }

        private static WatcherOptions copy(WatcherOptions current, boolean enabled) {
            return new WatcherOptions(
                    enabled,
                    current.maxParallelScans(),
                    current.scanInterval(),
                    current.historyFlushDebounce(),
                    current.historyRetention());
        }
    }
}
