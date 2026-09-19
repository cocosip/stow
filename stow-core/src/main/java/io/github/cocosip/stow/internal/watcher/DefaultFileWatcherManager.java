package io.github.cocosip.stow.internal.watcher;

import io.github.cocosip.stow.api.FileWatcherManager;
import io.github.cocosip.stow.api.FileWatcherOptionsManager;
import io.github.cocosip.stow.api.StoragePool;
import io.github.cocosip.stow.api.TenantManager;
import io.github.cocosip.stow.model.WatcherConfiguration;
import io.github.cocosip.stow.model.WatcherOptions;
import io.github.cocosip.stow.model.WatcherScanResult;
import java.nio.file.Path;
import java.time.Clock;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/** Public watcher configuration facade backed by durable state. */
public final class DefaultFileWatcherManager implements FileWatcherManager, AutoCloseable {

    private final WatcherConfigurationStore store;
    private final WatcherScanner scanner;
    private final DefaultOptionsManager options;
    private final AtomicBoolean running = new AtomicBoolean();
    private ScheduledExecutorService scheduler;

    public DefaultFileWatcherManager(
            Path watcherDirectory, StoragePool storagePool, TenantManager tenants, Clock clock) {
        store = new WatcherConfigurationStore(watcherDirectory);
        ImportedFileHistory history = new ImportedFileHistory(watcherDirectory.resolve("history"), clock);
        scanner = new WatcherScanner(storagePool, tenants, history, clock);
        options = new DefaultOptionsManager(store);
    }

    public DefaultFileWatcherManager(WatcherConfigurationStore store, WatcherScanner scanner) {
        this.store = Objects.requireNonNull(store, "store");
        this.scanner = Objects.requireNonNull(scanner, "scanner");
        options = new DefaultOptionsManager(store);
    }

    @Override
    public WatcherConfiguration register(WatcherConfiguration configuration) {
        Objects.requireNonNull(configuration, "configuration");
        if (store.find(configuration.watcherId()).isPresent()) {
            throw new IllegalArgumentException("Watcher already exists: " + configuration.watcherId());
        }
        return store.save(configuration);
    }

    @Override
    public WatcherConfiguration update(WatcherConfiguration configuration) {
        Objects.requireNonNull(configuration, "configuration");
        if (store.find(configuration.watcherId()).isEmpty()) {
            throw new IllegalArgumentException("Watcher does not exist: " + configuration.watcherId());
        }
        return store.save(configuration);
    }

    @Override
    public void remove(String watcherId) {
        requireExisting(watcherId);
        store.remove(watcherId);
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
        return scanner.scan(requireExisting(watcherId));
    }

    public FileWatcherOptionsManager options() {
        return options;
    }

    /** Starts the polling loop; lifecycle owners may call this after runtime recovery is complete. */
    public synchronized void start() {
        if (!running.compareAndSet(false, true)) return;
        scheduler = Executors.newSingleThreadScheduledExecutor(Thread.ofPlatform()
                .daemon(true)
                .name("stow-watcher-scheduler-", 0)
                .factory());
        long interval = Math.max(1, options.get().scanInterval().toMillis());
        scheduler.scheduleWithFixedDelay(this::poll, 0, interval, TimeUnit.MILLISECONDS);
    }

    public synchronized void stop() {
        if (!running.compareAndSet(true, false)) return;
        if (scheduler != null) {
            scheduler.shutdownNow();
            scheduler = null;
        }
    }

    WatcherConfigurationStore store() {
        return store;
    }

    @Override
    public void close() {
        stop();
    }

    private void poll() {
        if (!running.get() || !options.get().enabled()) return;
        List<WatcherConfiguration> enabled =
                list().stream().filter(WatcherConfiguration::enabled).toList();
        if (enabled.isEmpty()) return;
        WatcherOptions current = options.get();
        Semaphore permits = new Semaphore(current.maxParallelScans());
        try (ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor()) {
            for (WatcherConfiguration configuration : enabled) {
                executor.submit(() -> {
                    boolean acquired = false;
                    try {
                        permits.acquire();
                        acquired = true;
                        scanner.scan(configuration);
                    } catch (InterruptedException exception) {
                        Thread.currentThread().interrupt();
                    } catch (RuntimeException ignored) {
                        // A single polling failure is isolated to this round.
                    } finally {
                        if (acquired) permits.release();
                    }
                });
            }
        }
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
                configuration.historyFlushInterval());
    }

    private static final class DefaultOptionsManager implements FileWatcherOptionsManager {
        private final WatcherConfigurationStore store;

        private DefaultOptionsManager(WatcherConfigurationStore store) {
            this.store = store;
        }

        @Override
        public WatcherOptions get() {
            return store.readOptions();
        }

        @Override
        public void update(WatcherOptions options) {
            store.writeOptions(options);
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
