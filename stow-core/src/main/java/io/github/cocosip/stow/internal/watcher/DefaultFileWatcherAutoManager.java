package io.github.cocosip.stow.internal.watcher;

import io.github.cocosip.stow.api.FileWatcherAutoManager;
import io.github.cocosip.stow.api.TenantManager;
import io.github.cocosip.stow.model.TenantContext;
import io.github.cocosip.stow.model.WatcherConfiguration;
import io.github.cocosip.stow.model.WatcherRootConfiguration;
import io.github.cocosip.stow.model.WatcherTenantMode;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Clock;
import java.time.Duration;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

/** Creates one durable multi-tenant watcher per configured root. */
public final class DefaultFileWatcherAutoManager implements FileWatcherAutoManager {

    private final WatcherConfigurationStore store;
    private final DefaultFileWatcherManager manager;
    private final TenantManager tenants;
    private final Clock clock;

    public DefaultFileWatcherAutoManager(
            Path watcherDirectory, DefaultFileWatcherManager manager, TenantManager tenants, Clock clock) {
        store = new WatcherConfigurationStore(watcherDirectory);
        this.manager = Objects.requireNonNull(manager, "manager");
        this.tenants = Objects.requireNonNull(tenants, "tenants");
        this.clock = Objects.requireNonNull(clock, "clock");
    }

    @Override
    public int apply(WatcherRootConfiguration configuration) {
        Objects.requireNonNull(configuration, "configuration");
        store.writeRoot(configuration);
        removeManagedWatchers();
        return discoverAndCreate();
    }

    @Override
    public int discoverAndCreate() {
        Optional<WatcherRootConfiguration> root = store.readRoot();
        if (root.isEmpty()) return 0;
        WatcherRootConfiguration configuration = root.orElseThrow();
        Path rootPath = configuration.rootPath();
        try {
            Files.createDirectories(rootPath);
            if (configuration.autoCreateTenantDirectories()) {
                // Locus-aligned provisioning: import directories are created for
                // existing tenants only; unknown directory names never mint tenants.
                for (TenantContext tenant : tenants.list()) {
                    Files.createDirectories(rootPath.resolve(tenant.tenantId()));
                }
            }
        } catch (IOException exception) {
            throw new IllegalStateException("Unable to initialize watcher root", exception);
        }
        WatcherConfiguration watcher = watcherFor(configuration);
        if (manager.find(watcher.watcherId()).isPresent()) manager.update(watcher);
        else manager.register(watcher);
        store.writeManagedWatcherIds(Set.of(watcher.watcherId()));
        return 1;
    }

    @Override
    public void removeManagedWatchers() {
        for (String watcherId : store.readManagedWatcherIds()) {
            manager.find(watcherId).ifPresent(ignored -> manager.remove(watcherId));
        }
        store.writeManagedWatcherIds(Set.of());
    }

    @Override
    public Optional<WatcherRootConfiguration> currentRoot() {
        return store.readRoot();
    }

    private WatcherConfiguration watcherFor(WatcherRootConfiguration root) {
        return new WatcherConfiguration(
                watcherId(root.rootPath()),
                null,
                WatcherTenantMode.SUBDIRECTORY_TENANTS,
                root.autoCreateTenantDirectories(),
                root.rootPath(),
                root.enabled(),
                root.recursive(),
                root.globs(),
                root.postImportAction(),
                root.moveDirectory(),
                root.pollInterval(),
                root.maxFileSize(),
                root.minimumFileAge(),
                root.stabilityCheckInterval(),
                root.stabilityCheckCount(),
                root.concurrentImports(),
                Duration.ofDays(30),
                Duration.ofSeconds(5),
                root.sourceCleanupFailureDirectory(),
                5,
                Duration.ofSeconds(5),
                Duration.ofMinutes(5));
    }

    private static String watcherId(Path root) {
        try {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            digest.update("multi-tenant:".getBytes(StandardCharsets.UTF_8));
            digest.update(root.toString().getBytes(StandardCharsets.UTF_8));
            byte[] hashBytes = digest.digest();
            StringBuilder hash = new StringBuilder();
            for (int index = 0; index < 8; index++) hash.append(String.format("%02x", hashBytes[index]));
            return "auto-multi-tenant-" + hash;
        } catch (NoSuchAlgorithmException exception) {
            throw new IllegalStateException("SHA-256 is unavailable", exception);
        }
    }
}
