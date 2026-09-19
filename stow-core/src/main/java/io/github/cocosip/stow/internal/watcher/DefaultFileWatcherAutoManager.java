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
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Clock;
import java.time.Duration;
import java.util.HashSet;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

/** Creates one durable watcher per tenant directory below a configured root. */
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
        } catch (IOException exception) {
            throw new IllegalStateException("Unable to initialize watcher root", exception);
        }
        Set<String> managed = new HashSet<>();
        try {
            if (configuration.autoCreateTenantDirectories()) {
                for (TenantContext tenant : tenants.list())
                    Files.createDirectories(rootPath.resolve(tenant.tenantId()));
            }
            try (var paths = Files.list(rootPath)) {
                paths.filter(path -> Files.isDirectory(path, LinkOption.NOFOLLOW_LINKS))
                        .sorted()
                        .forEach(path -> {
                            String tenantId = path.getFileName().toString();
                            Optional<TenantContext> tenant = tenants.find(tenantId);
                            if (tenant == null) tenant = Optional.empty();
                            if (tenant.isEmpty()) {
                                if (!configuration.autoCreateTenantDirectories()) return;
                                tenant = Optional.of(tenants.create(tenantId));
                            }
                            WatcherConfiguration watcher = watcherFor(
                                    configuration, tenant.orElseThrow().tenantId(), path);
                            if (manager.find(watcher.watcherId()).isPresent()) manager.update(watcher);
                            else manager.register(watcher);
                            managed.add(watcher.watcherId());
                        });
            }
        } catch (IOException exception) {
            throw new IllegalStateException("Unable to discover watcher tenant directories", exception);
        }
        store.writeManagedWatcherIds(managed);
        return managed.size();
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

    private WatcherConfiguration watcherFor(WatcherRootConfiguration root, String tenantId, Path watchPath) {
        return new WatcherConfiguration(
                watcherId(root.rootPath(), tenantId),
                tenantId,
                WatcherTenantMode.SINGLE_TENANT,
                root.autoCreateTenantDirectories(),
                watchPath,
                root.enabled(),
                root.recursive(),
                root.globs(),
                root.postImportAction(),
                root.moveDirectory(),
                Duration.ofSeconds(30),
                0,
                Duration.ZERO,
                Duration.ZERO,
                1,
                1,
                Duration.ofDays(30),
                Duration.ofSeconds(5));
    }

    private static String watcherId(Path root, String tenantId) {
        try {
            byte[] digest =
                    MessageDigest.getInstance("SHA-256").digest(root.toString().getBytes(StandardCharsets.UTF_8));
            StringBuilder hash = new StringBuilder();
            for (int index = 0; index < 6; index++) hash.append(String.format("%02x", digest[index]));
            String suffix = tenantId.length() > 110 ? tenantId.substring(0, 110) : tenantId;
            return "auto-" + hash + "-" + suffix;
        } catch (NoSuchAlgorithmException exception) {
            throw new IllegalStateException("SHA-256 is unavailable", exception);
        }
    }
}
