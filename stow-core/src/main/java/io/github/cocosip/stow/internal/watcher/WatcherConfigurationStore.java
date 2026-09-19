package io.github.cocosip.stow.internal.watcher;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import io.github.cocosip.stow.internal.sqlite.AtomicJsonFile;
import io.github.cocosip.stow.model.PostImportAction;
import io.github.cocosip.stow.model.WatcherConfiguration;
import io.github.cocosip.stow.model.WatcherOptions;
import io.github.cocosip.stow.model.WatcherRootConfiguration;
import io.github.cocosip.stow.model.WatcherTenantMode;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.locks.ReentrantLock;

/** Durable JSON state for watcher configurations and global watcher settings. */
public final class WatcherConfigurationStore {

    private static final int SCHEMA_VERSION = 1;
    private static final WatcherOptions DEFAULT_OPTIONS =
            new WatcherOptions(true, 1, Duration.ofSeconds(30), Duration.ofSeconds(5), Duration.ofDays(30));

    private final Path directory;
    private final Path configurations;
    private final ObjectMapper mapper;
    private final ReentrantLock lock = new ReentrantLock();

    public WatcherConfigurationStore(Path watcherDirectory) {
        directory = Objects.requireNonNull(watcherDirectory, "watcherDirectory")
                .toAbsolutePath()
                .normalize();
        configurations = directory.resolve("watchers");
        mapper = new ObjectMapper()
                .registerModule(new Jdk8Module())
                .registerModule(new JavaTimeModule())
                .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
        try {
            Files.createDirectories(configurations);
            deleteStaleTemporaryFiles();
        } catch (IOException exception) {
            throw failure("Unable to initialize watcher state", exception);
        }
    }

    public List<WatcherConfiguration> list() {
        lock.lock();
        try {
            if (!Files.isDirectory(configurations, LinkOption.NOFOLLOW_LINKS)) return List.of();
            try (var paths = Files.list(configurations)) {
                return paths.filter(path -> path.getFileName().toString().endsWith(".json"))
                        .filter(path -> Files.isRegularFile(path, LinkOption.NOFOLLOW_LINKS))
                        .map(this::readConfiguration)
                        .sorted(Comparator.comparing(WatcherConfiguration::watcherId))
                        .toList();
            } catch (IOException exception) {
                throw failure("Unable to list watcher configurations", exception);
            }
        } finally {
            lock.unlock();
        }
    }

    public Optional<WatcherConfiguration> find(String watcherId) {
        Objects.requireNonNull(watcherId, "watcherId");
        lock.lock();
        try {
            Path path = configurationPath(watcherId);
            return Files.exists(path, LinkOption.NOFOLLOW_LINKS)
                    ? Optional.of(readConfiguration(path))
                    : Optional.empty();
        } finally {
            lock.unlock();
        }
    }

    public WatcherConfiguration save(WatcherConfiguration configuration) {
        Objects.requireNonNull(configuration, "configuration");
        lock.lock();
        try {
            write(configurationPath(configuration.watcherId()), ConfigurationDocument.from(configuration));
            return configuration;
        } finally {
            lock.unlock();
        }
    }

    public void remove(String watcherId) {
        Objects.requireNonNull(watcherId, "watcherId");
        lock.lock();
        try {
            try {
                Files.deleteIfExists(configurationPath(watcherId));
            } catch (IOException exception) {
                throw failure("Unable to remove watcher configuration", exception);
            }
        } finally {
            lock.unlock();
        }
    }

    public WatcherOptions readOptions() {
        lock.lock();
        try {
            Path path = directory.resolve("options.json");
            if (!Files.exists(path, LinkOption.NOFOLLOW_LINKS)) return DEFAULT_OPTIONS;
            return read(path, OptionsDocument.class).toModel();
        } finally {
            lock.unlock();
        }
    }

    public void writeOptions(WatcherOptions options) {
        Objects.requireNonNull(options, "options");
        lock.lock();
        try {
            write(directory.resolve("options.json"), OptionsDocument.from(options));
        } finally {
            lock.unlock();
        }
    }

    public Optional<WatcherRootConfiguration> readRoot() {
        lock.lock();
        try {
            Path path = directory.resolve("root.json");
            return Files.exists(path, LinkOption.NOFOLLOW_LINKS)
                    ? Optional.of(read(path, RootDocument.class).toModel())
                    : Optional.empty();
        } finally {
            lock.unlock();
        }
    }

    public void writeRoot(WatcherRootConfiguration root) {
        Objects.requireNonNull(root, "root");
        lock.lock();
        try {
            write(directory.resolve("root.json"), RootDocument.from(root));
        } finally {
            lock.unlock();
        }
    }

    public void deleteRoot() {
        lock.lock();
        try {
            try {
                Files.deleteIfExists(directory.resolve("root.json"));
            } catch (IOException exception) {
                throw failure("Unable to remove watcher root configuration", exception);
            }
        } finally {
            lock.unlock();
        }
    }

    public Set<String> readManagedWatcherIds() {
        lock.lock();
        try {
            Path path = directory.resolve("managed.json");
            if (!Files.exists(path, LinkOption.NOFOLLOW_LINKS)) return Set.of();
            return Set.copyOf(read(path, ManagedDocument.class).watcherIds());
        } finally {
            lock.unlock();
        }
    }

    public void writeManagedWatcherIds(Set<String> watcherIds) {
        Objects.requireNonNull(watcherIds, "watcherIds");
        lock.lock();
        try {
            write(
                    directory.resolve("managed.json"),
                    new ManagedDocument(
                            SCHEMA_VERSION, watcherIds.stream().sorted().toList()));
        } finally {
            lock.unlock();
        }
    }

    private WatcherConfiguration readConfiguration(Path path) {
        return read(path, ConfigurationDocument.class).toModel();
    }

    private <T> T read(Path path, Class<T> type) {
        try {
            return mapper.readValue(path.toFile(), type);
        } catch (IOException | RuntimeException exception) {
            throw failure("Unable to read watcher state " + path.getFileName(), exception);
        }
    }

    private void write(Path path, Object document) {
        try {
            new AtomicJsonFile<Object>(path, mapper, Object.class).write(document);
        } catch (IOException exception) {
            throw failure("Unable to persist watcher state " + path.getFileName(), exception);
        }
    }

    private void deleteStaleTemporaryFiles() throws IOException {
        deleteStaleIn(directory);
        deleteStaleIn(configurations);
    }

    private static void deleteStaleIn(Path path) throws IOException {
        if (!Files.isDirectory(path, LinkOption.NOFOLLOW_LINKS)) return;
        try (var files = Files.list(path)) {
            files.filter(candidate -> candidate.getFileName().toString().startsWith(".")
                            && candidate.getFileName().toString().endsWith(".tmp"))
                    .filter(candidate -> Files.isRegularFile(candidate, LinkOption.NOFOLLOW_LINKS))
                    .forEach(candidate -> {
                        try {
                            Files.deleteIfExists(candidate);
                        } catch (IOException exception) {
                            throw failure("Unable to remove stale watcher temporary file", exception);
                        }
                    });
        }
    }

    private Path configurationPath(String watcherId) {
        if (watcherId == null || watcherId.isBlank() || watcherId.contains(".") || watcherId.contains("/")) {
            throw new IllegalArgumentException("Invalid watcher ID");
        }
        return configurations.resolve(watcherId + ".json").normalize();
    }

    private static IllegalStateException failure(String message, Throwable cause) {
        return new IllegalStateException(message, cause);
    }

    record ConfigurationDocument(
            int schemaVersion,
            String watcherId,
            String tenantId,
            String tenantMode,
            boolean autoCreateTenantDirectories,
            String watchPath,
            boolean enabled,
            boolean recursive,
            List<String> globs,
            String postImportAction,
            String moveDirectory,
            long pollIntervalMillis,
            long maxFileSize,
            long minimumFileAgeMillis,
            long stabilityCheckIntervalMillis,
            int stabilityCheckCount,
            int concurrentImports,
            long historyRetentionMillis,
            long historyFlushIntervalMillis) {

        static ConfigurationDocument from(WatcherConfiguration model) {
            return new ConfigurationDocument(
                    SCHEMA_VERSION,
                    model.watcherId(),
                    model.tenantId(),
                    model.tenantMode().name(),
                    model.autoCreateTenantDirectories(),
                    model.watchPath().toString(),
                    model.enabled(),
                    model.recursive(),
                    model.globs(),
                    model.postImportAction().name(),
                    model.moveDirectory() == null ? null : model.moveDirectory().toString(),
                    model.pollInterval().toMillis(),
                    model.maxFileSize(),
                    model.minimumFileAge().toMillis(),
                    model.stabilityCheckInterval().toMillis(),
                    model.stabilityCheckCount(),
                    model.concurrentImports(),
                    model.historyRetention().toMillis(),
                    model.historyFlushInterval().toMillis());
        }

        WatcherConfiguration toModel() {
            if (schemaVersion != SCHEMA_VERSION)
                throw new IllegalArgumentException("Unsupported watcher schema version");
            return new WatcherConfiguration(
                    watcherId,
                    tenantId,
                    WatcherTenantMode.valueOf(tenantMode),
                    autoCreateTenantDirectories,
                    Path.of(watchPath),
                    enabled,
                    recursive,
                    globs,
                    PostImportAction.valueOf(postImportAction),
                    moveDirectory == null ? null : Path.of(moveDirectory),
                    Duration.ofMillis(pollIntervalMillis),
                    maxFileSize,
                    Duration.ofMillis(minimumFileAgeMillis),
                    Duration.ofMillis(stabilityCheckIntervalMillis),
                    stabilityCheckCount,
                    concurrentImports,
                    Duration.ofMillis(historyRetentionMillis),
                    Duration.ofMillis(historyFlushIntervalMillis));
        }
    }

    record OptionsDocument(
            int schemaVersion,
            boolean enabled,
            int maxParallelScans,
            long scanIntervalMillis,
            long historyFlushDebounceMillis,
            long historyRetentionMillis) {

        static OptionsDocument from(WatcherOptions model) {
            return new OptionsDocument(
                    SCHEMA_VERSION,
                    model.enabled(),
                    model.maxParallelScans(),
                    model.scanInterval().toMillis(),
                    model.historyFlushDebounce().toMillis(),
                    model.historyRetention().toMillis());
        }

        WatcherOptions toModel() {
            if (schemaVersion != SCHEMA_VERSION)
                throw new IllegalArgumentException("Unsupported watcher options schema version");
            return new WatcherOptions(
                    enabled,
                    maxParallelScans,
                    Duration.ofMillis(scanIntervalMillis),
                    Duration.ofMillis(historyFlushDebounceMillis),
                    Duration.ofMillis(historyRetentionMillis));
        }
    }

    record RootDocument(
            int schemaVersion,
            String rootPath,
            boolean enabled,
            boolean autoCreateTenantDirectories,
            boolean recursive,
            List<String> globs,
            String postImportAction,
            String moveDirectory) {

        static RootDocument from(WatcherRootConfiguration model) {
            return new RootDocument(
                    SCHEMA_VERSION,
                    model.rootPath().toString(),
                    model.enabled(),
                    model.autoCreateTenantDirectories(),
                    model.recursive(),
                    model.globs(),
                    model.postImportAction().name(),
                    model.moveDirectory() == null ? null : model.moveDirectory().toString());
        }

        WatcherRootConfiguration toModel() {
            if (schemaVersion != SCHEMA_VERSION)
                throw new IllegalArgumentException("Unsupported watcher root schema version");
            return new WatcherRootConfiguration(
                    Path.of(rootPath),
                    enabled,
                    autoCreateTenantDirectories,
                    recursive,
                    globs,
                    PostImportAction.valueOf(postImportAction),
                    moveDirectory == null ? null : Path.of(moveDirectory));
        }
    }

    record ManagedDocument(int schemaVersion, List<String> watcherIds) {
        ManagedDocument {
            watcherIds = List.copyOf(watcherIds);
        }
    }
}
