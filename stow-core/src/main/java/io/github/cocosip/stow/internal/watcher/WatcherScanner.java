package io.github.cocosip.stow.internal.watcher;

import io.github.cocosip.stow.api.ContentSource;
import io.github.cocosip.stow.api.StoragePool;
import io.github.cocosip.stow.api.TenantManager;
import io.github.cocosip.stow.model.MaintenanceError;
import io.github.cocosip.stow.model.TenantContext;
import io.github.cocosip.stow.model.WatcherConfiguration;
import io.github.cocosip.stow.model.WatcherScanResult;
import io.github.cocosip.stow.model.WatcherTenantMode;
import io.github.cocosip.stow.model.WriteOptions;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.FileStore;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.function.Predicate;
import java.util.stream.Stream;

/** Performs a bounded polling scan and imports only stable, matching files. */
public final class WatcherScanner {

    private final StoragePool storagePool;
    private final TenantManager tenants;
    private final ImportedFileHistory history;
    private final Clock clock;

    public WatcherScanner(StoragePool storagePool, TenantManager tenants, ImportedFileHistory history, Clock clock) {
        this.storagePool = Objects.requireNonNull(storagePool, "storagePool");
        this.tenants = Objects.requireNonNull(tenants, "tenants");
        this.history = Objects.requireNonNull(history, "history");
        this.clock = Objects.requireNonNull(clock, "clock");
    }

    public WatcherScanResult scan(WatcherConfiguration configuration) {
        Objects.requireNonNull(configuration, "configuration");
        Instant started = clock.instant();
        ResultBuilder result = new ResultBuilder(configuration.watcherId(), started);
        List<Path> candidates;
        try {
            candidates = discover(configuration);
        } catch (RuntimeException exception) {
            result.error(tenantForError(configuration), "scan", exception);
            return result.build(clock.instant());
        }
        history.prune(
                configuration.watcherId(), configuration.historyRetention(), configuration.historyFlushInterval());
        result.discovered(candidates.size());
        try (ExecutorService executor = Executors.newFixedThreadPool(
                configuration.concurrentImports(),
                Thread.ofVirtual().name("stow-watcher-", 0).factory())) {
            List<Future<Outcome>> futures = candidates.stream()
                    .map(path -> executor.submit(() -> process(configuration, path)))
                    .toList();
            for (Future<Outcome> future : futures) {
                try {
                    result.add(future.get());
                } catch (InterruptedException exception) {
                    Thread.currentThread().interrupt();
                    result.error(tenantForError(configuration), "scan", exception);
                } catch (ExecutionException exception) {
                    Throwable cause = exception.getCause() == null ? exception : exception.getCause();
                    result.error(tenantForError(configuration), "import", cause);
                }
            }
        }
        return result.build(clock.instant());
    }

    private List<Path> discover(WatcherConfiguration configuration) {
        Path root = configuration.watchPath();
        if (!Files.isDirectory(root, LinkOption.NOFOLLOW_LINKS)) {
            throw new IllegalStateException("Watch path is not a directory: " + root);
        }
        Predicate<Path> matcher = globMatcher(root, configuration);
        Path moveDirectory = configuration.moveDirectory();
        try (Stream<Path> paths = configuration.recursive() ? Files.walk(root) : Files.list(root)) {
            return paths.filter(path -> !path.equals(root))
                    .filter(path -> moveDirectory == null
                            || !path.toAbsolutePath().normalize().startsWith(moveDirectory))
                    .filter(path -> Files.isRegularFile(path, LinkOption.NOFOLLOW_LINKS))
                    .filter(matcher)
                    .filter(path -> eligibleSizeAndAge(path, configuration))
                    .sorted()
                    .toList();
        } catch (IOException exception) {
            throw new IllegalStateException("Unable to scan watch path", exception);
        }
    }

    private Outcome process(WatcherConfiguration configuration, Path source) {
        try {
            StableFile stable = stable(source, configuration);
            TenantContext tenant = resolveTenant(configuration, source);
            Optional<ImportedFileHistory.HistoryEntry> previous =
                    history.find(configuration.watcherId(), source, stable.size(), stable.modifiedAtMillis());
            if (previous.isPresent()) {
                ImportedFileHistory.HistoryEntry entry = previous.orElseThrow();
                if (entry.actionCompleted()) return Outcome.skippedOutcome();
                applyAction(configuration, source);
                history.recordActionCompleted(
                        configuration.watcherId(), source, stable.size(), stable.modifiedAtMillis(), entry.fileKey());
                return Outcome.skippedOutcome();
            }
            Path sourceFileName = source.getFileName();
            if (sourceFileName == null) {
                throw new IllegalStateException("Watcher source has no file name: " + source);
            }
            String fileKey = storagePool.write(
                    tenant,
                    new PathContentSource(source, stable.size()),
                    WriteOptions.ofOriginalFileName(sourceFileName.toString()));
            history.recordImported(
                    configuration.watcherId(), source, stable.size(), stable.modifiedAtMillis(), fileKey, false);
            try {
                applyAction(configuration, source);
                history.recordActionCompleted(
                        configuration.watcherId(), source, stable.size(), stable.modifiedAtMillis(), fileKey);
            } catch (RuntimeException exception) {
                return Outcome.importedAndFailed(
                        stable.size(), tenantForError(configuration), "post-action", exception);
            }
            return Outcome.importedOutcome(stable.size());
        } catch (RuntimeException exception) {
            return Outcome.failedOutcome(tenantForError(configuration), "import", exception);
        }
    }

    private StableFile stable(Path source, WatcherConfiguration configuration) {
        try {
            var first = Files.readAttributes(
                    source, java.nio.file.attribute.BasicFileAttributes.class, LinkOption.NOFOLLOW_LINKS);
            long size = first.size();
            long modified = first.lastModifiedTime().toMillis();
            for (int count = 1; count < configuration.stabilityCheckCount(); count++) {
                sleep(configuration.stabilityCheckInterval());
                var next = Files.readAttributes(
                        source, java.nio.file.attribute.BasicFileAttributes.class, LinkOption.NOFOLLOW_LINKS);
                if (next.size() != size || next.lastModifiedTime().toMillis() != modified) {
                    throw new IllegalStateException("File is not stable: " + source.getFileName());
                }
            }
            return new StableFile(size, modified);
        } catch (IOException exception) {
            throw new IllegalStateException("Unable to inspect source file", exception);
        }
    }

    private TenantContext resolveTenant(WatcherConfiguration configuration, Path source) {
        String tenantId = configuration.tenantId();
        if (configuration.tenantMode() == WatcherTenantMode.SUBDIRECTORY_TENANTS) {
            Path relative = configuration.watchPath().relativize(source);
            if (relative.getNameCount() < 2) throw new IllegalStateException("Source is not under a tenant directory");
            tenantId = relative.getName(0).toString();
        }
        Optional<TenantContext> existing = tenants.find(tenantId);
        if (existing.isPresent()) return existing.orElseThrow();
        if (!configuration.autoCreateTenantDirectories()) {
            throw new IllegalStateException("Tenant does not exist: " + tenantId);
        }
        return tenants.create(tenantId);
    }

    private void applyAction(WatcherConfiguration configuration, Path source) {
        switch (configuration.postImportAction()) {
            case KEEP -> {}
            case DELETE -> {
                try {
                    Files.deleteIfExists(source);
                } catch (IOException exception) {
                    throw new IllegalStateException("Unable to delete imported source", exception);
                }
            }
            case MOVE -> move(configuration, source);
        }
    }

    private void move(WatcherConfiguration configuration, Path source) {
        Path root = configuration.watchPath().toAbsolutePath().normalize();
        Path configuredTarget = configuration.moveDirectory();
        if (configuredTarget == null) throw new IllegalStateException("Move action requires a move directory");
        Path targetRoot = configuredTarget.toAbsolutePath().normalize();
        Path relative = root.relativize(source.toAbsolutePath().normalize());
        Path target = targetRoot.resolve(relative).normalize();
        if (!target.startsWith(targetRoot)) throw new IllegalStateException("Move target escapes move directory");
        Path targetParent = target.getParent();
        if (targetParent == null) throw new IllegalStateException("Move target has no parent: " + target);
        try {
            Files.createDirectories(targetParent);
            FileStore sourceStore = Files.getFileStore(source);
            FileStore targetStore = Files.getFileStore(targetParent);
            if (!sourceStore.equals(targetStore))
                throw new IllegalStateException("Move directory is on another file system");
            Files.move(source, target, StandardCopyOption.ATOMIC_MOVE);
        } catch (IOException exception) {
            throw new IllegalStateException("Unable to move imported source", exception);
        }
    }

    private boolean eligibleSizeAndAge(Path path, WatcherConfiguration configuration) {
        try {
            var attributes = Files.readAttributes(
                    path, java.nio.file.attribute.BasicFileAttributes.class, LinkOption.NOFOLLOW_LINKS);
            return (configuration.maxFileSize() == 0 || attributes.size() <= configuration.maxFileSize())
                    && !attributes
                            .lastModifiedTime()
                            .toInstant()
                            .isAfter(clock.instant().minus(configuration.minimumFileAge()));
        } catch (IOException exception) {
            return false;
        }
    }

    private Predicate<Path> globMatcher(Path root, WatcherConfiguration configuration) {
        List<GlobMatcher> matchers = configuration.globs().stream()
                .map(glob -> new GlobMatcher(
                        root.getFileSystem().getPathMatcher("glob:" + glob),
                        glob.startsWith("**/")
                                ? root.getFileSystem().getPathMatcher("glob:" + glob.substring(3))
                                : null))
                .toList();
        return path -> {
            Path relative = root.relativize(path);
            return matchers.stream().anyMatch(matcher -> matcher.matches(relative));
        };
    }

    private record GlobMatcher(java.nio.file.PathMatcher full, java.nio.file.PathMatcher fileName) {
        private boolean matches(Path relative) {
            return full.matches(relative) || (fileName != null && fileName.matches(relative.getFileName()));
        }
    }

    private static void sleep(Duration duration) {
        if (duration.isZero()) return;
        try {
            Thread.sleep(duration);
        } catch (InterruptedException exception) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Watcher stability check interrupted", exception);
        }
    }

    private static String tenantForError(WatcherConfiguration configuration) {
        return configuration.tenantMode() == WatcherTenantMode.SINGLE_TENANT ? configuration.tenantId() : null;
    }

    private record StableFile(long size, long modifiedAtMillis) {}

    private record PathContentSource(Path path, long size) implements ContentSource {
        @Override
        public InputStream openStream() {
            try {
                return Files.newInputStream(path);
            } catch (IOException exception) {
                throw new IllegalStateException("Unable to open watcher source", exception);
            }
        }

        @Override
        public java.util.OptionalLong length() {
            return java.util.OptionalLong.of(size);
        }

        @Override
        public boolean repeatable() {
            return true;
        }
    }

    private record Outcome(long imported, long skipped, long failed, long bytes, MaintenanceError error) {
        static Outcome importedOutcome(long bytes) {
            return new Outcome(1, 0, 0, bytes, null);
        }

        static Outcome skippedOutcome() {
            return new Outcome(0, 1, 0, 0, null);
        }

        static Outcome failedOutcome(String tenantId, String operation, Throwable exception) {
            String summary =
                    exception.getMessage() == null ? exception.getClass().getSimpleName() : exception.getMessage();
            return new Outcome(0, 0, 1, 0, new MaintenanceError(tenantId, operation, summary));
        }

        static Outcome importedAndFailed(long bytes, String tenantId, String operation, Throwable exception) {
            String summary =
                    exception.getMessage() == null ? exception.getClass().getSimpleName() : exception.getMessage();
            return new Outcome(1, 0, 1, bytes, new MaintenanceError(tenantId, operation, summary));
        }
    }

    private static final class ResultBuilder {
        private final String watcherId;
        private final Instant startedAt;
        private long discovered;
        private long imported;
        private long skipped;
        private long failed;
        private long bytes;
        private final List<MaintenanceError> errors = new ArrayList<>();

        private ResultBuilder(String watcherId, Instant startedAt) {
            this.watcherId = watcherId;
            this.startedAt = startedAt;
        }

        private synchronized void add(Outcome outcome) {
            imported += outcome.imported();
            skipped += outcome.skipped();
            failed += outcome.failed();
            bytes += outcome.bytes();
            if (outcome.error() != null) errors.add(outcome.error());
        }

        private synchronized void discovered(long count) {
            discovered = count;
        }

        private synchronized void error(String tenantId, String operation, Throwable exception) {
            failed++;
            String summary =
                    exception.getMessage() == null ? exception.getClass().getSimpleName() : exception.getMessage();
            errors.add(new MaintenanceError(tenantId, operation, summary));
        }

        private synchronized WatcherScanResult build(Instant finishedAt) {
            return new WatcherScanResult(
                    watcherId, startedAt, finishedAt, discovered, imported, skipped, failed, bytes, errors);
        }
    }
}
