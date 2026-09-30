package io.github.cocosip.stow.internal.watcher;

import io.github.cocosip.stow.api.ContentSource;
import io.github.cocosip.stow.api.IdempotentStoragePool;
import io.github.cocosip.stow.api.StoragePool;
import io.github.cocosip.stow.api.TenantManager;
import io.github.cocosip.stow.config.SourceCleanupConfiguration;
import io.github.cocosip.stow.exception.TenantDisabledException;
import io.github.cocosip.stow.internal.statistics.NoopStatisticsRecorder;
import io.github.cocosip.stow.internal.statistics.StatisticsRecorder;
import io.github.cocosip.stow.model.MaintenanceError;
import io.github.cocosip.stow.model.TenantContext;
import io.github.cocosip.stow.model.TenantStatus;
import io.github.cocosip.stow.model.WatcherConfiguration;
import io.github.cocosip.stow.model.WatcherScanResult;
import io.github.cocosip.stow.model.WatcherTenantMode;
import io.github.cocosip.stow.model.WriteOptions;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.Iterator;
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
    private final StatisticsRecorder statistics;
    private final SourceCleanupConfiguration cleanupConfiguration;
    private final SourceCleanupStore cleanupStore;
    private final SourceCleanupGate cleanupGate;
    private final SourceFileRelocator relocator = new SourceFileRelocator();

    public WatcherScanner(StoragePool storagePool, TenantManager tenants, ImportedFileHistory history, Clock clock) {
        this(storagePool, tenants, history, clock, new NoopStatisticsRecorder(clock));
    }

    public WatcherScanner(
            StoragePool storagePool,
            TenantManager tenants,
            ImportedFileHistory history,
            Clock clock,
            StatisticsRecorder statistics) {
        this(storagePool, tenants, history, clock, statistics, null, null, null);
    }

    WatcherScanner(
            StoragePool storagePool,
            TenantManager tenants,
            ImportedFileHistory history,
            Clock clock,
            StatisticsRecorder statistics,
            SourceCleanupConfiguration cleanupConfiguration,
            SourceCleanupStore cleanupStore,
            SourceCleanupGate cleanupGate) {
        this.storagePool = Objects.requireNonNull(storagePool, "storagePool");
        this.tenants = Objects.requireNonNull(tenants, "tenants");
        this.history = Objects.requireNonNull(history, "history");
        this.clock = Objects.requireNonNull(clock, "clock");
        this.statistics = Objects.requireNonNull(statistics, "statistics");
        this.cleanupConfiguration = cleanupConfiguration;
        this.cleanupStore = cleanupStore;
        this.cleanupGate = cleanupGate;
    }

    public WatcherScanResult scan(WatcherConfiguration configuration) {
        return scan(configuration, Duration.ZERO);
    }

    /**
     * Runs one scan. The global history-flush debounce (watcher options) takes
     * precedence over the per-watcher flush interval when it is non-zero, making
     * the option live as the minimum interval between history prune passes.
     */
    public WatcherScanResult scan(WatcherConfiguration configuration, Duration globalHistoryFlushDebounce) {
        Objects.requireNonNull(configuration, "configuration");
        Instant started = clock.instant();
        ResultBuilder result = new ResultBuilder(configuration.watcherId(), started);
        Duration pruneThrottle = globalHistoryFlushDebounce == null || globalHistoryFlushDebounce.isZero()
                ? configuration.historyFlushInterval()
                : globalHistoryFlushDebounce;
        history.prune(configuration.watcherId(), configuration.historyRetention(), pruneThrottle);
        statistics.recordWatcherScan(configuration.watcherId());
        try (Stream<Path> candidates = discover(configuration);
                ExecutorService executor = Executors.newFixedThreadPool(
                        configuration.concurrentImports(),
                        Thread.ofVirtual().name("stow-watcher-", 0).factory())) {
            Deque<Future<Outcome>> futures = new ArrayDeque<>(configuration.concurrentImports());
            Iterator<Path> iterator = candidates.iterator();
            while (iterator.hasNext()) {
                if (futures.size() == configuration.concurrentImports()) {
                    result.add(await(futures.removeFirst(), configuration));
                }
                Path candidate = iterator.next();
                result.discoveredOne();
                futures.addLast(executor.submit(() -> process(configuration, candidate)));
            }
            while (!futures.isEmpty()) result.add(await(futures.removeFirst(), configuration));
        } catch (RuntimeException exception) {
            result.error(tenantForError(configuration), "scan", exception);
        }
        result.recordInto(statistics);
        return result.build(clock.instant());
    }

    private Stream<Path> discover(WatcherConfiguration configuration) {
        Path root = configuration.watchPath();
        if (!Files.isDirectory(root, LinkOption.NOFOLLOW_LINKS)) {
            throw new IllegalStateException("Watch path is not a directory: " + root);
        }
        Predicate<Path> matcher = globMatcher(root, configuration);
        Path moveDirectory = configuration.moveDirectory();
        try {
            Stream<Path> paths = configuration.recursive() ? Files.walk(root) : Files.list(root);
            return paths.filter(path -> !path.equals(root))
                    .filter(path -> moveDirectory == null
                            || !path.toAbsolutePath().normalize().startsWith(moveDirectory))
                    .filter(path -> Files.isRegularFile(path, LinkOption.NOFOLLOW_LINKS))
                    .filter(matcher)
                    .filter(path -> eligibleSizeAndAge(path, configuration));
        } catch (IOException exception) {
            throw new IllegalStateException("Unable to scan watch path", exception);
        }
    }

    private Outcome process(WatcherConfiguration configuration, Path source) {
        try {
            StableFile stable = stable(source, configuration);
            if (stable.size() == 0) {
                // Locus skips empty files: a 0-byte source is never a valid import.
                return Outcome.skippedOutcome();
            }
            SourceFingerprint fingerprint = fingerprint(source);
            TenantContext tenant = resolveTenant(configuration, source);
            if (cleanupEnabled()) {
                return processDurably(configuration, source, stable, fingerprint, tenant);
            }
            Optional<ImportedFileHistory.HistoryEntry> previous = history.find(configuration.watcherId(), fingerprint);
            if (previous.isPresent()) {
                ImportedFileHistory.HistoryEntry entry = previous.orElseThrow();
                if (entry.actionCompleted()) return Outcome.skippedOutcome();
                if (entry.isQuarantined()) {
                    // Terminal after the retry budget was exhausted (register C16).
                    return Outcome.quarantinedOutcome();
                }
                if (entry.nextActionAttemptAt() != null && clock.instant().isBefore(entry.nextActionAttemptAt())) {
                    // Backoff window between post-import action retries.
                    return Outcome.skippedOutcome();
                }
                try {
                    applyAction(configuration, source, fingerprint);
                } catch (RuntimeException actionFailure) {
                    return recordActionFailure(configuration, source, fingerprint, entry, actionFailure);
                }
                history.recordActionCompleted(configuration.watcherId(), fingerprint, entry.fileKey());
                return Outcome.retriedOutcome();
            }
            Path sourceFileName = source.getFileName();
            if (sourceFileName == null) {
                throw new IllegalStateException("Watcher source has no file name: " + source);
            }
            String fileKey = storagePool.write(
                    tenant,
                    new PathContentSource(source, stable.size()),
                    WriteOptions.ofOriginalFileName(sourceFileName.toString()));
            statistics.recordWatcherImport(configuration.watcherId(), tenant.tenantId(), stable.size());
            history.recordImported(configuration.watcherId(), fingerprint, fileKey, false);
            try {
                applyAction(configuration, source, fingerprint);
                history.recordActionCompleted(configuration.watcherId(), fingerprint, fileKey);
            } catch (RuntimeException exception) {
                return Outcome.importedAndFailed(
                        stable.size(), tenantForError(configuration), "post-action", exception);
            }
            return Outcome.importedOutcome(stable.size());
        } catch (io.github.cocosip.stow.exception.TenantDisabledException exception) {
            // Disabled tenants are skipped, not failed: importing for them would
            // violate isolation and their files stay for a later re-enable.
            return Outcome.skippedOutcome();
        } catch (UnstableSourceException exception) {
            // A file that changes between stability probes is still being written;
            // Locus treats this as a skip for the next scan to re-evaluate.
            return Outcome.skippedOutcome();
        } catch (UnknownTenantSourceException exception) {
            // Files outside a tenant directory (or under an unknown one) are not
            // import sources; Locus never mints tenants from directory names.
            return Outcome.skippedOutcome();
        } catch (RuntimeException exception) {
            return Outcome.failedOutcome(tenantForError(configuration), "import", exception);
        }
    }

    private Outcome processDurably(
            WatcherConfiguration configuration,
            Path source,
            StableFile stable,
            SourceFingerprint fingerprint,
            TenantContext tenant) {
        SourceCleanupJob requested = SourceCleanupJob.reservation(
                configuration.watcherId(),
                tenant.tenantId(),
                source,
                fingerprint,
                operationId(configuration, fingerprint),
                SourceCleanupAction.valueOf(configuration.postImportAction().name()),
                moveTarget(configuration, source),
                configuration.sourceCleanupFailureDirectory(),
                configuration.maxPostImportActionAttempts(),
                configuration.postImportRetryInitialDelay(),
                configuration.postImportRetryMaxDelay(),
                clock.instant());
        SourceCleanupStore.Reservation reservation = cleanupStore.tryReserve(requested);
        if (reservation.status() == SourceCleanupStore.ReservationStatus.CAPACITY_FULL) {
            return Outcome.deferredOutcome();
        }
        SourceCleanupJob job = reservation.job();
        if (reservation.status() == SourceCleanupStore.ReservationStatus.ALREADY_ACTIVE) {
            boolean staleImport = job.state() == SourceCleanupState.IMPORTING
                    && !job.updatedAt()
                            .plus(cleanupConfiguration.importReservationTimeout())
                            .isAfter(clock.instant());
            if (!staleImport) return Outcome.skippedOutcome();
            job = job.withUpdatedAt(clock.instant());
            cleanupStore.update(job);
        }

        boolean writeCompleted = false;
        try {
            Path fileName = source.getFileName();
            if (fileName == null) throw new IllegalStateException("Watcher source has no file name: " + source);
            WriteOptions writeOptions = WriteOptions.ofOriginalFileName(fileName.toString());
            ContentSource content = new PathContentSource(source, stable.size());
            String fileKey = storagePool instanceof IdempotentStoragePool idempotent
                    ? idempotent.writeIdempotently(tenant, content, writeOptions, job.importOperationId())
                    : storagePool.write(tenant, content, writeOptions);
            if (idempotentWrite() && !Files.exists(source)) {
                // The idempotent write was served from the operation cache, and the
                // source is already gone: a concurrent post-import action handled this
                // file between fingerprinting and the write. This scan re-reserved the
                // finished job; drop it instead of double-counting the import.
                return Outcome.skippedOutcome();
            }
            writeCompleted = true;
            cleanupStore.activate(job.id(), fileKey);
            statistics.recordWatcherImport(configuration.watcherId(), tenant.tenantId(), stable.size());
            return Outcome.importedOutcome(stable.size());
        } finally {
            if (!writeCompleted && reservation.status() == SourceCleanupStore.ReservationStatus.RESERVED) {
                cleanupStore.remove(job.id());
            }
        }
    }

    private boolean idempotentWrite() {
        return storagePool instanceof IdempotentStoragePool;
    }

    /**
     * Legacy-path post-import failure handling (register C16): exponential backoff
     * capped at the configured maximum, a retry budget, and quarantine into the
     * failure directory once the budget is exhausted. A failed quarantine move still
     * marks the source terminal, like Locus.
     */
    private Outcome recordActionFailure(
            WatcherConfiguration configuration,
            Path source,
            SourceFingerprint fingerprint,
            ImportedFileHistory.HistoryEntry entry,
            RuntimeException actionFailure) {
        int attempts = entry.attempts() + 1;
        if (attempts >= configuration.maxPostImportActionAttempts()) {
            quarantine(configuration, source, fingerprint);
            history.recordQuarantined(configuration.watcherId(), entry);
            return Outcome.quarantinedOutcome();
        }
        long backoffMillis = configuration
                .postImportRetryInitialDelay()
                .multipliedBy(1L << Math.min(attempts - 1, 16))
                .toMillis();
        if (backoffMillis < 0
                || backoffMillis > configuration.postImportRetryMaxDelay().toMillis()) {
            backoffMillis = configuration.postImportRetryMaxDelay().toMillis();
        }
        history.recordActionRetry(
                configuration.watcherId(), entry, attempts, clock.instant().plusMillis(backoffMillis));
        return Outcome.failedOutcome(tenantForError(configuration), "post-action", actionFailure);
    }

    private void quarantine(WatcherConfiguration configuration, Path source, SourceFingerprint fingerprint) {
        Path failureDirectory = configuration.sourceCleanupFailureDirectory();
        if (failureDirectory == null || source.getFileName() == null) return;
        Path target = failureDirectory.resolve(configuration.watcherId()).resolve(source.getFileName());
        try {
            Path targetParent = target.getParent();
            if (targetParent == null) return;
            Files.createDirectories(targetParent);
            // Fingerprint-guarded move: if the source changed after the failed action,
            // the mismatch leaves it in place and the terminal flag alone stops retries.
            relocator.moveIfMatching(source, target, fingerprint);
        } catch (IOException | RuntimeException ignored) {
            // A failed quarantine becomes terminal via the history flag; the next
            // scan will not retry the action either way.
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
                    throw new UnstableSourceException("File is not stable: " + source.getFileName());
                }
            }
            return new StableFile(size, modified);
        } catch (UnstableSourceException exception) {
            throw exception;
        } catch (IOException exception) {
            throw new IllegalStateException("Unable to inspect source file", exception);
        }
    }

    private TenantContext resolveTenant(WatcherConfiguration configuration, Path source) {
        String tenantId = configuration.tenantId();
        if (configuration.tenantMode() == WatcherTenantMode.SUBDIRECTORY_TENANTS) {
            Path relative = configuration.watchPath().relativize(source);
            if (relative.getNameCount() < 2) {
                throw new UnknownTenantSourceException("Source is not under a tenant directory");
            }
            tenantId = relative.getName(0).toString();
        }
        Optional<TenantContext> existing = tenants.find(tenantId);
        if (existing.isPresent()) {
            TenantContext tenant = existing.orElseThrow();
            // A disabled tenant rejects every operation; importing for it would both
            // violate the isolation contract and consume its quota.
            if (tenant.status() != TenantStatus.ENABLED) {
                throw new TenantDisabledException("Tenant is disabled: " + tenantId);
            }
            return tenant;
        }
        // In multi-tenant mode an unknown directory is not an import source: tenants
        // are never minted from directory names (Locus-aligned, register C9).
        if (configuration.tenantMode() == WatcherTenantMode.SUBDIRECTORY_TENANTS
                || !configuration.autoCreateTenantDirectories()) {
            throw new UnknownTenantSourceException("Tenant does not exist: " + tenantId);
        }
        return tenants.create(tenantId);
    }

    private void applyAction(WatcherConfiguration configuration, Path source, SourceFingerprint fingerprint) {
        try {
            SourceFileRelocator.Status status =
                    switch (configuration.postImportAction()) {
                        case KEEP -> SourceFileRelocator.Status.COMPLETED;
                        case DELETE -> relocator.deleteIfMatching(source, fingerprint);
                        case MOVE ->
                            relocator
                                    .moveIfMatching(source, moveTarget(configuration, source), fingerprint)
                                    .status();
                    };
            if (status == SourceFileRelocator.Status.FINGERPRINT_MISMATCH) {
                throw new IllegalStateException("Source changed before post-import action");
            }
        } catch (IOException exception) {
            throw new IllegalStateException("Unable to apply imported source action", exception);
        }
    }

    private static Path moveTarget(WatcherConfiguration configuration, Path source) {
        if (configuration.postImportAction() != io.github.cocosip.stow.model.PostImportAction.MOVE) return null;
        Path targetRoot = Objects.requireNonNull(configuration.moveDirectory(), "moveDirectory")
                .toAbsolutePath()
                .normalize();
        Path relative = configuration
                .watchPath()
                .toAbsolutePath()
                .normalize()
                .relativize(source.toAbsolutePath().normalize());
        Path target = targetRoot.resolve(relative).normalize();
        if (!target.startsWith(targetRoot)) throw new IllegalStateException("Move target escapes move directory");
        return target;
    }

    private boolean cleanupEnabled() {
        return cleanupConfiguration != null && cleanupStore != null && cleanupGate != null && cleanupGate.enabled();
    }

    private static String operationId(WatcherConfiguration configuration, SourceFingerprint fingerprint) {
        return configuration.watcherId() + ":" + fingerprint.sampleSha256();
    }

    private static SourceFingerprint fingerprint(Path source) {
        try {
            return SourceFingerprint.capture(source);
        } catch (IOException exception) {
            throw new IllegalStateException("Unable to fingerprint source file", exception);
        }
    }

    private static Outcome await(Future<Outcome> future, WatcherConfiguration configuration) {
        try {
            return future.get();
        } catch (InterruptedException exception) {
            Thread.currentThread().interrupt();
            return Outcome.failedOutcome(tenantForError(configuration), "scan", exception);
        } catch (ExecutionException exception) {
            Throwable cause = exception.getCause() == null ? exception : exception.getCause();
            return Outcome.failedOutcome(tenantForError(configuration), "import", cause);
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

    /** Marks a source whose bytes changed between stability probes; treated as a skip. */
    private static final class UnstableSourceException extends RuntimeException {

        @java.io.Serial
        private static final long serialVersionUID = 1L;

        private UnstableSourceException(String message) {
            super(message);
        }
    }

    /** Marks a source that has no tenant to import for; treated as a skip, not a failure. */
    private static final class UnknownTenantSourceException extends RuntimeException {

        @java.io.Serial
        private static final long serialVersionUID = 1L;

        private UnknownTenantSourceException(String message) {
            super(message);
        }
    }

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

    private record Outcome(
            long imported,
            long skipped,
            long failed,
            long bytes,
            long retried,
            long quarantined,
            long deferred,
            MaintenanceError error) {
        static Outcome importedOutcome(long bytes) {
            return new Outcome(1, 0, 0, bytes, 0, 0, 0, null);
        }

        static Outcome skippedOutcome() {
            return new Outcome(0, 1, 0, 0, 0, 0, 0, null);
        }

        static Outcome retriedOutcome() {
            return new Outcome(0, 1, 0, 0, 1, 0, 0, null);
        }

        static Outcome deferredOutcome() {
            return new Outcome(0, 1, 0, 0, 0, 0, 1, null);
        }

        static Outcome quarantinedOutcome() {
            return new Outcome(0, 1, 0, 0, 0, 1, 0, null);
        }

        static Outcome failedOutcome(String tenantId, String operation, Throwable exception) {
            String summary =
                    exception.getMessage() == null ? exception.getClass().getSimpleName() : exception.getMessage();
            return new Outcome(0, 0, 1, 0, 0, 0, 0, new MaintenanceError(tenantId, operation, summary));
        }

        static Outcome importedAndFailed(long bytes, String tenantId, String operation, Throwable exception) {
            String summary =
                    exception.getMessage() == null ? exception.getClass().getSimpleName() : exception.getMessage();
            return new Outcome(1, 0, 1, bytes, 0, 0, 0, new MaintenanceError(tenantId, operation, summary));
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
        private long retried;
        private long quarantined;
        private long deferred;
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
            retried += outcome.retried();
            quarantined += outcome.quarantined();
            deferred += outcome.deferred();
            if (outcome.error() != null) errors.add(outcome.error());
        }

        private synchronized void discoveredOne() {
            discovered++;
        }

        private synchronized void error(String tenantId, String operation, Throwable exception) {
            failed++;
            String summary =
                    exception.getMessage() == null ? exception.getClass().getSimpleName() : exception.getMessage();
            errors.add(new MaintenanceError(tenantId, operation, summary));
        }

        private synchronized WatcherScanResult build(Instant finishedAt) {
            return new WatcherScanResult(
                    watcherId,
                    startedAt,
                    finishedAt,
                    discovered,
                    imported,
                    skipped,
                    failed,
                    bytes,
                    retried,
                    quarantined,
                    deferred,
                    errors);
        }

        private void recordInto(StatisticsRecorder statistics) {
            long discoveredTotal;
            long skippedTotal;
            long failedTotal;
            synchronized (this) {
                discoveredTotal = discovered;
                skippedTotal = skipped;
                failedTotal = failed;
            }
            statistics.recordWatcherScanFiles(
                    watcherId, StatisticsRecorder.WatcherFileOutcome.DISCOVERED, discoveredTotal);
            statistics.recordWatcherScanFiles(watcherId, StatisticsRecorder.WatcherFileOutcome.SKIPPED, skippedTotal);
            statistics.recordWatcherScanFiles(watcherId, StatisticsRecorder.WatcherFileOutcome.FAILED, failedTotal);
        }
    }
}
