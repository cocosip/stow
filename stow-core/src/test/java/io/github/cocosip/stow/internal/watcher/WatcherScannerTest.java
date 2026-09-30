package io.github.cocosip.stow.internal.watcher;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.github.cocosip.stow.api.ContentSource;
import io.github.cocosip.stow.api.IdempotentStoragePool;
import io.github.cocosip.stow.api.StoragePool;
import io.github.cocosip.stow.api.TenantManager;
import io.github.cocosip.stow.config.SourceCleanupConfiguration;
import io.github.cocosip.stow.internal.statistics.StatisticsRecorder;
import io.github.cocosip.stow.model.PostImportAction;
import io.github.cocosip.stow.model.TenantContext;
import io.github.cocosip.stow.model.TenantStatus;
import io.github.cocosip.stow.model.WatcherConfiguration;
import io.github.cocosip.stow.model.WatcherScanResult;
import io.github.cocosip.stow.model.WatcherTenantMode;
import io.github.cocosip.stow.model.WriteOptions;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.FileTime;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

class WatcherScannerTest {

    private static final Clock CLOCK = Clock.systemUTC();

    @Test
    void filtersGlobRecursionAndSizeAndAppliesDeleteMoveAndKeep() throws Exception {
        Path root = Files.createTempDirectory(Path.of("target"), "watcher-scan-");
        Path input = root.resolve("input");
        Path nested = input.resolve("nested");
        Files.createDirectories(nested);
        Path top = input.resolve("top.dcm");
        Path deep = nested.resolve("deep.dcm");
        Path tooLarge = input.resolve("large.dcm");
        Files.writeString(top, "top");
        Files.writeString(deep, "deep");
        Files.writeString(tooLarge, "0123456789");
        StoragePool pool = mock(StoragePool.class);
        when(pool.write(any(TenantContext.class), any(ContentSource.class), any()))
                .thenReturn("0123456789abcdef0123456789abcdef");
        TenantManager tenants = mock(TenantManager.class);
        when(tenants.find("tenant-a")).thenReturn(Optional.of(context("tenant-a")));
        ImportedFileHistory history = new ImportedFileHistory(root.resolve("history"), CLOCK);
        WatcherScanner scanner = new WatcherScanner(pool, tenants, history, CLOCK);

        WatcherConfiguration delete = configuration(input, PostImportAction.DELETE, null, true, 5);
        WatcherScanResult deleted = scanner.scan(delete);
        assertThat(deleted.discoveredCount()).isEqualTo(2);
        assertThat(deleted.importedCount()).isEqualTo(2);
        assertThat(top).doesNotExist();
        assertThat(deep).doesNotExist();
        assertThat(tooLarge).exists();

        Path kept = input.resolve("kept.dcm");
        Files.writeString(kept, "keep");
        WatcherConfiguration keep = configuration(input, PostImportAction.KEEP, null, false, 5);
        WatcherScanResult keptResult = scanner.scan(keep);
        assertThat(keptResult.importedCount()).isEqualTo(1);
        assertThat(kept).exists();

        Path moved = input.resolve("moved.dcm");
        Files.writeString(moved, "move");
        Path destination = root.resolve("done");
        WatcherScanResult movedResult =
                scanner.scan(configuration(input, PostImportAction.MOVE, destination, false, 5));
        assertThat(movedResult.importedCount()).isEqualTo(1);
        assertThat(destination.resolve("moved.dcm")).exists();
    }

    @Test
    void retriesOnlyPostActionAfterHistoryWasWritten() throws Exception {
        Path root = Files.createTempDirectory(Path.of("target"), "watcher-retry-");
        Path input = root.resolve("input");
        Files.createDirectories(input);
        Path source = input.resolve("retry.dcm");
        Files.writeString(source, "retry");
        Path invalidDestination = root.resolve("not-a-directory");
        Files.writeString(invalidDestination, "block");
        StoragePool pool = mock(StoragePool.class);
        when(pool.write(any(TenantContext.class), any(ContentSource.class), any()))
                .thenReturn("fedcba9876543210fedcba9876543210");
        TenantManager tenants = mock(TenantManager.class);
        when(tenants.find("tenant-a")).thenReturn(Optional.of(context("tenant-a")));
        ImportedFileHistory history = new ImportedFileHistory(root.resolve("history"), CLOCK);
        WatcherScanner scanner = new WatcherScanner(pool, tenants, history, CLOCK);
        WatcherConfiguration failedMove = configuration(input, PostImportAction.MOVE, invalidDestination, false, 0);

        WatcherScanResult first = scanner.scan(failedMove);
        assertThat(first.importedCount()).isEqualTo(1);
        assertThat(first.failedCount()).isEqualTo(1);
        Path destination = root.resolve("done");
        WatcherScanResult retried = scanner.scan(configuration(input, PostImportAction.MOVE, destination, false, 0));
        assertThat(retried.importedCount()).isEqualTo(0);
        assertThat(retried.skippedCount()).isEqualTo(1);
        assertThat(destination.resolve("retry.dcm")).exists();
        verify(pool, times(1)).write(any(TenantContext.class), any(ContentSource.class), any());
    }

    @Test
    void supportsSubdirectoryTenantsAndBoundsConcurrentImports() throws Exception {
        Path root = Files.createTempDirectory(Path.of("target"), "watcher-multi-");
        Path input = root.resolve("input");
        Files.createDirectories(input.resolve("tenant-a"));
        Files.createDirectories(input.resolve("tenant-b"));
        Files.writeString(input.resolve("tenant-a").resolve("a.dcm"), "a");
        Files.writeString(input.resolve("tenant-b").resolve("b.dcm"), "b");
        StoragePool pool = mock(StoragePool.class);
        AtomicInteger active = new AtomicInteger();
        AtomicInteger maximum = new AtomicInteger();
        doAnswer(invocation -> {
                    int now = active.incrementAndGet();
                    maximum.accumulateAndGet(now, Math::max);
                    Thread.sleep(25);
                    active.decrementAndGet();
                    return "0123456789abcdef0123456789abcdef";
                })
                .when(pool)
                .write(any(TenantContext.class), any(ContentSource.class), any());
        TenantManager tenants = mock(TenantManager.class);
        when(tenants.find(anyString())).thenAnswer(invocation -> Optional.of(context(invocation.getArgument(0))));
        WatcherScanner scanner =
                new WatcherScanner(pool, tenants, new ImportedFileHistory(root.resolve("history"), CLOCK), CLOCK);
        WatcherConfiguration configuration = new WatcherConfiguration(
                "watcher-multi",
                null,
                WatcherTenantMode.SUBDIRECTORY_TENANTS,
                false,
                input,
                true,
                true,
                List.of("**/*.dcm"),
                PostImportAction.KEEP,
                null,
                Duration.ZERO,
                0,
                Duration.ZERO,
                Duration.ZERO,
                1,
                1,
                Duration.ofDays(1),
                Duration.ZERO);

        assertThat(scanner.scan(configuration).importedCount()).isEqualTo(2);
        assertThat(maximum).hasValue(1);
    }

    @Test
    void reservesBeforeIdempotentWriteAndActivatesBeforeStatistics() throws Exception {
        Path root = Files.createTempDirectory(Path.of("target"), "watcher-durable-");
        Path input = root.resolve("input");
        Files.createDirectories(input);
        Path source = input.resolve("image.dcm");
        Files.writeString(source, "data");
        StoragePool pool = mock(
                StoragePool.class, org.mockito.Mockito.withSettings().extraInterfaces(IdempotentStoragePool.class));
        IdempotentStoragePool idempotent = (IdempotentStoragePool) pool;
        when(idempotent.writeIdempotently(any(), any(), any(), anyString())).thenReturn("file-a");
        TenantManager tenants = tenants();
        SourceCleanupStore store = mock(SourceCleanupStore.class);
        when(store.tryReserve(any())).thenAnswer(invocation -> {
            SourceCleanupJob requested = invocation.getArgument(0);
            return new SourceCleanupStore.Reservation(
                    SourceCleanupStore.ReservationStatus.RESERVED, withId(requested, 7));
        });
        StatisticsRecorder statistics = mock(StatisticsRecorder.class);
        WatcherScanner scanner = cleanupScanner(root, pool, tenants, store, statistics);

        WatcherScanResult result = scanner.scan(configuration(input, PostImportAction.DELETE, null, false, 0));

        assertThat(result.importedCount()).isEqualTo(1);
        assertThat(source).exists();
        var order = inOrder(store, pool, statistics);
        order.verify(store).tryReserve(any());
        order.verify(idempotent).writeIdempotently(any(), any(), any(), anyString());
        order.verify(store).activate(7, "file-a");
        order.verify(statistics).recordWatcherImport("watcher-a", "tenant-a", 4);
    }

    @Test
    void defersImportWithoutStorageWriteWhenCleanupCapacityIsFull() throws Exception {
        Path root = Files.createTempDirectory(Path.of("target"), "watcher-capacity-");
        Path input = root.resolve("input");
        Files.createDirectories(input);
        Files.writeString(input.resolve("image.dcm"), "data");
        StoragePool pool = mock(
                StoragePool.class, org.mockito.Mockito.withSettings().extraInterfaces(IdempotentStoragePool.class));
        SourceCleanupStore store = mock(SourceCleanupStore.class);
        when(store.tryReserve(any()))
                .thenReturn(
                        new SourceCleanupStore.Reservation(SourceCleanupStore.ReservationStatus.CAPACITY_FULL, null));
        WatcherScanner scanner = cleanupScanner(root, pool, tenants(), store, mock(StatisticsRecorder.class));

        WatcherScanResult result = scanner.scan(configuration(input, PostImportAction.DELETE, null, false, 0));

        assertThat(result.importedCount()).isZero();
        assertThat(result.importsDeferred()).isEqualTo(1);
        verify((IdempotentStoragePool) pool, never()).writeIdempotently(any(), any(), any(), anyString());
    }

    @Test
    void keepsReservationAfterStorageWriteWhenActivationFails() throws Exception {
        Path root = Files.createTempDirectory(Path.of("target"), "watcher-activation-");
        Path input = root.resolve("input");
        Files.createDirectories(input);
        Files.writeString(input.resolve("image.dcm"), "data");
        StoragePool pool = mock(
                StoragePool.class, org.mockito.Mockito.withSettings().extraInterfaces(IdempotentStoragePool.class));
        when(((IdempotentStoragePool) pool).writeIdempotently(any(), any(), any(), anyString()))
                .thenReturn("file-a");
        SourceCleanupStore store = mock(SourceCleanupStore.class);
        when(store.tryReserve(any()))
                .thenAnswer(invocation -> new SourceCleanupStore.Reservation(
                        SourceCleanupStore.ReservationStatus.RESERVED, withId(invocation.getArgument(0), 7)));
        when(store.activate(7, "file-a")).thenThrow(new IllegalStateException("activation failed"));
        WatcherScanner scanner = cleanupScanner(root, pool, tenants(), store, mock(StatisticsRecorder.class));

        WatcherScanResult result = scanner.scan(configuration(input, PostImportAction.DELETE, null, false, 0));

        assertThat(result.failedCount()).isEqualTo(1);
        verify(store, never()).remove(7);
    }

    @Test
    void activationPrecedesStatisticsEvenWhenStatisticsFails() throws Exception {
        Path root = Files.createTempDirectory(Path.of("target"), "watcher-statistics-");
        Path input = root.resolve("input");
        Files.createDirectories(input);
        Files.writeString(input.resolve("image.dcm"), "data");
        StoragePool pool = mock(
                StoragePool.class, org.mockito.Mockito.withSettings().extraInterfaces(IdempotentStoragePool.class));
        when(((IdempotentStoragePool) pool).writeIdempotently(any(), any(), any(), anyString()))
                .thenReturn("file-a");
        SourceCleanupStore store = mock(SourceCleanupStore.class);
        when(store.tryReserve(any()))
                .thenAnswer(invocation -> new SourceCleanupStore.Reservation(
                        SourceCleanupStore.ReservationStatus.RESERVED, withId(invocation.getArgument(0), 7)));
        StatisticsRecorder statistics = mock(StatisticsRecorder.class);
        when(statistics.recordWatcherImport(anyString(), anyString(), anyLong()))
                .thenThrow(new IllegalStateException("statistics failed"));
        WatcherScanner scanner = cleanupScanner(root, pool, tenants(), store, statistics);

        WatcherScanResult result = scanner.scan(configuration(input, PostImportAction.DELETE, null, false, 0));

        assertThat(result.failedCount()).isEqualTo(1);
        var order = inOrder(store, statistics);
        order.verify(store).activate(7, "file-a");
        order.verify(statistics).recordWatcherImport("watcher-a", "tenant-a", 4);
        verify(store, never()).remove(7);
    }

    @Test
    void strongHistoryDoesNotSuppressChangedContentWithReusedSizeAndTimestamp() throws Exception {
        Path root = Files.createTempDirectory(Path.of("target"), "watcher-history-strong-");
        Path input = root.resolve("input");
        Files.createDirectories(input);
        Path source = input.resolve("image.dcm");
        Files.writeString(source, "aaaa");
        FileTime timestamp = Files.getLastModifiedTime(source);
        StoragePool pool = mock(StoragePool.class);
        when(pool.write(any(TenantContext.class), any(ContentSource.class), any(WriteOptions.class)))
                .thenReturn("file-a", "file-b");
        WatcherScanner scanner =
                new WatcherScanner(pool, tenants(), new ImportedFileHistory(root.resolve("history"), CLOCK), CLOCK);
        WatcherConfiguration keep = configuration(input, PostImportAction.KEEP, null, false, 0);

        assertThat(scanner.scan(keep).importedCount()).isEqualTo(1);
        Files.writeString(source, "bbbb");
        Files.setLastModifiedTime(source, timestamp);

        assertThat(scanner.scan(keep).importedCount()).isEqualTo(1);
        verify(pool, times(2)).write(any(TenantContext.class), any(ContentSource.class), any(WriteOptions.class));
    }

    private WatcherScanner cleanupScanner(
            Path root,
            StoragePool pool,
            TenantManager tenants,
            SourceCleanupStore store,
            StatisticsRecorder statistics) {
        SourceCleanupConfiguration cleanup = new SourceCleanupConfiguration(
                true,
                root.resolve("source-cleanup.db"),
                Duration.ofSeconds(1),
                2,
                100,
                Duration.ofDays(1),
                Duration.ofMinutes(10),
                false,
                Duration.ofDays(1),
                50);
        SourceCleanupGate gate = mock(SourceCleanupGate.class);
        when(gate.enabled()).thenReturn(true);
        return new WatcherScanner(
                pool,
                tenants,
                new ImportedFileHistory(root.resolve("history"), CLOCK),
                CLOCK,
                statistics,
                cleanup,
                store,
                gate);
    }

    private static TenantManager tenants() {
        TenantManager tenants = mock(TenantManager.class);
        when(tenants.find("tenant-a")).thenReturn(Optional.of(context("tenant-a")));
        return tenants;
    }

    private static SourceCleanupJob withId(SourceCleanupJob job, long id) {
        return new SourceCleanupJob(
                id,
                job.watcherId(),
                job.tenantId(),
                job.sourcePath(),
                job.fingerprint(),
                job.importOperationId(),
                job.fileKey(),
                job.action(),
                job.moveTargetPath(),
                job.failureDirectory(),
                job.maxAttempts(),
                job.retryInitialDelay(),
                job.retryMaxDelay(),
                job.attemptCount(),
                job.state(),
                job.nextAttemptAt(),
                job.lastError(),
                job.createdAt(),
                job.updatedAt(),
                job.leaseUntil());
    }

    @Test
    void legacyActionRetriesWithBackoffThenQuarantines() throws Exception {
        Path root = Files.createTempDirectory(Path.of("target"), "watcher-legacy-retry-");
        Path input = root.resolve("input");
        Files.createDirectories(input);
        Path source = input.resolve("a.dcm");
        Files.writeString(source, "data");
        // A regular file where a move directory is expected makes every MOVE fail.
        Path blocker = root.resolve("blocker");
        Files.writeString(blocker, "not a directory");
        StoragePool pool = mock(StoragePool.class);
        when(pool.write(any(TenantContext.class), any(ContentSource.class), any()))
                .thenReturn("0123456789abcdef0123456789abcdef");
        TenantManager tenants = mock(TenantManager.class);
        when(tenants.find("tenant-a")).thenReturn(Optional.of(context("tenant-a")));
        ImportedFileHistory history = new ImportedFileHistory(root.resolve("history"), CLOCK);
        // A clock in the future keeps freshly written sources eligible under minimumFileAge.
        MutableClock clock = new MutableClock(Instant.parse("2099-01-01T00:00:00Z"));
        WatcherScanner clockedScanner = new WatcherScanner(pool, tenants, history, clock);
        Path failureDirectory = root.resolve("failed");
        WatcherConfiguration configuration = new WatcherConfiguration(
                "watcher-a",
                "tenant-a",
                WatcherTenantMode.SINGLE_TENANT,
                false,
                input,
                true,
                false,
                List.of("**/*.dcm"),
                PostImportAction.MOVE,
                blocker,
                Duration.ZERO,
                0,
                Duration.ZERO,
                Duration.ZERO,
                1,
                2,
                Duration.ofDays(1),
                Duration.ZERO,
                failureDirectory,
                5,
                Duration.ofSeconds(5),
                Duration.ofMinutes(5));

        // Import succeeds; the immediate post-import MOVE fails (imported + failed).
        WatcherScanResult first = clockedScanner.scan(configuration);
        assertThat(first.importedCount()).isEqualTo(1);
        assertThat(first.failedCount()).isEqualTo(1);
        assertThat(source).exists();

        // Retry 1 is due immediately, fails, and arms a 5 s backoff.
        WatcherScanResult second = clockedScanner.scan(configuration);
        assertThat(second.failedCount()).isEqualTo(1);
        assertThat(source).exists();

        // Inside the backoff window the source is skipped, not failed again.
        WatcherScanResult skipped = clockedScanner.scan(configuration);
        assertThat(skipped.skippedCount()).isGreaterThanOrEqualTo(1);

        // Exhaust the remaining budget (4 more due attempts reach the cap of 5).
        for (int index = 0; index < 4; index++) {
            clock.advance(Duration.ofMinutes(6));
            clockedScanner.scan(configuration);
        }
        Path quarantined = failureDirectory.resolve("watcher-a").resolve("a.dcm");
        assertThat(quarantined).exists();
        assertThat(source).doesNotExist();

        // The terminal flag survives: the quarantined fingerprint is not retried.
        assertThat(history.read("watcher-a").stream()
                        .filter(entry -> entry.isQuarantined())
                        .count())
                .isEqualTo(1);
    }

    private static final class MutableClock extends Clock {

        private volatile Instant instant;

        private MutableClock(Instant instant) {
            this.instant = instant;
        }

        private void advance(Duration duration) {
            instant = instant.plus(duration);
        }

        @Override
        public ZoneId getZone() {
            return ZoneOffset.UTC;
        }

        @Override
        public Clock withZone(ZoneId zone) {
            return this;
        }

        @Override
        public Instant instant() {
            return instant;
        }
    }

    private static WatcherConfiguration configuration(
            Path input, PostImportAction action, Path moveDirectory, boolean recursive, long maxFileSize) {
        return new WatcherConfiguration(
                "watcher-a",
                "tenant-a",
                WatcherTenantMode.SINGLE_TENANT,
                false,
                input,
                true,
                recursive,
                List.of("**/*.dcm"),
                action,
                moveDirectory,
                Duration.ZERO,
                maxFileSize,
                Duration.ZERO,
                Duration.ZERO,
                1,
                2,
                Duration.ofDays(1),
                Duration.ZERO);
    }

    private static TenantContext context(String tenantId) {
        return new TenantContext(tenantId, TenantStatus.ENABLED, Instant.EPOCH, Instant.EPOCH);
    }
}
