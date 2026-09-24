package io.github.cocosip.stow.internal.watcher;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import io.github.cocosip.stow.api.FileWatcherManager;
import io.github.cocosip.stow.api.FileWatcherOptionsManager;
import io.github.cocosip.stow.config.SourceCleanupConfiguration;
import io.github.cocosip.stow.model.WatcherConfiguration;
import io.github.cocosip.stow.model.WatcherOptions;
import java.io.IOException;
import java.nio.file.Path;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.mockito.ArgumentCaptor;

class SourceCleanupWorkerTest {

    private static final Instant NOW = Instant.parse("2026-09-24T08:00:00Z");

    @ParameterizedTest
    @CsvSource({
        "false,false,false",
        "false,false,true",
        "false,true,false",
        "false,true,true",
        "true,false,false",
        "true,false,true",
        "true,true,false",
        "true,true,true"
    })
    void requiresSourceCleanupGlobalWatcherAndEnabledConfiguration(
            boolean cleanupEnabled, boolean globalEnabled, boolean watcherEnabled) {
        SourceCleanupStore store = mock(SourceCleanupStore.class);
        SourceCleanupConfiguration configuration = configuration(cleanupEnabled, false);
        SourceCleanupGate gate = gate(configuration, globalEnabled, watcherEnabled);
        when(store.claimDue(any(), anyInt(), any())).thenReturn(List.of());

        SourceCleanupWorker.CycleResult result = worker(configuration, store, gate, mock(SourceFileRelocator.class))
                .runCycle();

        if (!cleanupEnabled || !globalEnabled || !watcherEnabled) {
            assertThat(result).isEqualTo(SourceCleanupWorker.CycleResult.empty());
            verifyNoInteractions(store);
        } else {
            verify(store).releaseStaleImports(NOW.minus(Duration.ofMinutes(10)), 50);
            verify(store).pruneTerminal(NOW.minus(Duration.ofDays(1)), 50);
            verify(store).claimDue(NOW, 2, Duration.ofMinutes(5));
        }
    }

    @Test
    void runsBoundedMaintenanceAndOptimizationOnlyAfterItsInterval() {
        MutableClock clock = new MutableClock(NOW);
        SourceCleanupStore store = mock(SourceCleanupStore.class);
        SourceCleanupConfiguration configuration = configuration(true, true);
        SourceCleanupGate gate = gate(configuration, true, true);
        when(store.releaseStaleImports(any(), anyInt())).thenReturn(3);
        when(store.pruneTerminal(any(), anyInt())).thenReturn(4);
        when(store.claimDue(any(), anyInt(), any())).thenReturn(List.of());
        when(store.optimize()).thenReturn(new SourceCleanupStore.OptimizationResult(10, 5));
        SourceCleanupWorker worker = worker(configuration, store, gate, mock(SourceFileRelocator.class), clock);

        assertThat(worker.runCycle().optimized()).isZero();
        verify(store, never()).optimize();

        clock.advance(Duration.ofHours(2));
        SourceCleanupWorker.CycleResult result = worker.runCycle();

        assertThat(result.staleReleased()).isEqualTo(3);
        assertThat(result.pruned()).isEqualTo(4);
        assertThat(result.optimized()).isEqualTo(1);
        verify(store).optimize();
    }

    @Test
    void deletesMatchingSourceAndRemovesCompletedJob() throws Exception {
        SourceCleanupStore store = mock(SourceCleanupStore.class);
        SourceFileRelocator relocator = mock(SourceFileRelocator.class);
        SourceCleanupJob job = job(SourceCleanupAction.DELETE, 0, 5, Path.of("failed"));
        when(store.claimDue(any(), anyInt(), any())).thenReturn(List.of(job), List.of());
        when(relocator.deleteIfMatching(job.sourcePath(), job.fingerprint()))
                .thenReturn(SourceFileRelocator.Status.COMPLETED);

        SourceCleanupWorker.CycleResult result = enabledWorker(store, relocator).runCycle();

        assertThat(result.claimed()).isEqualTo(1);
        assertThat(result.completed()).isEqualTo(1);
        verify(store).remove(job.id());
    }

    @Test
    void movesMatchingSourceToConfiguredDestination() throws Exception {
        SourceCleanupStore store = mock(SourceCleanupStore.class);
        SourceFileRelocator relocator = mock(SourceFileRelocator.class);
        SourceCleanupJob job = job(SourceCleanupAction.MOVE, 0, 5, Path.of("failed"));
        when(store.claimDue(any(), anyInt(), any())).thenReturn(List.of(job), List.of());
        when(relocator.moveIfMatching(job.sourcePath(), job.moveTargetPath(), job.fingerprint()))
                .thenReturn(new SourceFileRelocator.Result(SourceFileRelocator.Status.COMPLETED, job.moveTargetPath()));

        SourceCleanupWorker.CycleResult result = enabledWorker(store, relocator).runCycle();

        assertThat(result.completed()).isEqualTo(1);
        verify(store).remove(job.id());
    }

    @Test
    void discardsJobWithoutTouchingReplacementSourceOnFingerprintMismatch() throws Exception {
        SourceCleanupStore store = mock(SourceCleanupStore.class);
        SourceFileRelocator relocator = mock(SourceFileRelocator.class);
        SourceCleanupJob job = job(SourceCleanupAction.DELETE, 0, 5, Path.of("failed"));
        when(store.claimDue(any(), anyInt(), any())).thenReturn(List.of(job), List.of());
        when(relocator.deleteIfMatching(job.sourcePath(), job.fingerprint()))
                .thenReturn(SourceFileRelocator.Status.FINGERPRINT_MISMATCH);

        SourceCleanupWorker.CycleResult result = enabledWorker(store, relocator).runCycle();

        assertThat(result.discardedMismatch()).isEqualTo(1);
        verify(store).remove(job.id());
    }

    @Test
    void retriesCleanupWithCappedExponentialBackoff() throws Exception {
        SourceCleanupStore store = mock(SourceCleanupStore.class);
        SourceFileRelocator relocator = mock(SourceFileRelocator.class);
        SourceCleanupJob job = job(SourceCleanupAction.DELETE, 2, 5, Path.of("failed"));
        when(store.claimDue(any(), anyInt(), any())).thenReturn(List.of(job), List.of());
        doThrow(new IOException("locked")).when(relocator).deleteIfMatching(job.sourcePath(), job.fingerprint());

        SourceCleanupWorker.CycleResult result = enabledWorker(store, relocator).runCycle();

        ArgumentCaptor<SourceCleanupJob> updated = ArgumentCaptor.forClass(SourceCleanupJob.class);
        verify(store).update(updated.capture());
        assertThat(updated.getValue().attemptCount()).isEqualTo(3);
        assertThat(updated.getValue().state()).isEqualTo(SourceCleanupState.RETRYING);
        assertThat(updated.getValue().nextAttemptAt()).isEqualTo(NOW.plusSeconds(8));
        assertThat(result.retried()).isEqualTo(1);
    }

    @Test
    void quarantinesSourceAfterFinalCleanupFailure() throws Exception {
        SourceCleanupStore store = mock(SourceCleanupStore.class);
        SourceFileRelocator relocator = mock(SourceFileRelocator.class);
        SourceCleanupJob job = job(SourceCleanupAction.DELETE, 0, 1, Path.of("failed"));
        Path quarantine = job.failureDirectory()
                .resolve(job.watcherId())
                .resolve(job.sourcePath().getFileName());
        when(store.claimDue(any(), anyInt(), any())).thenReturn(List.of(job), List.of());
        doThrow(new IOException("locked")).when(relocator).deleteIfMatching(job.sourcePath(), job.fingerprint());
        when(relocator.moveIfMatching(job.sourcePath(), quarantine, job.fingerprint()))
                .thenReturn(new SourceFileRelocator.Result(SourceFileRelocator.Status.COMPLETED, quarantine));

        SourceCleanupWorker.CycleResult result = enabledWorker(store, relocator).runCycle();

        assertThat(result.quarantined()).isEqualTo(1);
        verify(store).remove(job.id());
    }

    @Test
    void recordsTerminalFailureWhenQuarantineAlsoFails() throws Exception {
        SourceCleanupStore store = mock(SourceCleanupStore.class);
        SourceFileRelocator relocator = mock(SourceFileRelocator.class);
        SourceCleanupJob job = job(SourceCleanupAction.DELETE, 0, 1, Path.of("failed"));
        when(store.claimDue(any(), anyInt(), any())).thenReturn(List.of(job), List.of());
        doThrow(new IOException("locked")).when(relocator).deleteIfMatching(job.sourcePath(), job.fingerprint());
        when(relocator.moveIfMatching(eq(job.sourcePath()), any(), eq(job.fingerprint())))
                .thenThrow(new IOException("still locked"));

        SourceCleanupWorker.CycleResult result = enabledWorker(store, relocator).runCycle();

        ArgumentCaptor<SourceCleanupJob> updated = ArgumentCaptor.forClass(SourceCleanupJob.class);
        verify(store).update(updated.capture());
        assertThat(updated.getValue().state()).isEqualTo(SourceCleanupState.TERMINAL_FAILED);
        assertThat(updated.getValue().nextAttemptAt()).isNull();
        assertThat(result.terminalFailed()).isEqualTo(1);
    }

    @Test
    void closeWaitsForRunningCleanupAction() throws Exception {
        SourceCleanupStore store = mock(SourceCleanupStore.class);
        SourceFileRelocator relocator = mock(SourceFileRelocator.class);
        SourceCleanupJob job = job(SourceCleanupAction.DELETE, 0, 5, Path.of("failed"));
        CountDownLatch actionStarted = new CountDownLatch(1);
        CountDownLatch releaseAction = new CountDownLatch(1);
        when(store.claimDue(any(), anyInt(), any())).thenReturn(List.of(job), List.of());
        doAnswer(invocation -> {
                    actionStarted.countDown();
                    assertThat(releaseAction.await(2, TimeUnit.SECONDS)).isTrue();
                    return SourceFileRelocator.Status.COMPLETED;
                })
                .when(relocator)
                .deleteIfMatching(job.sourcePath(), job.fingerprint());
        SourceCleanupConfiguration configuration = configuration(true, false);

        try (var actionExecutor = Executors.newVirtualThreadPerTaskExecutor();
                var scheduler = Executors.newSingleThreadScheduledExecutor();
                var closeExecutor = Executors.newVirtualThreadPerTaskExecutor()) {
            SourceCleanupWorker worker = new SourceCleanupWorker(
                    configuration,
                    store,
                    gate(configuration, true, true),
                    relocator,
                    Clock.fixed(NOW, ZoneOffset.UTC),
                    actionExecutor,
                    scheduler);
            worker.start();
            assertThat(actionStarted.await(1, TimeUnit.SECONDS)).isTrue();

            var close = closeExecutor.submit(worker::close);
            Thread.sleep(50);
            assertThat(close.isDone()).isFalse();
            releaseAction.countDown();
            close.get(1, TimeUnit.SECONDS);
        }
    }

    private SourceCleanupWorker enabledWorker(SourceCleanupStore store, SourceFileRelocator relocator) {
        SourceCleanupConfiguration configuration = configuration(true, false);
        return worker(configuration, store, gate(configuration, true, true), relocator);
    }

    private SourceCleanupWorker worker(
            SourceCleanupConfiguration configuration,
            SourceCleanupStore store,
            SourceCleanupGate gate,
            SourceFileRelocator relocator) {
        return worker(configuration, store, gate, relocator, Clock.fixed(NOW, ZoneOffset.UTC));
    }

    private SourceCleanupWorker worker(
            SourceCleanupConfiguration configuration,
            SourceCleanupStore store,
            SourceCleanupGate gate,
            SourceFileRelocator relocator,
            Clock clock) {
        return new SourceCleanupWorker(configuration, store, gate, relocator, clock, Runnable::run, null);
    }

    private SourceCleanupGate gate(
            SourceCleanupConfiguration configuration, boolean globalEnabled, boolean watcherEnabled) {
        FileWatcherOptionsManager options = mock(FileWatcherOptionsManager.class);
        FileWatcherManager watchers = mock(FileWatcherManager.class);
        WatcherConfiguration watcher = mock(WatcherConfiguration.class);
        when(options.get())
                .thenReturn(
                        new WatcherOptions(globalEnabled, 2, Duration.ofSeconds(1), Duration.ZERO, Duration.ofDays(1)));
        when(watcher.enabled()).thenReturn(watcherEnabled);
        when(watchers.list()).thenReturn(List.of(watcher));
        return new SourceCleanupGate(configuration, options, watchers);
    }

    private SourceCleanupConfiguration configuration(boolean enabled, boolean optimize) {
        return new SourceCleanupConfiguration(
                enabled,
                Path.of("source-cleanup.db"),
                Duration.ofMillis(10),
                2,
                100,
                Duration.ofDays(1),
                Duration.ofMinutes(10),
                optimize,
                Duration.ofHours(1),
                50);
    }

    private SourceCleanupJob job(SourceCleanupAction action, int attempts, int maxAttempts, Path failureDirectory) {
        Path source = Path.of("inbox", "image.dcm").toAbsolutePath();
        SourceFingerprint fingerprint = new SourceFingerprint(1, source.toString(), 3, 10, 9, "abc");
        return new SourceCleanupJob(
                7,
                "watcher-a",
                "tenant-a",
                source,
                fingerprint,
                "operation-a",
                "file-a",
                action,
                action == SourceCleanupAction.MOVE ? Path.of("archive", "image.dcm") : null,
                failureDirectory,
                maxAttempts,
                Duration.ofSeconds(2),
                Duration.ofSeconds(10),
                attempts,
                SourceCleanupState.PENDING,
                NOW,
                null,
                NOW.minusSeconds(60),
                NOW.minusSeconds(10),
                NOW.plusSeconds(60));
    }

    private static final class MutableClock extends Clock {
        private Instant instant;

        private MutableClock(Instant instant) {
            this.instant = instant;
        }

        void advance(Duration duration) {
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
}
