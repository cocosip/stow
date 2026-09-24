package io.github.cocosip.stow.internal.watcher;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.github.cocosip.stow.api.StoragePool;
import io.github.cocosip.stow.api.TenantManager;
import io.github.cocosip.stow.model.PostImportAction;
import io.github.cocosip.stow.model.WatcherConfiguration;
import io.github.cocosip.stow.model.WatcherOptions;
import io.github.cocosip.stow.model.WatcherScanResult;
import io.github.cocosip.stow.model.WatcherTenantMode;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

class FileWatcherManagerTest {

    private static final Clock CLOCK = Clock.fixed(Instant.parse("2026-09-19T00:00:00Z"), ZoneOffset.UTC);

    @Test
    void persistsConfigurationAndSupportsLifecycleOperations() throws Exception {
        Path root = Files.createTempDirectory(Path.of("target"), "watcher-manager-");
        TenantManager tenants = mock(TenantManager.class);
        StoragePool pool = mock(StoragePool.class);
        DefaultFileWatcherManager manager = new DefaultFileWatcherManager(root, pool, tenants, CLOCK);
        WatcherConfiguration configuration = configuration(root.resolve("in"), "watcher-a", "tenant-a");

        assertThat(manager.register(configuration)).isEqualTo(configuration);
        assertThat(manager.list()).containsExactly(configuration);
        assertThat(manager.listForTenant("tenant-a")).containsExactly(configuration);

        WatcherConfiguration updated = new WatcherConfiguration(
                "watcher-a",
                "tenant-a",
                WatcherTenantMode.SINGLE_TENANT,
                false,
                root.resolve("changed"),
                false,
                true,
                List.of("**/*.dcm"),
                PostImportAction.KEEP,
                null,
                Duration.ofSeconds(1),
                0,
                Duration.ZERO,
                Duration.ZERO,
                1,
                1,
                Duration.ofDays(1),
                Duration.ZERO,
                root.resolve("failed"),
                9,
                Duration.ofSeconds(2),
                Duration.ofMinutes(2));
        assertThat(manager.update(updated)).isEqualTo(updated);
        manager.enable("watcher-a");
        assertThat(manager.find("watcher-a"))
                .get()
                .extracting(WatcherConfiguration::enabled)
                .isEqualTo(true);
        manager.disable("watcher-a");
        assertThat(manager.find("watcher-a"))
                .get()
                .extracting(WatcherConfiguration::enabled)
                .isEqualTo(false);

        DefaultFileWatcherManager reopened = new DefaultFileWatcherManager(root, pool, tenants, CLOCK);
        assertThat(reopened.find("watcher-a")).contains(updated);
        reopened.remove("watcher-a");
        assertThat(reopened.find("watcher-a")).isEmpty();
    }

    @Test
    void mapsSingleAndSubdirectoryTenantsAndPersistsOptions() throws Exception {
        Path root = Files.createTempDirectory(Path.of("target"), "watcher-options-");
        TenantManager tenants = mock(TenantManager.class);
        StoragePool pool = mock(StoragePool.class);
        DefaultFileWatcherManager manager = new DefaultFileWatcherManager(root, pool, tenants, CLOCK);

        assertThat(manager.options().get().enabled()).isTrue();
        manager.options().disable();
        DefaultFileWatcherManager reopened = new DefaultFileWatcherManager(root, pool, tenants, CLOCK);
        assertThat(reopened.options().get().enabled()).isFalse();
        manager.register(new WatcherConfiguration(
                "watcher-multi",
                null,
                WatcherTenantMode.SUBDIRECTORY_TENANTS,
                true,
                root.resolve("incoming"),
                true,
                true,
                List.of("**/*.dcm"),
                PostImportAction.KEEP,
                null,
                Duration.ofSeconds(1),
                0,
                Duration.ZERO,
                Duration.ZERO,
                1,
                1,
                Duration.ofDays(1),
                Duration.ZERO));
        assertThat(manager.listForTenant("tenant-a")).isEmpty();
    }

    @Test
    void loadsLegacyWatcherJsonWithCleanupDefaults() throws Exception {
        Path root = Files.createTempDirectory(Path.of("target"), "watcher-legacy-");
        Files.createDirectories(root.resolve("watchers"));
        Files.writeString(
                root.resolve("watchers").resolve("legacy.json"),
                """
                {"schemaVersion":1,"watcherId":"legacy","tenantId":"tenant-a",\
                "tenantMode":"SINGLE_TENANT","autoCreateTenantDirectories":false,\
                "watchPath":"inbox","enabled":true,"recursive":true,"globs":["**/*.dcm"],\
                "postImportAction":"DELETE","moveDirectory":null,"pollIntervalMillis":1000,\
                "maxFileSize":0,"minimumFileAgeMillis":0,"stabilityCheckIntervalMillis":0,\
                "stabilityCheckCount":1,"concurrentImports":1,"historyRetentionMillis":86400000,\
                "historyFlushIntervalMillis":0}
                """,
                StandardOpenOption.CREATE_NEW);

        WatcherConfiguration configuration =
                new WatcherConfigurationStore(root).find("legacy").orElseThrow();

        assertThat(configuration.sourceCleanupFailureDirectory())
                .isEqualTo(Path.of("stow-source-failed").toAbsolutePath().normalize());
        assertThat(configuration.maxPostImportActionAttempts()).isEqualTo(5);
        assertThat(configuration.postImportRetryInitialDelay()).isEqualTo(Duration.ofSeconds(5));
        assertThat(configuration.postImportRetryMaxDelay()).isEqualTo(Duration.ofMinutes(5));
    }

    @Test
    void schedulesEachWatcherUsingItsOwnPollingInterval() throws Exception {
        Path root = Files.createTempDirectory(Path.of("target"), "watcher-schedule-");
        MutableClock clock = new MutableClock(Instant.parse("2026-09-24T00:00:00Z"));
        WatcherScanner scanner = mock(WatcherScanner.class);
        when(scanner.scan(any())).thenAnswer(invocation -> result(invocation.getArgument(0)));
        DefaultFileWatcherManager manager =
                new DefaultFileWatcherManager(new WatcherConfigurationStore(root), scanner, clock);
        manager.register(configuration(root.resolve("fast"), "fast", "tenant-a", Duration.ofMillis(10)));
        manager.register(configuration(root.resolve("slow"), "slow", "tenant-a", Duration.ofMillis(100)));
        try {
            manager.pollOnce();
            await().untilAsserted(
                            () -> verify(scanner).scan(manager.find("fast").orElseThrow()));
            verify(scanner).scan(manager.find("slow").orElseThrow());

            clock.advance(Duration.ofMillis(20));
            manager.pollOnce();
            await().untilAsserted(() ->
                    verify(scanner, times(2)).scan(manager.find("fast").orElseThrow()));
            verify(scanner).scan(manager.find("slow").orElseThrow());

            clock.advance(Duration.ofMillis(80));
            manager.pollOnce();
            await().untilAsserted(() ->
                    verify(scanner, times(3)).scan(manager.find("fast").orElseThrow()));
            verify(scanner, times(2)).scan(manager.find("slow").orElseThrow());
        } finally {
            manager.close();
        }
    }

    @Test
    void backgroundCoordinatorWakesForWatcherIntervalRatherThanGlobalInterval() throws Exception {
        Path root = Files.createTempDirectory(Path.of("target"), "watcher-background-interval-");
        WatcherScanner scanner = mock(WatcherScanner.class);
        when(scanner.scan(any())).thenAnswer(invocation -> result(invocation.getArgument(0)));
        DefaultFileWatcherManager manager =
                new DefaultFileWatcherManager(new WatcherConfigurationStore(root), scanner, Clock.systemUTC());
        manager.register(configuration(root.resolve("fast"), "fast", "tenant-a", Duration.ofMillis(20)));
        try {
            manager.start();
            await().atMost(Duration.ofSeconds(1))
                    .untilAsserted(() -> verify(scanner, atLeast(2)).scan(any()));
        } finally {
            manager.close();
        }
    }

    @Test
    void manualAndBackgroundScansForSameWatcherShareOneExecution() throws Exception {
        Path root = Files.createTempDirectory(Path.of("target"), "watcher-overlap-");
        WatcherScanner scanner = mock(WatcherScanner.class);
        CountDownLatch started = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        when(scanner.scan(any())).thenAnswer(invocation -> {
            started.countDown();
            assertThat(release.await(2, TimeUnit.SECONDS)).isTrue();
            return result(invocation.getArgument(0));
        });
        DefaultFileWatcherManager manager = new DefaultFileWatcherManager(
                new WatcherConfigurationStore(root), scanner, new MutableClock(Instant.EPOCH));
        manager.register(configuration(root.resolve("in"), "watcher-a", "tenant-a"));
        try (var calls = Executors.newVirtualThreadPerTaskExecutor()) {
            manager.pollOnce();
            assertThat(started.await(1, TimeUnit.SECONDS)).isTrue();
            var manual = calls.submit(() -> manager.scanNow("watcher-a"));

            Thread.sleep(50);
            verify(scanner).scan(any());
            release.countDown();
            manual.get(1, TimeUnit.SECONDS);
            verify(scanner).scan(any());
        } finally {
            manager.close();
        }
    }

    @Test
    void differentWatchersRunInParallelWithinDynamicGlobalLimit() throws Exception {
        Path root = Files.createTempDirectory(Path.of("target"), "watcher-parallel-");
        WatcherScanner scanner = mock(WatcherScanner.class);
        CountDownLatch twoStarted = new CountDownLatch(2);
        CountDownLatch release = new CountDownLatch(1);
        AtomicInteger active = new AtomicInteger();
        AtomicInteger maximum = new AtomicInteger();
        when(scanner.scan(any())).thenAnswer(invocation -> {
            int current = active.incrementAndGet();
            maximum.accumulateAndGet(current, Math::max);
            twoStarted.countDown();
            assertThat(release.await(2, TimeUnit.SECONDS)).isTrue();
            active.decrementAndGet();
            return result(invocation.getArgument(0));
        });
        DefaultFileWatcherManager manager = new DefaultFileWatcherManager(
                new WatcherConfigurationStore(root), scanner, new MutableClock(Instant.EPOCH));
        manager.options()
                .update(new WatcherOptions(
                        true, 2, Duration.ofSeconds(30), Duration.ofSeconds(5), Duration.ofDays(30)));
        manager.register(configuration(root.resolve("a"), "watcher-a", "tenant-a"));
        manager.register(configuration(root.resolve("b"), "watcher-b", "tenant-a"));
        try {
            manager.pollOnce();
            assertThat(twoStarted.await(1, TimeUnit.SECONDS)).isTrue();
            assertThat(maximum).hasValue(2);
        } finally {
            release.countDown();
            manager.close();
        }
    }

    @Test
    void closeStopsAdmissionAndWaitsForActiveScan() throws Exception {
        Path root = Files.createTempDirectory(Path.of("target"), "watcher-close-");
        WatcherScanner scanner = mock(WatcherScanner.class);
        CountDownLatch started = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        when(scanner.scan(any())).thenAnswer(invocation -> {
            started.countDown();
            assertThat(release.await(2, TimeUnit.SECONDS)).isTrue();
            return result(invocation.getArgument(0));
        });
        DefaultFileWatcherManager manager = new DefaultFileWatcherManager(
                new WatcherConfigurationStore(root), scanner, new MutableClock(Instant.EPOCH));
        manager.register(configuration(root.resolve("in"), "watcher-a", "tenant-a"));
        manager.pollOnce();
        assertThat(started.await(1, TimeUnit.SECONDS)).isTrue();

        try (var executor = Executors.newVirtualThreadPerTaskExecutor()) {
            var close = executor.submit(manager::close);
            Thread.sleep(50);
            assertThat(close.isDone()).isFalse();
            release.countDown();
            close.get(1, TimeUnit.SECONDS);
        }
    }

    private static WatcherConfiguration configuration(Path path, String watcherId, String tenantId) {
        return configuration(path, watcherId, tenantId, Duration.ofSeconds(1));
    }

    private static WatcherConfiguration configuration(
            Path path, String watcherId, String tenantId, Duration pollInterval) {
        return new WatcherConfiguration(
                watcherId,
                tenantId,
                WatcherTenantMode.SINGLE_TENANT,
                false,
                path,
                true,
                true,
                List.of("**/*.dcm"),
                PostImportAction.KEEP,
                null,
                pollInterval,
                0,
                Duration.ZERO,
                Duration.ZERO,
                1,
                1,
                Duration.ofDays(1),
                Duration.ZERO);
    }

    private static WatcherScanResult result(WatcherConfiguration configuration) {
        return new WatcherScanResult(configuration.watcherId(), Instant.EPOCH, Instant.EPOCH, 0, 0, 0, 0, 0, List.of());
    }

    private static final class MutableClock extends Clock {
        private Instant now;

        private MutableClock(Instant now) {
            this.now = now;
        }

        void advance(Duration duration) {
            now = now.plus(duration);
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
            return now;
        }
    }
}
