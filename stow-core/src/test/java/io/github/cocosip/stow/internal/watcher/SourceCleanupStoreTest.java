package io.github.cocosip.stow.internal.watcher;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.cocosip.stow.config.SourceCleanupConfiguration;
import io.github.cocosip.stow.internal.sqlite.SqliteConnectionFactory;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import org.junit.jupiter.api.Test;

class SourceCleanupStoreTest {

    private static final Instant NOW = Instant.parse("2026-09-24T00:00:00Z");
    private static final Clock CLOCK = Clock.fixed(NOW, ZoneOffset.UTC);

    @Test
    void createsDatabaseLazilyAndReplacesChangedFingerprintReservation() throws Exception {
        Path root = Files.createTempDirectory(Path.of("target"), "cleanup-store-lazy-");
        Path database = root.resolve("source-cleanup.db");
        SourceCleanupStore store = store(database, 10);

        assertThat(Files.exists(database)).isFalse();
        Path source = root.resolve("source.bin");
        Files.writeString(source, "one");
        SourceCleanupJob first = reservation(source, SourceFingerprint.capture(source), "operation-1");
        assertThat(store.tryReserve(first).status()).isEqualTo(SourceCleanupStore.ReservationStatus.RESERVED);
        SourceCleanupStore.Reservation duplicate =
                store.tryReserve(reservation(source, SourceFingerprint.capture(source), "operation-2"));
        assertThat(duplicate.status()).isEqualTo(SourceCleanupStore.ReservationStatus.ALREADY_ACTIVE);
        assertThat(duplicate.job().importOperationId()).isEqualTo("operation-1");

        Files.writeString(source, "two");
        SourceCleanupStore.Reservation replacement =
                store.tryReserve(reservation(source, SourceFingerprint.capture(source), "operation-3"));

        assertThat(replacement.status()).isEqualTo(SourceCleanupStore.ReservationStatus.RESERVED);
        assertThat(replacement.job().importOperationId()).isEqualTo("operation-3");
        assertThat(store.activeCount()).isEqualTo(1);
        store.close();
    }

    @Test
    void enforcesCapacityAtomicallyAcrossStoreInstances() throws Exception {
        Path root = Files.createTempDirectory(Path.of("target"), "cleanup-store-capacity-");
        Path database = root.resolve("source-cleanup.db");
        Path firstPath = Files.writeString(root.resolve("one.bin"), "one");
        Path secondPath = Files.writeString(root.resolve("two.bin"), "two");
        SourceCleanupStore firstStore = store(database, 1);
        SourceCleanupStore secondStore = store(database, 1);
        CountDownLatch start = new CountDownLatch(1);

        SourceCleanupStore.Reservation first;
        SourceCleanupStore.Reservation second;
        try (var executor = Executors.newVirtualThreadPerTaskExecutor()) {
            var one = executor.submit(() -> {
                start.await();
                return firstStore.tryReserve(
                        reservation(firstPath, SourceFingerprint.capture(firstPath), "operation-1"));
            });
            var two = executor.submit(() -> {
                start.await();
                return secondStore.tryReserve(
                        reservation(secondPath, SourceFingerprint.capture(secondPath), "operation-2"));
            });
            start.countDown();
            first = one.get();
            second = two.get();
        }

        assertThat(java.util.List.of(first.status(), second.status()))
                .containsExactlyInAnyOrder(
                        SourceCleanupStore.ReservationStatus.RESERVED,
                        SourceCleanupStore.ReservationStatus.CAPACITY_FULL);
        assertThat(firstStore.activeCount()).isEqualTo(1);
    }

    @Test
    void activatesClaimsAndReclaimsExpiredLease() throws Exception {
        Path root = Files.createTempDirectory(Path.of("target"), "cleanup-store-lease-");
        Path source = Files.writeString(root.resolve("source.bin"), "one");
        SourceCleanupStore store = store(root.resolve("source-cleanup.db"), 10);
        SourceCleanupJob reserved = store.tryReserve(
                        reservation(source, SourceFingerprint.capture(source), "operation-1"))
                .job();
        SourceCleanupJob active = store.activate(reserved.id(), "0123456789abcdef0123456789abcdef");

        assertThat(active.state()).isEqualTo(SourceCleanupState.PENDING);
        assertThat(store.claimDue(NOW, 1, Duration.ofMinutes(1)))
                .singleElement()
                .satisfies(job -> {
                    assertThat(job.id()).isEqualTo(active.id());
                    assertThat(job.leaseUntil()).isEqualTo(NOW.plus(Duration.ofMinutes(1)));
                });
        assertThat(store.claimDue(NOW.plusSeconds(30), 1, Duration.ofMinutes(1)))
                .isEmpty();
        assertThat(store.claimDue(NOW.plusSeconds(61), 1, Duration.ofMinutes(1)))
                .singleElement()
                .extracting(SourceCleanupJob::id)
                .isEqualTo(active.id());
    }

    @Test
    void releasesOnlyStaleImportsAndPrunesBoundedTerminalRows() throws Exception {
        Path root = Files.createTempDirectory(Path.of("target"), "cleanup-store-maintenance-");
        SourceCleanupStore store = store(root.resolve("source-cleanup.db"), 10);
        Path stale = Files.writeString(root.resolve("stale.bin"), "stale");
        Path fresh = Files.writeString(root.resolve("fresh.bin"), "fresh");
        Path keep = Files.writeString(root.resolve("keep.bin"), "keep");
        SourceCleanupJob staleJob = store.tryReserve(
                        reservation(stale, SourceFingerprint.capture(stale), "operation-1"))
                .job();
        SourceCleanupJob freshJob = store.tryReserve(
                        reservation(fresh, SourceFingerprint.capture(fresh), "operation-2"))
                .job();
        SourceCleanupJob keepJob = store.tryReserve(
                        reservation(keep, SourceFingerprint.capture(keep), "operation-3", SourceCleanupAction.KEEP))
                .job();
        store.update(staleJob.withUpdatedAt(NOW.minus(Duration.ofMinutes(20))));
        store.update(freshJob.withUpdatedAt(NOW.minus(Duration.ofMinutes(2))));
        SourceCleanupJob terminal = store.activate(keepJob.id(), "fedcba9876543210fedcba9876543210");
        store.update(terminal.withUpdatedAt(NOW.minus(Duration.ofDays(2))));

        assertThat(store.releaseStaleImports(NOW.minus(Duration.ofMinutes(10)), 10))
                .isEqualTo(1);
        assertThat(store.findActive(stale)).isEmpty();
        assertThat(store.findActive(fresh)).isPresent();
        assertThat(store.pruneTerminal(NOW.minus(Duration.ofDays(1)), 1)).isEqualTo(1);
        assertThat(store.findActive(keep)).isEmpty();
        assertThat(store.optimize().sizeAfter()).isGreaterThanOrEqualTo(0);
    }

    private static SourceCleanupStore store(Path database, int maxActiveJobs) {
        SourceCleanupConfiguration configuration = new SourceCleanupConfiguration(
                true,
                database,
                Duration.ofSeconds(5),
                2,
                maxActiveJobs,
                Duration.ofDays(1),
                Duration.ofMinutes(10),
                true,
                Duration.ofDays(1),
                5_000);
        return new SourceCleanupStore(configuration, SqliteConnectionFactory.defaults(), CLOCK);
    }

    private static SourceCleanupJob reservation(Path source, SourceFingerprint fingerprint, String operationId) {
        return reservation(source, fingerprint, operationId, SourceCleanupAction.DELETE);
    }

    private static SourceCleanupJob reservation(
            Path source, SourceFingerprint fingerprint, String operationId, SourceCleanupAction action) {
        return SourceCleanupJob.reservation(
                "watcher-a",
                "tenant-a",
                source,
                fingerprint,
                operationId,
                action,
                null,
                null,
                5,
                Duration.ofSeconds(5),
                Duration.ofMinutes(5),
                NOW);
    }
}
