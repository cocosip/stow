package io.github.cocosip.stow.internal.tenant;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.github.cocosip.stow.exception.StowInterruptedException;
import io.github.cocosip.stow.exception.TenantNotFoundException;
import io.github.cocosip.stow.model.TenantStatus;
import java.io.IOException;
import java.lang.reflect.Field;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReentrantLock;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class DefaultTenantManagerTest {

    private static final Instant NOW = Instant.parse("2026-09-18T00:00:00Z");
    private static final Clock CLOCK = Clock.fixed(NOW, ZoneOffset.UTC);

    @TempDir
    Path temporaryDirectory;

    @Test
    void rejectsMissingTenantWhenAutoCreateIsDisabled() {
        DefaultTenantManager manager = manager(false, 0);

        assertThat(manager.find("tenant-a")).isEmpty();
        assertThatThrownBy(() -> manager.get("tenant-a")).isInstanceOf(TenantNotFoundException.class);
    }

    @Test
    void autoCreatesAndPersistsEnabledTenantWithCopiedDefaultQuota() {
        DefaultTenantManager manager = manager(true, 42);

        var created = manager.get("tenant-a");

        assertThat(created.tenantId()).isEqualTo("tenant-a");
        assertThat(created.status()).isEqualTo(TenantStatus.ENABLED);
        assertThat(created.createdAt()).isEqualTo(NOW);
        JsonTenantRepository reopenedRepository = new JsonTenantRepository(temporaryDirectory);
        DefaultTenantManager reopened = new DefaultTenantManager(reopenedRepository, CLOCK, false, 999);
        assertThat(reopened.get("tenant-a")).isEqualTo(created);
        assertThat(reopenedRepository.read().tenants().getFirst().maxFiles()).isEqualTo(42);
    }

    @Test
    void persistsDisableAndEnableTransitions() {
        DefaultTenantManager manager = manager(false, 0);
        manager.create("tenant-a");

        manager.disable("tenant-a");
        assertThat(manager.get("tenant-a").status()).isEqualTo(TenantStatus.DISABLED);

        manager.enable("tenant-a");
        JsonTenantRepository reopenedRepository = new JsonTenantRepository(temporaryDirectory);
        assertThat(new DefaultTenantManager(reopenedRepository, CLOCK, false, 0)
                        .get("tenant-a")
                        .status())
                .isEqualTo(TenantStatus.ENABLED);
    }

    @Test
    void createsSameTenantIdempotentlyUnderConcurrency() throws Exception {
        DefaultTenantManager manager = manager(false, 0);
        try (var executor = Executors.newVirtualThreadPerTaskExecutor()) {
            List<Callable<Object>> attempts = java.util.stream.IntStream.range(0, 32)
                    .mapToObj(ignored -> (Callable<Object>) () -> manager.create("tenant-a"))
                    .toList();
            var results = executor.invokeAll(attempts);

            assertThat(results).allSatisfy(result -> assertThat(result.get()).isEqualTo(manager.get("tenant-a")));
        }
        assertThat(manager.list()).hasSize(1);
        assertThat(new JsonTenantRepository(temporaryDirectory).read().tenants())
                .hasSize(1);
    }

    @Test
    void removesOnlyMatchingStaleTenantDocumentTempsAtRepositoryStartup() throws IOException {
        DefaultTenantManager manager = manager(false, 0);
        manager.create("tenant-a");
        Path stale = temporaryDirectory.resolve(".tenants.json.123e4567-e89b-12d3-a456-426614174000.tmp");
        Path nonMatching = temporaryDirectory.resolve(".tenants.json.not-a-uuid.tmp");
        Files.writeString(stale, "stale");
        Files.writeString(nonMatching, "retain");

        JsonTenantRepository reopened = new JsonTenantRepository(temporaryDirectory);

        assertThat(reopened.read().tenants()).hasSize(1);
        assertThat(stale).doesNotExist();
        assertThat(nonMatching).exists();
    }

    @Test
    void preservesInterruptWhenWaitingForRepositoryLock() throws Exception {
        Path documentPath = temporaryDirectory.toAbsolutePath().normalize().resolve("tenants.json");
        SignalingLock lock = new SignalingLock();
        repositoryLocks().put(documentPath, lock);
        JsonTenantRepository repository = new JsonTenantRepository(temporaryDirectory);
        AtomicReference<Thread> workerThread = new AtomicReference<>();
        lock.lockForTest();
        try (var executor = Executors.newVirtualThreadPerTaskExecutor()) {
            var result = executor.submit(() -> {
                workerThread.set(Thread.currentThread());
                try {
                    repository.read();
                    return new InterruptedLockResult(
                            false, Thread.currentThread().isInterrupted());
                } catch (StowInterruptedException exception) {
                    return new InterruptedLockResult(
                            true, Thread.currentThread().isInterrupted());
                }
            });
            assertThat(lock.awaitingAcquisition.await(1, TimeUnit.SECONDS)).isTrue();

            workerThread.get().interrupt();
            lock.unlock();

            InterruptedLockResult interrupted = result.get(1, TimeUnit.SECONDS);
            assertThat(interrupted.exceptionThrown()).isTrue();
            assertThat(interrupted.interruptFlagPreserved()).isTrue();
        } finally {
            if (lock.isHeldByCurrentThread()) {
                lock.unlock();
            }
            repositoryLocks().remove(documentPath, lock);
        }
    }

    private DefaultTenantManager manager(boolean autoCreate, long defaultQuota) {
        return new DefaultTenantManager(new JsonTenantRepository(temporaryDirectory), CLOCK, autoCreate, defaultQuota);
    }

    @SuppressWarnings("unchecked")
    private static ConcurrentHashMap<Path, ReentrantLock> repositoryLocks() throws ReflectiveOperationException {
        Field locks = JsonTenantRepository.class.getDeclaredField("LOCKS");
        locks.setAccessible(true);
        return (ConcurrentHashMap<Path, ReentrantLock>) locks.get(null);
    }

    private record InterruptedLockResult(boolean exceptionThrown, boolean interruptFlagPreserved) {}

    private static final class SignalingLock extends ReentrantLock {

        private final CountDownLatch awaitingAcquisition = new CountDownLatch(1);

        void lockForTest() {
            super.lock();
        }

        @Override
        public void lock() {
            awaitingAcquisition.countDown();
            super.lock();
        }

        @Override
        public void lockInterruptibly() throws InterruptedException {
            awaitingAcquisition.countDown();
            super.lockInterruptibly();
        }
    }
}
