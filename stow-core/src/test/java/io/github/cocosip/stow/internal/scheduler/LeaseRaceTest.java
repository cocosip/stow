package io.github.cocosip.stow.internal.scheduler;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.github.cocosip.stow.api.ContentSources;
import io.github.cocosip.stow.config.JournalAckMode;
import io.github.cocosip.stow.config.JournalConfiguration;
import io.github.cocosip.stow.config.JournalFormat;
import io.github.cocosip.stow.exception.LeaseMismatchException;
import io.github.cocosip.stow.internal.journal.BinaryV1JournalCodec;
import io.github.cocosip.stow.internal.journal.FileQueueEventJournal;
import io.github.cocosip.stow.internal.projection.ProjectionCursorStore;
import io.github.cocosip.stow.internal.projection.QueueEventReducer;
import io.github.cocosip.stow.internal.projection.QueueProjectionService;
import io.github.cocosip.stow.internal.projection.SqliteMetadataProjectionStore;
import io.github.cocosip.stow.internal.quota.SqliteQuotaRepository;
import io.github.cocosip.stow.internal.sqlite.SqliteConnectionFactory;
import io.github.cocosip.stow.model.ProcessingLease;
import io.github.cocosip.stow.model.TenantContext;
import io.github.cocosip.stow.model.TenantStatus;
import io.github.cocosip.stow.spi.StorageVolume;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import org.junit.jupiter.api.Test;

class LeaseRaceTest {

    @Test
    void duplicateCompletionIsIdempotentButDifferentLeaseIsRejected() throws Exception {
        Fixture fixture = new Fixture();
        try (fixture) {
            String key = fixture.pool.write(fixture.tenant, ContentSources.of(new byte[] {1}), null);
            ProcessingLease lease =
                    fixture.pool.claimNext(fixture.tenant).orElseThrow().lease();
            fixture.pool.complete(lease);
            fixture.pool.complete(lease);
            ProcessingLease stale =
                    new ProcessingLease(fixture.tenant.tenantId(), key, UUID.randomUUID(), lease.startedAt());
            assertThatThrownBy(() -> fixture.pool.complete(stale)).isInstanceOf(LeaseMismatchException.class);
        }
    }

    @Test
    void crossTenantLeaseCannotCompleteAnotherTenantFile() throws Exception {
        Fixture fixture = new Fixture();
        try (fixture) {
            fixture.pool.write(fixture.tenant, ContentSources.of(new byte[] {1}), null);
            ProcessingLease lease =
                    fixture.pool.claimNext(fixture.tenant).orElseThrow().lease();
            ProcessingLease crossTenant =
                    new ProcessingLease("tenant-b", lease.fileKey(), lease.leaseId(), lease.startedAt());
            assertThatThrownBy(() -> fixture.pool.complete(crossTenant)).isInstanceOf(LeaseMismatchException.class);
        }
    }

    @Test
    void completeAndFailRaceHasOneDeterministicWinner() throws Exception {
        Fixture fixture = new Fixture();
        try (fixture) {
            fixture.pool.write(fixture.tenant, ContentSources.of(new byte[] {1}), null);
            ProcessingLease lease =
                    fixture.pool.claimNext(fixture.tenant).orElseThrow().lease();
            ExecutorService executor = Executors.newFixedThreadPool(2);
            try {
                Future<?> complete = executor.submit(() -> fixture.pool.complete(lease));
                Future<?> fail = executor.submit(() -> fixture.pool.fail(lease, "race"));
                int successes = 0;
                int mismatches = 0;
                for (Future<?> outcome : new Future<?>[] {complete, fail}) {
                    try {
                        outcome.get();
                        successes++;
                    } catch (ExecutionException exception) {
                        if (exception.getCause() instanceof LeaseMismatchException) {
                            mismatches++;
                        } else {
                            throw exception;
                        }
                    }
                }
                assertThat(successes).isEqualTo(1);
                assertThat(mismatches).isEqualTo(1);
            } finally {
                executor.shutdownNow();
            }
        }
    }

    private static final class Fixture implements AutoCloseable {
        private final Path root;
        private final Clock clock;
        private final TenantContext tenant;
        private final SqliteQuotaRepository quota;
        private final SqliteMetadataProjectionStore metadata;
        private final FileQueueEventJournal journal;
        private final DefaultStoragePool pool;

        private Fixture() throws IOException {
            root = Files.createTempDirectory(Path.of("target"), "lease-race-");
            clock = Clock.fixed(Instant.parse("2026-09-19T00:00:00Z"), ZoneOffset.UTC);
            tenant = new TenantContext("tenant-a", TenantStatus.ENABLED, clock.instant(), clock.instant());
            quota = new SqliteQuotaRepository(
                    root.resolve("quota"), SqliteConnectionFactory.defaults(), clock, ignored -> 20);
            metadata = new SqliteMetadataProjectionStore(
                    root.resolve("metadata"), SqliteConnectionFactory.defaults(), clock);
            journal = new FileQueueEventJournal(root.resolve("journal"), configuration(), new BinaryV1JournalCodec());
            QueueProjectionService projection = new QueueProjectionService(
                    journal,
                    new QueueEventReducer(metadata, quota),
                    new ProjectionCursorStore(root.resolve("cursor"), clock),
                    clock,
                    null);
            pool = new DefaultStoragePool(
                    id -> tenant.tenantId().equals(id) ? tenant : null,
                    quota,
                    metadata,
                    projection,
                    journal,
                    new TestVolume(root.resolve("volume")),
                    clock);
        }

        @Override
        public void close() {
            journal.close();
        }
    }

    private static JournalConfiguration configuration() {
        return new JournalConfiguration(
                true,
                true,
                JournalFormat.BINARY_V1,
                JournalAckMode.DURABLE,
                Duration.ZERO,
                Duration.ZERO,
                16,
                262_144,
                Duration.ofSeconds(30),
                16,
                Duration.ZERO);
    }

    private static final class TestVolume implements StorageVolume {
        private final Path root;
        private final Map<Path, byte[]> files = new ConcurrentHashMap<>();

        private TestVolume(Path root) throws IOException {
            this.root = root;
            Files.createDirectories(root);
        }

        @Override
        public String id() {
            return "volume-a";
        }

        @Override
        public Path mountPath() {
            return root;
        }

        @Override
        public boolean healthy() {
            return true;
        }

        @Override
        public long totalCapacity() {
            return 1_000_000;
        }

        @Override
        public long availableCapacity() {
            return 1_000_000;
        }

        @Override
        public Path buildPath(String tenantId, String fileKey, String extension) {
            return root.resolve(tenantId).resolve(fileKey + extension);
        }

        @Override
        public long write(Path target, InputStream content) {
            try {
                Files.createDirectories(target.getParent());
                ByteArrayOutputStream out = new ByteArrayOutputStream();
                content.transferTo(out);
                byte[] bytes = out.toByteArray();
                files.put(target, bytes);
                Files.write(target, bytes);
                return bytes.length;
            } catch (IOException exception) {
                throw new RuntimeException(exception);
            }
        }

        @Override
        public InputStream read(Path path) {
            return new java.io.ByteArrayInputStream(files.get(path));
        }

        @Override
        public void delete(Path path) {
            files.remove(path);
            try {
                Files.deleteIfExists(path);
            } catch (IOException ignored) {
            }
        }

        @Override
        public void move(Path source, Path target) {
            try {
                Files.createDirectories(target.getParent());
                byte[] bytes = files.remove(source);
                files.put(target, bytes);
                Files.move(source, target);
            } catch (IOException exception) {
                throw new RuntimeException(exception);
            }
        }

        @Override
        public void close() {}
    }
}
