package io.github.cocosip.stow.internal.scheduler;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.cocosip.stow.api.ContentSources;
import io.github.cocosip.stow.config.JournalAckMode;
import io.github.cocosip.stow.config.JournalConfiguration;
import io.github.cocosip.stow.config.JournalFormat;
import io.github.cocosip.stow.internal.journal.BinaryV1JournalCodec;
import io.github.cocosip.stow.internal.journal.FileQueueEventJournal;
import io.github.cocosip.stow.internal.projection.ProjectionCursorStore;
import io.github.cocosip.stow.internal.projection.QueueEventReducer;
import io.github.cocosip.stow.internal.projection.QueueProjectionService;
import io.github.cocosip.stow.internal.projection.SqliteMetadataProjectionStore;
import io.github.cocosip.stow.internal.quota.SqliteQuotaRepository;
import io.github.cocosip.stow.internal.sqlite.SqliteConnectionFactory;
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
import java.time.ZoneId;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.junit.jupiter.api.Test;

class ProcessingTimeoutRecoveryTest {

    @Test
    void invalidatesOldLeaseWithoutIncreasingRetryCount() throws Exception {
        Path root = Files.createTempDirectory(Path.of("target"), "scheduler-timeout-");
        MutableClock clock = new MutableClock(Instant.parse("2026-09-19T00:00:00Z"));
        TenantContext tenant = new TenantContext("tenant-a", TenantStatus.ENABLED, clock.instant(), clock.instant());
        SqliteQuotaRepository quota = new SqliteQuotaRepository(
                root.resolve("quota"), SqliteConnectionFactory.defaults(), clock, ignored -> 20);
        SqliteMetadataProjectionStore metadata =
                new SqliteMetadataProjectionStore(root.resolve("metadata"), SqliteConnectionFactory.defaults(), clock);
        try (FileQueueEventJournal journal =
                new FileQueueEventJournal(root.resolve("journal"), configuration(), new BinaryV1JournalCodec())) {
            QueueProjectionService projection = new QueueProjectionService(
                    journal,
                    new QueueEventReducer(metadata, quota),
                    new ProjectionCursorStore(root.resolve("cursor"), clock),
                    clock,
                    null);
            DefaultStoragePool pool = new DefaultStoragePool(
                    id -> tenant.tenantId().equals(id) ? tenant : null,
                    quota,
                    metadata,
                    projection,
                    journal,
                    new TestVolume(root.resolve("volume")),
                    clock);
            pool.write(tenant, ContentSources.of(new byte[] {1}), null);
            var claimed = pool.claimNext(tenant).orElseThrow();
            clock.advance(Duration.ofMinutes(10));

            assertThat(new ProcessingTimeoutRecovery(pool).recover(Duration.ofMinutes(1)))
                    .isEqualTo(1);
            assertThat(pool.status(tenant, claimed.location().fileKey()))
                    .isEqualTo(io.github.cocosip.stow.model.FileProcessingStatus.PENDING);
            assertThat(pool.claimNext(tenant)).isPresent();
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

    private static final class MutableClock extends Clock {
        private Instant current;

        private MutableClock(Instant initial) {
            current = initial;
        }

        private void advance(Duration duration) {
            current = current.plus(duration);
        }

        @Override
        public ZoneId getZone() {
            return ZoneId.of("UTC");
        }

        @Override
        public Clock withZone(ZoneId zone) {
            return this;
        }

        @Override
        public Instant instant() {
            return current;
        }
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
