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
import io.github.cocosip.stow.model.FileProcessingStatus;
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
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.junit.jupiter.api.Test;

/** Late completions after a timeout reclaim win, and empty claims reclaim stuck leases inline. */
class LateCompletionRecoveryTest {

    private static final Duration TIMEOUT = Duration.ofMinutes(30);

    @Test
    void emptyClaimReclaimsTimedOutLeasesInline() throws Exception {
        Fixture fixture = new Fixture();
        try (fixture) {
            String key = fixture.pool.write(fixture.tenant, ContentSources.of(new byte[] {1}), null);
            ProcessingLease stuck =
                    fixture.pool.claimNext(fixture.tenant).orElseThrow().lease();
            fixture.clock.advance(TIMEOUT.plusMinutes(1));

            // The empty queue is itself the reclaim trigger; the same file is
            // immediately claimable with a fresh lease instead of starving for a
            // full cleanup interval.
            ProcessingLease reclaimed =
                    fixture.pool.claimNext(fixture.tenant).orElseThrow().lease();

            assertThat(reclaimed.fileKey()).isEqualTo(key);
            assertThat(reclaimed.leaseId()).isNotEqualTo(stuck.leaseId());
        }
    }

    @Test
    void lateCompletionAfterReclaimConvergesTheFile() throws Exception {
        Fixture fixture = new Fixture();
        try (fixture) {
            String key = fixture.pool.write(fixture.tenant, ContentSources.of(new byte[] {1}), null);
            ProcessingLease lease =
                    fixture.pool.claimNext(fixture.tenant).orElseThrow().lease();
            fixture.clock.advance(TIMEOUT.plusMinutes(1));
            assertThat(fixture.pool.recoverTimedOut(TIMEOUT)).isEqualTo(1);

            // The worker finished while the lease was already reclaimed: the completion
            // wins so the finished work is not lost.
            fixture.pool.complete(lease);

            assertThat(fixture.pool.status(fixture.tenant, key)).isEqualTo(FileProcessingStatus.DELETE_REQUESTED);
            assertThat(fixture.pool.claimNext(fixture.tenant)).isEmpty();
        }
    }

    private static final class Fixture implements AutoCloseable {

        private final AdvanceableClock clock = new AdvanceableClock(Instant.parse("2026-09-20T00:00:00Z"));
        private final TenantContext tenant;
        private final FileQueueEventJournal journal;
        private final DefaultStoragePool pool;

        private Fixture() throws IOException {
            Path root = Files.createTempDirectory(Path.of("target"), "late-completion-");
            tenant = new TenantContext("tenant-a", TenantStatus.ENABLED, clock.instant(), clock.instant());
            SqliteQuotaRepository quota = new SqliteQuotaRepository(
                    root.resolve("quota"), SqliteConnectionFactory.defaults(), clock, ignored -> 20);
            SqliteMetadataProjectionStore metadata = new SqliteMetadataProjectionStore(
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

    private static final class AdvanceableClock extends Clock {

        private Instant instant;

        private AdvanceableClock(Instant instant) {
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
            throw new UnsupportedOperationException();
        }

        @Override
        public Instant instant() {
            return instant;
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
