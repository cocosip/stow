package io.github.cocosip.stow.internal.recovery;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.cocosip.stow.config.SqliteConfiguration;
import io.github.cocosip.stow.internal.journal.JournalReadBatch;
import io.github.cocosip.stow.internal.projection.ProjectionCursorStore;
import io.github.cocosip.stow.internal.projection.QueueEventReducer;
import io.github.cocosip.stow.internal.projection.QueueProjectionService;
import io.github.cocosip.stow.internal.projection.SqliteMetadataProjectionStore;
import io.github.cocosip.stow.internal.quota.SqliteQuotaRepository;
import io.github.cocosip.stow.internal.sqlite.SqliteConnectionFactory;
import io.github.cocosip.stow.model.QueueEventRecord;
import io.github.cocosip.stow.spi.QueueEventJournal;
import io.github.cocosip.stow.spi.StorageVolume;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import org.junit.jupiter.api.Test;

class OrphanFileRecoveryTest {

    @Test
    void recoversPhysicalFileOnceAndConsumesAReservationThroughProjection() throws Exception {
        Path root = Files.createTempDirectory(Path.of("target"), "orphan-");
        Path mount = root.resolve("volume");
        String tenant = "tenant-a";
        String key = "fedcba9876543210fedcba9876543210";
        Path file = mount.resolve(tenant).resolve(key + ".dcm");
        Files.createDirectories(file.getParent());
        Files.writeString(file, "orphan");
        Clock clock = Clock.fixed(Instant.parse("2026-09-19T00:00:00Z"), ZoneOffset.UTC);
        SqliteConfiguration sqlite = SqliteConnectionFactory.defaults();
        SqliteMetadataProjectionStore metadata =
                new SqliteMetadataProjectionStore(root.resolve("metadata"), sqlite, clock);
        SqliteQuotaRepository quota = new SqliteQuotaRepository(root.resolve("quota"), sqlite, clock, ignored -> 10);
        TestJournal journal = new TestJournal();
        QueueProjectionService projection = new QueueProjectionService(
                journal,
                new QueueEventReducer(metadata, quota),
                new ProjectionCursorStore(root.resolve("cursor"), clock),
                clock,
                null);
        OrphanFileRecovery recovery = new OrphanFileRecovery(
                journal, metadata, quota, projection, List.of(new TestVolume("volume-a", mount)), clock);

        assertThat(recovery.recover(tenant, 100).succeededCount()).isEqualTo(1);
        assertThat(recovery.recover(tenant, 100).skippedCount()).isEqualTo(1);
        assertThat(metadata.find(tenant, key)).isPresent();
        assertThat(quota.tenantCurrentCount(tenant)).isEqualTo(1);
        assertThat(journal.events).hasSize(1);
    }

    @Test
    void skipsPhysicalFileWhenItsAcceptedEventAlreadyExists() throws Exception {
        Path root = Files.createTempDirectory(Path.of("target"), "orphan-event-");
        Path mount = root.resolve("volume");
        String tenant = "tenant-a";
        String key = "fedcba9876543210fedcba9876543210";
        Path file = mount.resolve(tenant).resolve(key + ".dcm");
        Files.createDirectories(file.getParent());
        Files.writeString(file, "orphan");
        TestJournal journal = new TestJournal();
        journal.events.add(new QueueEventRecord(
                1,
                java.util.UUID.randomUUID(),
                tenant,
                key,
                io.github.cocosip.stow.model.QueueEventType.ACCEPTED,
                Instant.EPOCH,
                1,
                "volume-a",
                file,
                "/",
                6,
                io.github.cocosip.stow.model.FileProcessingStatus.PENDING,
                null,
                null,
                0,
                null,
                null,
                "x.dcm",
                ".dcm"));
        Clock clock = Clock.fixed(Instant.parse("2026-09-19T00:00:00Z"), ZoneOffset.UTC);
        SqliteMetadataProjectionStore metadata =
                new SqliteMetadataProjectionStore(root.resolve("metadata"), SqliteConnectionFactory.defaults(), clock);
        SqliteQuotaRepository quota = new SqliteQuotaRepository(
                root.resolve("quota"), SqliteConnectionFactory.defaults(), clock, ignored -> 10);
        QueueProjectionService projection = new QueueProjectionService(
                journal,
                new QueueEventReducer(metadata, quota),
                new ProjectionCursorStore(root.resolve("cursor"), clock),
                clock,
                null);
        OrphanFileRecovery recovery = new OrphanFileRecovery(
                journal, metadata, quota, projection, List.of(new TestVolume("volume-a", mount)), clock);

        assertThat(recovery.recover(tenant, 100).skippedCount()).isEqualTo(1);
        assertThat(journal.events).hasSize(1);
    }

    @Test
    void enforcesMaximumOrphanScanLimit() throws Exception {
        Path root = Files.createTempDirectory(Path.of("target"), "orphan-limit-");
        Path mount = root.resolve("volume");
        Path tenantRoot = mount.resolve("tenant-a");
        Files.createDirectories(tenantRoot);
        Files.writeString(tenantRoot.resolve("0123456789abcdef0123456789abcdef.bin"), "one");
        Files.writeString(tenantRoot.resolve("fedcba9876543210fedcba9876543210.bin"), "two");
        Clock clock = Clock.fixed(Instant.parse("2026-09-19T00:00:00Z"), ZoneOffset.UTC);
        SqliteMetadataProjectionStore metadata =
                new SqliteMetadataProjectionStore(root.resolve("metadata"), SqliteConnectionFactory.defaults(), clock);
        SqliteQuotaRepository quota = new SqliteQuotaRepository(
                root.resolve("quota"), SqliteConnectionFactory.defaults(), clock, ignored -> 10);
        TestJournal journal = new TestJournal();
        QueueProjectionService projection = new QueueProjectionService(
                journal,
                new QueueEventReducer(metadata, quota),
                new ProjectionCursorStore(root.resolve("cursor"), clock),
                clock,
                null);
        OrphanFileRecovery recovery = new OrphanFileRecovery(
                journal, metadata, quota, projection, List.of(new TestVolume("volume-a", mount)), clock);

        assertThat(recovery.recover("tenant-a", 1).scannedCount()).isEqualTo(1);
        assertThat(journal.events).hasSize(1);
    }

    private static final class TestJournal implements QueueEventJournal {
        private final List<QueueEventRecord> events = new ArrayList<>();

        @Override
        public long append(QueueEventRecord event) {
            events.add(event);
            return events.size();
        }

        @Override
        public JournalReadBatch readBatch(String tenantId, long offset, int maxRecords) {
            if (offset >= events.size())
                return new JournalReadBatch(
                        tenantId,
                        offset,
                        events.size(),
                        events.isEmpty() ? 0 : events.get(events.size() - 1).sequenceNumber(),
                        List.of());
            List<QueueEventRecord> selected = events.subList(
                    Math.toIntExact(offset), Math.min(events.size(), Math.toIntExact(offset) + maxRecords));
            return new JournalReadBatch(
                    tenantId,
                    offset,
                    offset + selected.size(),
                    selected.get(selected.size() - 1).sequenceNumber(),
                    selected);
        }

        @Override
        public long tailOffset(String tenantId) {
            return events.size();
        }

        @Override
        public long baseOffset(String tenantId) {
            return 0;
        }

        @Override
        public Set<String> tenantIds() {
            return Set.of("tenant-a");
        }

        @Override
        public void compact(String tenantId, long throughOffset) {}

        @Override
        public void flush() {}

        @Override
        public void close() {}
    }

    private static final class TestVolume implements StorageVolume {
        private final String id;
        private final Path mount;

        private TestVolume(String id, Path mount) {
            this.id = id;
            this.mount = mount;
        }

        @Override
        public String id() {
            return id;
        }

        @Override
        public Path mountPath() {
            return mount;
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
            return mount.resolve(tenantId).resolve(fileKey + extension);
        }

        @Override
        public long write(Path target, InputStream content) {
            throw new UnsupportedOperationException();
        }

        @Override
        public InputStream read(Path path) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void delete(Path path) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void move(Path source, Path target) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void close() {}
    }
}
