package io.github.cocosip.stow.internal.scheduler;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.github.cocosip.stow.api.ContentSources;
import io.github.cocosip.stow.api.IdempotentStoragePool;
import io.github.cocosip.stow.exception.StoredFileNotFoundException;
import io.github.cocosip.stow.internal.journal.BinaryV1JournalCodec;
import io.github.cocosip.stow.internal.journal.FileQueueEventJournal;
import io.github.cocosip.stow.internal.projection.ActiveFileCache;
import io.github.cocosip.stow.internal.projection.ProjectionCursorStore;
import io.github.cocosip.stow.internal.projection.QueueEventReducer;
import io.github.cocosip.stow.internal.projection.QueueProjectionService;
import io.github.cocosip.stow.internal.projection.SqliteMetadataProjectionStore;
import io.github.cocosip.stow.internal.quota.SqliteQuotaRepository;
import io.github.cocosip.stow.internal.sqlite.SqliteConnectionFactory;
import io.github.cocosip.stow.model.TenantContext;
import io.github.cocosip.stow.model.TenantStatus;
import io.github.cocosip.stow.model.WriteOptions;
import io.github.cocosip.stow.spi.QueueEventJournal;
import io.github.cocosip.stow.spi.StorageVolume;
import java.io.ByteArrayInputStream;
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
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import org.junit.jupiter.api.Test;

class StorageWriteReadTest {

    @Test
    void writesRepeatableContentProjectsMetadataAndReadsOnlyForOwner() throws Exception {
        Path root = Files.createTempDirectory(Path.of("target"), "storage-write-read-");
        Clock clock = Clock.fixed(Instant.parse("2026-09-18T00:00:00Z"), ZoneOffset.UTC);
        TenantContext tenant = tenant("tenant-a", TenantStatus.ENABLED, clock);
        SqliteQuotaRepository quota = new SqliteQuotaRepository(
                root.resolve("quota"), SqliteConnectionFactory.defaults(), clock, ignored -> 10);
        SqliteMetadataProjectionStore metadata =
                new SqliteMetadataProjectionStore(root.resolve("metadata"), SqliteConnectionFactory.defaults(), clock);
        QueueEventJournal journal =
                new FileQueueEventJournal(root.resolve("journal"), journalConfiguration(), new BinaryV1JournalCodec());
        QueueProjectionService projection = new QueueProjectionService(
                journal,
                new QueueEventReducer(metadata, quota),
                new ProjectionCursorStore(root.resolve("cursor"), clock),
                clock,
                new ActiveFileCache(metadata));
        TestVolume volume = new TestVolume("volume-a", root.resolve("volume"));
        DefaultStoragePool pool =
                new DefaultStoragePool(tenantManager(tenant), quota, metadata, projection, journal, volume, clock);

        String fileKey =
                pool.write(tenant, ContentSources.of("hello".getBytes()), new WriteOptions("scan.dcm", "studies"));

        assertThat(pool.findFileInfo(tenant, fileKey)).isPresent().get().satisfies(info -> {
            assertThat(info.fileSize()).isEqualTo(5);
            assertThat(info.fileExtension()).isEqualTo(".dcm");
        });
        assertThat(pool.findFileLocation(tenant, fileKey)).isPresent().get().satisfies(location -> {
            assertThat(location.logicalDirectory()).isEqualTo("/studies");
            assertThat(location.volumeId()).isEqualTo("volume-a");
        });
        try (InputStream input = pool.read(tenant, fileKey)) {
            assertThat(input.readAllBytes()).containsExactly("hello".getBytes());
        }
        TenantContext other = tenant("tenant-b", TenantStatus.ENABLED, clock);
        assertThat(pool.findFileInfo(other, fileKey)).isEmpty();
        assertThatThrownBy(() -> pool.read(other, fileKey)).isInstanceOf(StoredFileNotFoundException.class);
        assertThat(quota.tenantCurrentCount("tenant-a")).isEqualTo(1);
        assertThat(pool.totalCapacity()).isEqualTo(10_000);
        assertThat(pool.availableCapacity()).isEqualTo(9_995);
        journal.close();
    }

    @Test
    void inputStreamWriteDoesNotCloseCallerStream() throws Exception {
        Path root = Files.createTempDirectory(Path.of("target"), "storage-write-stream-");
        Clock clock = Clock.fixed(Instant.EPOCH, ZoneOffset.UTC);
        TenantContext tenant = tenant("tenant-a", TenantStatus.ENABLED, clock);
        SqliteQuotaRepository quota = new SqliteQuotaRepository(
                root.resolve("quota"), SqliteConnectionFactory.defaults(), clock, ignored -> 10);
        SqliteMetadataProjectionStore metadata = new SqliteMetadataProjectionStore(root.resolve("metadata"));
        QueueEventJournal journal = new RecordingJournal();
        QueueProjectionService projection = new QueueProjectionService(
                journal,
                new QueueEventReducer(metadata, quota),
                new ProjectionCursorStore(root.resolve("cursor"), clock));
        TestVolume volume = new TestVolume("volume-a", root.resolve("volume"));
        DefaultStoragePool pool =
                new DefaultStoragePool(tenantManager(tenant), quota, metadata, projection, journal, volume, clock);
        TrackingInputStream input = new TrackingInputStream("abc".getBytes());

        pool.write(tenant, input, WriteOptions.defaults());

        assertThat(input.closed).isFalse();
    }

    @Test
    void deduplicatesConcurrentWritesByTenantAndOperationId() throws Exception {
        Path root = Files.createTempDirectory(Path.of("target"), "storage-idempotent-");
        Clock clock = Clock.fixed(Instant.parse("2026-09-18T00:00:00Z"), ZoneOffset.UTC);
        TenantContext tenant = tenant("tenant-a", TenantStatus.ENABLED, clock);
        SqliteQuotaRepository quota = new SqliteQuotaRepository(
                root.resolve("quota"), SqliteConnectionFactory.defaults(), clock, ignored -> 10);
        SqliteMetadataProjectionStore metadata =
                new SqliteMetadataProjectionStore(root.resolve("metadata"), SqliteConnectionFactory.defaults(), clock);
        QueueEventJournal journal =
                new FileQueueEventJournal(root.resolve("journal"), journalConfiguration(), new BinaryV1JournalCodec());
        QueueProjectionService projection = new QueueProjectionService(
                journal,
                new QueueEventReducer(metadata, quota),
                new ProjectionCursorStore(root.resolve("cursor"), clock),
                clock,
                new ActiveFileCache(metadata));
        TestVolume volume = new TestVolume("volume-a", root.resolve("volume"));
        IdempotentStoragePool pool =
                new DefaultStoragePool(tenantManager(tenant), quota, metadata, projection, journal, volume, clock);

        String first;
        String second;
        try (var executor = Executors.newVirtualThreadPerTaskExecutor()) {
            var one = executor.submit(() -> pool.writeIdempotently(
                    tenant, ContentSources.of("hello".getBytes()), WriteOptions.defaults(), "operation-1"));
            var two = executor.submit(() -> pool.writeIdempotently(
                    tenant, ContentSources.of("hello".getBytes()), WriteOptions.defaults(), "operation-1"));
            first = one.get();
            second = two.get();
        }

        assertThat(second).isEqualTo(first);
        assertThat(volume.files).hasSize(1);
        assertThat(quota.tenantCurrentCount(tenant.tenantId())).isEqualTo(1);
        assertThat(journal.readBatch(tenant.tenantId(), 0, 10).events())
                .filteredOn(event -> event.eventType() == io.github.cocosip.stow.model.QueueEventType.ACCEPTED)
                .singleElement()
                .extracting(io.github.cocosip.stow.model.QueueEventRecord::importOperationId)
                .isEqualTo("operation-1");
        IdempotentStoragePool reopened =
                new DefaultStoragePool(tenantManager(tenant), quota, metadata, projection, journal, volume, clock);
        assertThat(reopened.writeIdempotently(
                        tenant, ContentSources.of("ignored".getBytes()), WriteOptions.defaults(), "operation-1"))
                .isEqualTo(first);
        assertThat(volume.files).hasSize(1);
        journal.close();
    }

    private static DefaultStoragePool.TenantLookup tenantManager(TenantContext expected) {
        return id -> expected.tenantId().equals(id) ? expected : null;
    }

    private static TenantContext tenant(String id, TenantStatus status, Clock clock) {
        return new TenantContext(id, status, clock.instant(), clock.instant());
    }

    private static io.github.cocosip.stow.config.JournalConfiguration journalConfiguration() {
        return new io.github.cocosip.stow.config.JournalConfiguration(
                true,
                true,
                io.github.cocosip.stow.config.JournalFormat.BINARY_V1,
                io.github.cocosip.stow.config.JournalAckMode.DURABLE,
                Duration.ZERO,
                Duration.ZERO,
                16,
                262_144,
                Duration.ofSeconds(30),
                16,
                Duration.ZERO);
    }

    private static final class TrackingInputStream extends ByteArrayInputStream {
        private boolean closed;

        private TrackingInputStream(byte[] bytes) {
            super(bytes);
        }

        @Override
        public void close() {
            closed = true;
        }
    }

    private static final class TestVolume implements StorageVolume {
        private final String id;
        private final Path root;
        private final Map<Path, byte[]> files = new ConcurrentHashMap<>();

        private TestVolume(String id, Path root) throws IOException {
            this.id = id;
            this.root = root;
            Files.createDirectories(root);
        }

        @Override
        public String id() {
            return id;
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
            return 10_000;
        }

        @Override
        public long availableCapacity() {
            return 10_000
                    - files.values().stream().mapToLong(value -> value.length).sum();
        }

        @Override
        public Path buildPath(String tenantId, String fileKey, String extension) {
            return root.resolve(tenantId).resolve(fileKey + extension);
        }

        @Override
        public long write(Path target, InputStream content) {
            try {
                Files.createDirectories(target.getParent());
                ByteArrayOutputStream output = new ByteArrayOutputStream();
                content.transferTo(output);
                byte[] bytes = output.toByteArray();
                files.put(target, bytes);
                Files.write(target, bytes);
                return bytes.length;
            } catch (IOException exception) {
                throw new RuntimeException(exception);
            }
        }

        @Override
        public InputStream read(Path path) {
            return new ByteArrayInputStream(files.get(path));
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

    private static final class RecordingJournal implements QueueEventJournal {
        private long sequence;

        @Override
        public long append(io.github.cocosip.stow.model.QueueEventRecord event) {
            sequence = event.sequenceNumber();
            return sequence;
        }

        @Override
        public io.github.cocosip.stow.internal.journal.JournalReadBatch readBatch(
                String tenantId, long offset, int maxRecords) {
            return new io.github.cocosip.stow.internal.journal.JournalReadBatch(
                    tenantId, offset, offset, sequence, java.util.List.of());
        }

        @Override
        public long tailOffset(String tenantId) {
            return sequence;
        }

        @Override
        public long baseOffset(String tenantId) {
            return 0;
        }

        @Override
        public java.util.Set<String> tenantIds() {
            return java.util.Set.of();
        }

        @Override
        public void compact(String tenantId, long throughOffset) {}

        @Override
        public void flush() {}

        @Override
        public void close() {}
    }
}
