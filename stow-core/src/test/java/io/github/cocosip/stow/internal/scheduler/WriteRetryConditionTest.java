package io.github.cocosip.stow.internal.scheduler;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.cocosip.stow.api.ContentSources;
import io.github.cocosip.stow.api.StoragePool;
import io.github.cocosip.stow.exception.StorageVolumeUnavailableException;
import io.github.cocosip.stow.internal.journal.JournalReadBatch;
import io.github.cocosip.stow.internal.projection.ProjectionCursorStore;
import io.github.cocosip.stow.internal.projection.QueueEventReducer;
import io.github.cocosip.stow.internal.projection.QueueProjectionService;
import io.github.cocosip.stow.internal.projection.SqliteMetadataProjectionStore;
import io.github.cocosip.stow.internal.quota.SqliteQuotaRepository;
import io.github.cocosip.stow.internal.sqlite.SqliteConnectionFactory;
import io.github.cocosip.stow.model.QueueEventRecord;
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
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import org.junit.jupiter.api.Test;

/**
 * Locus {@code IsRetryableWriteFailure}: only I/O-class failures advance to the next
 * volume candidate; any other RuntimeException stops candidate selection immediately.
 */
class WriteRetryConditionTest {

    @Test
    void ioFailureAdvancesToNextVolumeCandidate() throws Exception {
        Fixture fixture = new Fixture(new InjectedFailure(StorageVolumeUnavailableException.class, "disk offline"));

        String fileKey =
                fixture.pool.write(fixture.tenant, ContentSources.of("hello".getBytes()), WriteOptions.defaults());

        // Whatever the candidate order, the write converges on the healthy volume.
        assertThat(fixture.volumeB.files).hasSize(1);
        assertThat(fixture.volumeA.files).isEmpty();
        assertThat(fixture.quota.tenantCurrentCount("tenant-a")).isEqualTo(1);
    }

    @Test
    void nonIoFailureStopsCandidateSelection() throws Exception {
        Fixture fixture = new Fixture(new InjectedFailure(IllegalStateException.class, "corrupt content"));

        try {
            fixture.pool.write(fixture.tenant, ContentSources.of("hello".getBytes()), WriteOptions.defaults());
            // When the healthy volume drew the primary slot the write succeeds on it.
            assertThat(fixture.volumeB.files).hasSize(1);
        } catch (IllegalStateException expected) {
            // When the failing volume drew the primary slot, the non-I/O failure must
            // NOT advance: volume B was never attempted and the reservation was released.
            assertThat(fixture.volumeB.files).isEmpty();
            assertThat(fixture.volumeA.attempts).isEqualTo(1);
            assertThat(fixture.quota.tenantCurrentCount("tenant-a")).isZero();
        }
    }

    private record InjectedFailure(Class<? extends RuntimeException> type, String message) {}

    private static final class Fixture {
        private final TenantContext tenant =
                new TenantContext("tenant-a", TenantStatus.ENABLED, Instant.EPOCH, Instant.EPOCH);
        private final SqliteQuotaRepository quota;
        private final SqliteMetadataProjectionStore metadata;
        private final SelectiveVolume volumeA;
        private final SelectiveVolume volumeB;
        private final StoragePool pool;

        private Fixture(InjectedFailure failure) throws Exception {
            Path root = Files.createTempDirectory(Path.of("target"), "write-retry-");
            Clock clock = Clock.fixed(Instant.EPOCH, ZoneOffset.UTC);
            quota = new SqliteQuotaRepository(
                    root.resolve("quota"), SqliteConnectionFactory.defaults(), clock, ignored -> 10);
            metadata = new SqliteMetadataProjectionStore(root.resolve("metadata"));
            QueueEventJournal journal = new EmptyJournal();
            QueueProjectionService projection = new QueueProjectionService(
                    journal,
                    new QueueEventReducer(metadata, quota),
                    new ProjectionCursorStore(root.resolve("cursor"), clock));
            volumeA = new SelectiveVolume("volume-a", root.resolve("volume-a"), failure);
            volumeB = new SelectiveVolume("volume-b", root.resolve("volume-b"), null);
            pool = new DefaultStoragePool(
                    id -> tenant,
                    quota,
                    metadata,
                    projection,
                    journal,
                    List.of(volumeA, volumeB),
                    clock,
                    new io.github.cocosip.stow.config.RetryConfiguration(
                            3, java.time.Duration.ofSeconds(5), true, java.time.Duration.ofMinutes(5)));
        }
    }

    private static final class SelectiveVolume implements StorageVolume {
        private final String id;
        private final Path root;
        private final InjectedFailure failure;
        private final Map<Path, byte[]> files = new ConcurrentHashMap<>();
        private int attempts;

        private SelectiveVolume(String id, Path root, InjectedFailure failure) throws IOException {
            this.id = id;
            this.root = root;
            this.failure = failure;
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
            return 10_000;
        }

        @Override
        public Path buildPath(String tenantId, String fileKey, String extension) {
            return root.resolve(tenantId).resolve(fileKey + extension);
        }

        @Override
        public long write(Path target, InputStream content) {
            attempts++;
            if (failure != null) {
                try {
                    content.transferTo(ByteArrayOutputStream.nullOutputStream());
                } catch (IOException ignored) {
                }
                try {
                    throw failure.type().getConstructor(String.class).newInstance(failure.message());
                } catch (ReflectiveOperationException exception) {
                    throw new AssertionError(exception);
                }
            }
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
            byte[] bytes = files.get(path);
            if (bytes == null) {
                throw new io.github.cocosip.stow.exception.StoredFileNotFoundException("Stored file does not exist");
            }
            return new ByteArrayInputStream(bytes);
        }

        @Override
        public void delete(Path path) {
            files.remove(path);
        }

        @Override
        public void move(Path source, Path target) {
            files.put(target, files.remove(source));
        }

        @Override
        public void close() {}
    }

    private static final class EmptyJournal implements QueueEventJournal {
        @Override
        public long append(QueueEventRecord event) {
            return event.sequenceNumber();
        }

        @Override
        public JournalReadBatch readBatch(String tenantId, long offset, int maxRecords) {
            return new JournalReadBatch(tenantId, offset, offset, 0, List.of());
        }

        @Override
        public long tailOffset(String tenantId) {
            return 0;
        }

        @Override
        public long baseOffset(String tenantId) {
            return 0;
        }

        @Override
        public Set<String> tenantIds() {
            return Set.of();
        }

        @Override
        public void compact(String tenantId, long throughOffset) {}

        @Override
        public void flush() {}

        @Override
        public void close() {}
    }
}
