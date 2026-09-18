package io.github.cocosip.stow.internal.scheduler;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.github.cocosip.stow.api.ContentSources;
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
import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Set;
import org.junit.jupiter.api.Test;

class StorageWriteFailureTest {

    @Test
    void physicalWriteFailureRollsBackQuotaReservation() throws Exception {
        Fixture fixture = new Fixture(true, false);

        assertThatThrownBy(() -> fixture.pool.write(
                        fixture.tenant, ContentSources.of(new byte[] {1, 2}), WriteOptions.defaults()))
                .isInstanceOf(RuntimeException.class);

        assertThat(fixture.quota.tenantCurrentCount("tenant-a")).isZero();
    }

    @Test
    void journalFailureKeepsPublishedFileAndQuotaForRecovery() throws Exception {
        Fixture fixture = new Fixture(false, true);

        assertThatThrownBy(() -> fixture.pool.write(
                        fixture.tenant, ContentSources.of(new byte[] {1, 2}), WriteOptions.defaults()))
                .isInstanceOf(RuntimeException.class);

        assertThat(fixture.quota.tenantCurrentCount("tenant-a")).isEqualTo(1);
        assertThat(fixture.volume.published).isTrue();
    }

    private static final class Fixture {
        private final TenantContext tenant =
                new TenantContext("tenant-a", TenantStatus.ENABLED, Instant.EPOCH, Instant.EPOCH);
        private final SqliteQuotaRepository quota;
        private final FailingVolume volume;
        private final DefaultStoragePool pool;

        private Fixture(boolean failWrite, boolean failJournal) throws Exception {
            Path root = Files.createTempDirectory(Path.of("target"), "storage-write-failure-");
            Clock clock = Clock.fixed(Instant.EPOCH, ZoneOffset.UTC);
            quota = new SqliteQuotaRepository(
                    root.resolve("quota"), SqliteConnectionFactory.defaults(), clock, ignored -> 10);
            SqliteMetadataProjectionStore metadata = new SqliteMetadataProjectionStore(root.resolve("metadata"));
            QueueEventJournal journal = new FailingJournal(failJournal);
            QueueProjectionService projection = new QueueProjectionService(
                    journal,
                    new QueueEventReducer(metadata, quota),
                    new ProjectionCursorStore(root.resolve("cursor"), clock));
            volume = new FailingVolume(root.resolve("volume"), failWrite);
            pool = new DefaultStoragePool(id -> tenant, quota, metadata, projection, journal, volume, clock);
        }
    }

    private static final class FailingVolume implements StorageVolume {
        private final Path root;
        private final boolean failWrite;
        private boolean published;

        private FailingVolume(Path root, boolean failWrite) throws Exception {
            this.root = root;
            this.failWrite = failWrite;
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
            return 1000;
        }

        @Override
        public long availableCapacity() {
            return 1000;
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
                Files.write(target, output.toByteArray());
                if (failWrite) throw new RuntimeException("injected physical write failure");
                return output.size();
            } catch (Exception exception) {
                if (exception instanceof RuntimeException runtime) throw runtime;
                throw new RuntimeException(exception);
            }
        }

        @Override
        public InputStream read(Path path) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void delete(Path path) {
            try {
                Files.deleteIfExists(path);
            } catch (Exception ignored) {
            }
        }

        @Override
        public void move(Path source, Path target) {
            try {
                Files.createDirectories(target.getParent());
                Files.move(source, target);
                published = true;
            } catch (Exception exception) {
                throw new RuntimeException(exception);
            }
        }

        @Override
        public void close() {}
    }

    private static final class FailingJournal implements QueueEventJournal {
        private final boolean fail;

        private FailingJournal(boolean fail) {
            this.fail = fail;
        }

        @Override
        public long append(QueueEventRecord event) {
            if (fail) throw new RuntimeException("injected journal failure");
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
