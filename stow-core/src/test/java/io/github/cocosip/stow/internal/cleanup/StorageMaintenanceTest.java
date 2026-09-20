package io.github.cocosip.stow.internal.cleanup;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.cocosip.stow.config.PermanentlyFailedDisposition;
import io.github.cocosip.stow.config.SqliteConfiguration;
import io.github.cocosip.stow.internal.journal.JournalReadBatch;
import io.github.cocosip.stow.internal.projection.ProjectionCursorStore;
import io.github.cocosip.stow.internal.projection.QueueEventReducer;
import io.github.cocosip.stow.internal.projection.QueueProjectionService;
import io.github.cocosip.stow.internal.projection.SqliteMetadataProjectionStore;
import io.github.cocosip.stow.internal.quota.SqliteQuotaRepository;
import io.github.cocosip.stow.internal.sqlite.SqliteConnectionFactory;
import io.github.cocosip.stow.model.FileProcessingStatus;
import io.github.cocosip.stow.model.QueueEventRecord;
import io.github.cocosip.stow.model.QueueEventType;
import io.github.cocosip.stow.spi.QueueEventJournal;
import io.github.cocosip.stow.spi.StorageVolume;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import org.junit.jupiter.api.Test;

class StorageMaintenanceTest {

    private static final String TENANT = "tenant-a";
    private static final String KEY = "0123456789abcdef0123456789abcdef";
    private static final Instant NOW = Instant.parse("2026-09-19T00:00:00Z");

    @Test
    void completedFilesUseTwoPhaseDeleteAndReleaseQuotaAfterSuccess() throws Exception {
        Path root = Files.createTempDirectory(Path.of("target"), "maintenance-");
        Path mount = root.resolve("volume");
        Path physical = mount.resolve(TENANT).resolve(KEY + ".bin");
        Files.createDirectories(physical.getParent());
        Files.writeString(physical, "payload");
        SqliteConfiguration sqlite = SqliteConnectionFactory.defaults();
        SqliteMetadataProjectionStore metadata =
                new SqliteMetadataProjectionStore(root.resolve("metadata"), sqlite, Clock.fixed(NOW, ZoneOffset.UTC));
        SqliteQuotaRepository quota = new SqliteQuotaRepository(
                root.resolve("quota"), sqlite, Clock.fixed(NOW, ZoneOffset.UTC), ignored -> 10);
        UUID lease = UUID.randomUUID();
        TestJournal journal = new TestJournal(List.of(
                event(1, QueueEventType.ACCEPTED, FileProcessingStatus.PENDING, null, physical),
                event(2, QueueEventType.PROCESSING_STARTED, FileProcessingStatus.PROCESSING, lease, physical),
                event(3, QueueEventType.PROCESSING_COMPLETED, FileProcessingStatus.COMPLETED, lease, physical)));
        quota.reserve(TENANT, KEY, "/");
        QueueEventReducer reducer = new QueueEventReducer(metadata, quota);
        QueueProjectionService projection = new QueueProjectionService(
                journal,
                reducer,
                new ProjectionCursorStore(root.resolve("cursor"), Clock.fixed(NOW, ZoneOffset.UTC)),
                Clock.fixed(NOW, ZoneOffset.UTC),
                null);
        projection.projectTenantUntilCaughtUp(TENANT, 32);

        CompletedFileReaper reaper = new CompletedFileReaper(
                journal,
                metadata,
                projection,
                List.of(new TestVolume("volume-a", mount)),
                Clock.fixed(NOW, ZoneOffset.UTC));
        reaper.run(java.time.Duration.ZERO, 100);

        assertThat(Files.exists(physical)).isFalse();
        assertThat(metadata.find(TENANT, KEY)).isEmpty();
        assertThat(quota.tenantCurrentCount(TENANT)).isZero();
        assertThat(journal.events)
                .extracting(QueueEventRecord::eventType)
                .containsExactly(
                        QueueEventType.ACCEPTED,
                        QueueEventType.PROCESSING_STARTED,
                        QueueEventType.PROCESSING_COMPLETED,
                        QueueEventType.DELETE_REQUESTED,
                        QueueEventType.DELETE_SUCCEEDED);
    }

    @Test
    void missingCompletedFileIsAnIdempotentDeleteSuccess() throws Exception {
        Path root = Files.createTempDirectory(Path.of("target"), "maintenance-missing-");
        Path physical = root.resolve("volume").resolve(TENANT).resolve(KEY + ".bin");
        SqliteConfiguration sqlite = SqliteConnectionFactory.defaults();
        Clock clock = Clock.fixed(NOW, ZoneOffset.UTC);
        SqliteMetadataProjectionStore metadata =
                new SqliteMetadataProjectionStore(root.resolve("metadata"), sqlite, clock);
        SqliteQuotaRepository quota = new SqliteQuotaRepository(root.resolve("quota"), sqlite, clock, ignored -> 10);
        TestJournal journal = new TestJournal(
                List.of(event(1, QueueEventType.ACCEPTED, FileProcessingStatus.PENDING, null, physical)));
        quota.reserve(TENANT, KEY, "/");
        QueueProjectionService projection = projection(root, journal, metadata, quota, clock);
        projection.projectTenantUntilCaughtUp(TENANT, 32);
        // A completed row is normally produced by the scheduler; use the reducer directly for this boundary test.
        QueueEventRecord completed = event(
                2, QueueEventType.PROCESSING_COMPLETED, FileProcessingStatus.COMPLETED, UUID.randomUUID(), physical);
        new QueueEventReducer(metadata, quota).apply(completed);
        journal.events.add(completed);

        CompletedFileReaper reaper = new CompletedFileReaper(
                journal, metadata, projection, List.of(new TestVolume("volume-a", root.resolve("volume"))), clock);
        assertThat(reaper.run(java.time.Duration.ZERO, 100).succeededCount()).isEqualTo(1);
        assertThat(metadata.find(TENANT, KEY)).isEmpty();
    }

    @Test
    void durableDeleteRequestIsNotReportedAsFailedWhenProjectionTemporarilyFails() throws Exception {
        Path root = Files.createTempDirectory(Path.of("target"), "maintenance-projection-failure-");
        Path mount = root.resolve("volume");
        Path physical = mount.resolve(TENANT).resolve(KEY + ".bin");
        Files.createDirectories(physical.getParent());
        Files.writeString(physical, "payload");
        Clock clock = Clock.fixed(NOW, ZoneOffset.UTC);
        SqliteMetadataProjectionStore metadata =
                new SqliteMetadataProjectionStore(root.resolve("metadata"), SqliteConnectionFactory.defaults(), clock);
        SqliteQuotaRepository quota = new SqliteQuotaRepository(
                root.resolve("quota"), SqliteConnectionFactory.defaults(), clock, ignored -> 10);
        UUID lease = UUID.randomUUID();
        TestJournal journal = new TestJournal(List.of(
                event(1, QueueEventType.ACCEPTED, FileProcessingStatus.PENDING, null, physical),
                event(2, QueueEventType.PROCESSING_STARTED, FileProcessingStatus.PROCESSING, lease, physical),
                event(3, QueueEventType.PROCESSING_COMPLETED, FileProcessingStatus.COMPLETED, lease, physical)));
        quota.reserve(TENANT, KEY, "/");
        QueueProjectionService projection = projection(root, journal, metadata, quota, clock);
        projection.projectTenantUntilCaughtUp(TENANT, 32);
        CompletedFileReaper reaper = new CompletedFileReaper(
                journal, metadata, projection, List.of(new TestVolume("volume-a", mount)), clock);
        journal.failNextTerminalRead = true;

        assertThat(reaper.run(java.time.Duration.ZERO, 100).failedCount()).isZero();
        assertThat(journal.events)
                .filteredOn(event -> event.eventType() == QueueEventType.DELETE_REQUESTED)
                .hasSize(1);
        assertThat(physical).exists();

        projection.projectTenantUntilCaughtUp(TENANT, 32);
        assertThat(reaper.run(java.time.Duration.ZERO, 100).succeededCount()).isEqualTo(1);
        assertThat(physical).doesNotExist();
    }

    @Test
    void moveToDeadLetterCompletesAndRetriesAfterEventAppendFailure() throws Exception {
        Path root = Files.createTempDirectory(Path.of("target"), "dead-letter-");
        Path mount = root.resolve("volume");
        Path physical = mount.resolve(TENANT).resolve(KEY + ".bin");
        Files.createDirectories(physical.getParent());
        Files.writeString(physical, "failed");
        Clock clock = Clock.fixed(NOW, ZoneOffset.UTC);
        SqliteMetadataProjectionStore metadata =
                new SqliteMetadataProjectionStore(root.resolve("metadata"), SqliteConnectionFactory.defaults(), clock);
        SqliteQuotaRepository quota = new SqliteQuotaRepository(
                root.resolve("quota"), SqliteConnectionFactory.defaults(), clock, ignored -> 10);
        UUID lease = UUID.randomUUID();
        TestJournal journal = new TestJournal(List.of(
                event(1, QueueEventType.ACCEPTED, FileProcessingStatus.PENDING, null, physical),
                event(2, QueueEventType.PROCESSING_STARTED, FileProcessingStatus.PROCESSING, lease, physical),
                event(3, QueueEventType.PROCESSING_FAILED, FileProcessingStatus.PERMANENTLY_FAILED, lease, physical)));
        quota.reserve(TENANT, KEY, "/");
        QueueProjectionService projection = projection(root, journal, metadata, quota, clock);
        projection.projectTenantUntilCaughtUp(TENANT, 32);
        PermanentFailureReaper reaper = new PermanentFailureReaper(
                journal, metadata, projection, List.of(new TestVolume("volume-a", mount)), clock);

        journal.failNextAppend = true;
        assertThat(reaper.run(java.time.Duration.ZERO, 100, PermanentlyFailedDisposition.MOVE_TO_DEAD_LETTER)
                        .failedCount())
                .isEqualTo(1);
        Path deadLetter =
                mount.resolve(".deadletter").resolve(TENANT).resolve("20260919").resolve(KEY + ".bin");
        assertThat(deadLetter).exists();
        assertThat(metadata.find(TENANT, KEY)).isPresent();

        assertThat(reaper.run(java.time.Duration.ZERO, 100, PermanentlyFailedDisposition.MOVE_TO_DEAD_LETTER)
                        .succeededCount())
                .isEqualTo(1);
        assertThat(metadata.find(TENANT, KEY)).isEmpty();
        assertThat(quota.tenantCurrentCount(TENANT)).isZero();
    }

    @Test
    void keepDispositionDoesNotTouchPhysicalFileOrProjection() throws Exception {
        Path root = Files.createTempDirectory(Path.of("target"), "dead-letter-keep-");
        Path mount = root.resolve("volume");
        Path physical = mount.resolve(TENANT).resolve(KEY + ".bin");
        Files.createDirectories(physical.getParent());
        Files.writeString(physical, "failed");
        Clock clock = Clock.fixed(NOW, ZoneOffset.UTC);
        SqliteMetadataProjectionStore metadata =
                new SqliteMetadataProjectionStore(root.resolve("metadata"), SqliteConnectionFactory.defaults(), clock);
        SqliteQuotaRepository quota = new SqliteQuotaRepository(
                root.resolve("quota"), SqliteConnectionFactory.defaults(), clock, ignored -> 10);
        UUID lease = UUID.randomUUID();
        TestJournal journal = new TestJournal(List.of(
                event(1, QueueEventType.ACCEPTED, FileProcessingStatus.PENDING, null, physical),
                event(2, QueueEventType.PROCESSING_STARTED, FileProcessingStatus.PROCESSING, lease, physical),
                event(3, QueueEventType.PROCESSING_FAILED, FileProcessingStatus.PERMANENTLY_FAILED, lease, physical)));
        quota.reserve(TENANT, KEY, "/");
        QueueProjectionService projection = projection(root, journal, metadata, quota, clock);
        projection.projectTenantUntilCaughtUp(TENANT, 32);
        PermanentFailureReaper reaper = new PermanentFailureReaper(
                journal, metadata, projection, List.of(new TestVolume("volume-a", mount)), clock);
        assertThat(reaper.run(java.time.Duration.ZERO, 100, PermanentlyFailedDisposition.KEEP)
                        .skippedCount())
                .isEqualTo(1);
        assertThat(physical).exists();
        assertThat(metadata.find(TENANT, KEY)).isPresent();
    }

    @Test
    void deleteDispositionRemovesPermanentFailureAndReleasesQuota() throws Exception {
        Path root = Files.createTempDirectory(Path.of("target"), "dead-letter-delete-");
        Path mount = root.resolve("volume");
        Path physical = mount.resolve(TENANT).resolve(KEY + ".bin");
        Files.createDirectories(physical.getParent());
        Files.writeString(physical, "failed");
        Clock clock = Clock.fixed(NOW, ZoneOffset.UTC);
        SqliteMetadataProjectionStore metadata =
                new SqliteMetadataProjectionStore(root.resolve("metadata"), SqliteConnectionFactory.defaults(), clock);
        SqliteQuotaRepository quota = new SqliteQuotaRepository(
                root.resolve("quota"), SqliteConnectionFactory.defaults(), clock, ignored -> 10);
        UUID lease = UUID.randomUUID();
        TestJournal journal = new TestJournal(List.of(
                event(1, QueueEventType.ACCEPTED, FileProcessingStatus.PENDING, null, physical),
                event(2, QueueEventType.PROCESSING_STARTED, FileProcessingStatus.PROCESSING, lease, physical),
                event(3, QueueEventType.PROCESSING_FAILED, FileProcessingStatus.PERMANENTLY_FAILED, lease, physical)));
        quota.reserve(TENANT, KEY, "/");
        QueueProjectionService projection = projection(root, journal, metadata, quota, clock);
        projection.projectTenantUntilCaughtUp(TENANT, 32);
        PermanentFailureReaper reaper = new PermanentFailureReaper(
                journal, metadata, projection, List.of(new TestVolume("volume-a", mount)), clock);

        assertThat(reaper.run(java.time.Duration.ZERO, 100, PermanentlyFailedDisposition.DELETE)
                        .succeededCount())
                .isEqualTo(1);
        assertThat(physical).doesNotExist();
        assertThat(metadata.find(TENANT, KEY)).isEmpty();
        assertThat(quota.tenantCurrentCount(TENANT)).isZero();
    }

    @Test
    void rejectsPermanentFailurePathOutsideItsVolume() throws Exception {
        Path root = Files.createTempDirectory(Path.of("target"), "dead-letter-path-");
        Path mount = root.resolve("volume");
        Path outside = root.resolve("outside").resolve(KEY + ".bin");
        Files.createDirectories(outside.getParent());
        Files.writeString(outside, "failed");
        Clock clock = Clock.fixed(NOW, ZoneOffset.UTC);
        SqliteMetadataProjectionStore metadata =
                new SqliteMetadataProjectionStore(root.resolve("metadata"), SqliteConnectionFactory.defaults(), clock);
        SqliteQuotaRepository quota = new SqliteQuotaRepository(
                root.resolve("quota"), SqliteConnectionFactory.defaults(), clock, ignored -> 10);
        UUID lease = UUID.randomUUID();
        TestJournal journal = new TestJournal(List.of(
                event(1, QueueEventType.ACCEPTED, FileProcessingStatus.PENDING, null, outside),
                event(2, QueueEventType.PROCESSING_STARTED, FileProcessingStatus.PROCESSING, lease, outside),
                event(3, QueueEventType.PROCESSING_FAILED, FileProcessingStatus.PERMANENTLY_FAILED, lease, outside)));
        quota.reserve(TENANT, KEY, "/");
        QueueProjectionService projection = projection(root, journal, metadata, quota, clock);
        projection.projectTenantUntilCaughtUp(TENANT, 32);
        PermanentFailureReaper reaper = new PermanentFailureReaper(
                journal, metadata, projection, List.of(new TestVolume("volume-a", mount)), clock);

        assertThat(reaper.run(java.time.Duration.ZERO, 100, PermanentlyFailedDisposition.MOVE_TO_DEAD_LETTER)
                        .failedCount())
                .isEqualTo(1);
        assertThat(outside).exists();
        assertThat(metadata.find(TENANT, KEY)).isPresent();
    }

    @Test
    void junkAndBackupCleanupOnlyRemoveRecognizedArtifacts() throws Exception {
        Path root = Files.createTempDirectory(Path.of("target"), "maintenance-junk-");
        Path mount = root.resolve("volume");
        Path temporary = mount.resolve("tenant-a").resolve("." + KEY + ".bin.12345678-1234-1234-1234-123456789abc.tmp");
        Path unrelated = mount.resolve("tenant-a").resolve(".keep.tmp");
        Files.createDirectories(temporary.getParent());
        Files.writeString(temporary, "temporary");
        Files.writeString(unrelated, "keep");
        Path metadataRoot = root.resolve("metadata").resolve(TENANT);
        Files.createDirectories(metadataRoot);
        Path backup = metadataRoot.resolve("metadata.db.corrupt.123.bak");
        Files.writeString(backup, "backup");
        JunkFileCleaner cleaner = new JunkFileCleaner(
                List.of(new TestVolume("volume-a", mount)),
                List.of(root.resolve("metadata")),
                Clock.fixed(NOW, ZoneOffset.UTC));

        assertThat(cleaner.cleanupJunkFiles(100).succeededCount()).isEqualTo(1);
        assertThat(temporary).doesNotExist();
        assertThat(unrelated).exists();
        assertThat(cleaner.cleanupInvalidDatabaseBackups(100).succeededCount()).isEqualTo(1);
        assertThat(backup).doesNotExist();
    }

    private static QueueProjectionService projection(
            Path root,
            TestJournal journal,
            SqliteMetadataProjectionStore metadata,
            SqliteQuotaRepository quota,
            Clock clock) {
        return new QueueProjectionService(
                journal,
                new QueueEventReducer(metadata, quota),
                new ProjectionCursorStore(root.resolve("cursor"), clock),
                clock,
                null);
    }

    private static QueueEventRecord event(
            long sequence, QueueEventType type, FileProcessingStatus status, UUID lease, Path physical) {
        return new QueueEventRecord(
                1,
                UUID.randomUUID(),
                TENANT,
                KEY,
                type,
                NOW,
                sequence,
                "volume-a",
                physical,
                "/",
                7,
                status,
                lease,
                type == QueueEventType.PROCESSING_STARTED ? NOW : null,
                type == QueueEventType.PROCESSING_FAILED ? 3 : 0,
                null,
                type == QueueEventType.PROCESSING_FAILED ? "permanent failure" : null,
                "file.bin",
                ".bin");
    }

    private static final class TestJournal implements QueueEventJournal {
        private final List<QueueEventRecord> events = new ArrayList<>();
        private boolean failNextAppend;
        private boolean failNextTerminalRead;

        private TestJournal(List<QueueEventRecord> initial) {
            events.addAll(initial);
        }

        @Override
        public long append(QueueEventRecord event) {
            if (failNextAppend) {
                failNextAppend = false;
                throw new IllegalStateException("injected append failure");
            }
            events.add(event);
            return events.size();
        }

        @Override
        public JournalReadBatch readBatch(String tenantId, long offset, int maxRecords) {
            int start = Math.toIntExact(offset);
            if (start >= events.size())
                return new JournalReadBatch(tenantId, offset, events.size(), lastSequence(), List.of());
            List<QueueEventRecord> selected = events.subList(start, Math.min(events.size(), start + maxRecords));
            JournalReadBatch batch = new JournalReadBatch(
                    tenantId,
                    offset,
                    offset + selected.size(),
                    selected.get(selected.size() - 1).sequenceNumber(),
                    selected);
            if (failNextTerminalRead
                    && selected.stream().anyMatch(event -> event.eventType() == QueueEventType.DELETE_REQUESTED)) {
                failNextTerminalRead = false;
                throw new IllegalStateException("injected projection read failure");
            }
            return batch;
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
            return Set.of(TENANT);
        }

        @Override
        public void compact(String tenantId, long throughOffset) {}

        @Override
        public void flush() {}

        @Override
        public void close() {}

        private long lastSequence() {
            return events.isEmpty() ? 0 : events.get(events.size() - 1).sequenceNumber();
        }
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
        public InputStream read(Path path) throws RuntimeException {
            try {
                return Files.newInputStream(path);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

        @Override
        public void delete(Path path) {
            try {
                Files.deleteIfExists(path);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

        @Override
        public void move(Path source, Path target) {
            try {
                Files.createDirectories(target.getParent());
                Files.move(source, target);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

        @Override
        public void close() {}
    }
}
