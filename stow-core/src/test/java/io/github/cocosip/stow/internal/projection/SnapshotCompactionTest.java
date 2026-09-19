package io.github.cocosip.stow.internal.projection;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.github.cocosip.stow.internal.journal.JournalReadBatch;
import io.github.cocosip.stow.internal.quota.SqliteQuotaRepository;
import io.github.cocosip.stow.internal.sqlite.SqliteConnectionFactory;
import io.github.cocosip.stow.model.FileProcessingStatus;
import io.github.cocosip.stow.model.QueueEventRecord;
import io.github.cocosip.stow.model.QueueEventType;
import io.github.cocosip.stow.spi.QueueEventJournal;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import org.junit.jupiter.api.Test;

class SnapshotCompactionTest {

    @Test
    void snapshotBytesAreDeterministicAndCrcDetectsTampering() throws Exception {
        Path root = Files.createTempDirectory(Path.of("target"), "snapshot-");
        ProjectionSnapshotStore store = new ProjectionSnapshotStore(root);
        ProjectionSnapshotStore.Snapshot snapshot =
                new ProjectionSnapshotStore.Snapshot("tenant-a", 0, 12, 2, List.of());

        store.save(snapshot);
        byte[] first = Files.readAllBytes(store.path("tenant-a"));
        store.save(snapshot);
        assertThat(Files.readAllBytes(store.path("tenant-a"))).isEqualTo(first);

        byte[] corrupted = first.clone();
        corrupted[corrupted.length - 2] ^= 1;
        Files.write(store.path("tenant-a"), corrupted);
        assertThatThrownBy(() -> store.load("tenant-a"))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("CRC");
    }

    @Test
    void compactionRequiresProjectorToReachJournalTail() throws Exception {
        Path root = Files.createTempDirectory(Path.of("target"), "compact-");
        FakeJournal journal = new FakeJournal();
        ProjectionCursorStore cursors = new ProjectionCursorStore(root.resolve("cursor"));
        ProjectionSnapshotStore snapshots = new ProjectionSnapshotStore(root.resolve("snapshots"));
        ProjectionMaintenanceService service = new ProjectionMaintenanceService(journal, null, cursors, snapshots);

        assertThatThrownBy(() -> service.compact("tenant-a"))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("caught up");
        assertThat(journal.compactions).isZero();
    }

    @Test
    void rejectsCursorAndSnapshotOffsetsOutsideCurrentJournalBounds() throws Exception {
        Path root = Files.createTempDirectory(Path.of("target"), "snapshot-bounds-");
        FakeJournal journal = new FakeJournal();
        ProjectionCursorStore cursors = new ProjectionCursorStore(root.resolve("cursor"));
        ProjectionSnapshotStore snapshots = new ProjectionSnapshotStore(root.resolve("snapshots"));
        ProjectionMaintenanceService service = new ProjectionMaintenanceService(journal, null, cursors, snapshots);

        cursors.save(new ProjectionCursorStore.Cursor("tenant-a", 11, 1, UUID.randomUUID(), Instant.EPOCH));
        assertThatThrownBy(() -> service.snapshot("tenant-a"))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("outside journal bounds");

        cursors.save(new ProjectionCursorStore.Cursor("tenant-a", 10, 1, UUID.randomUUID(), Instant.EPOCH));
        snapshots.save(new ProjectionSnapshotStore.Snapshot("tenant-a", 1, 10, 1, List.of()));
        assertThatThrownBy(() -> service.compact("tenant-a"))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("journal base");
        assertThat(journal.compactions).isZero();
    }

    @Test
    void rebuildRestoresSnapshotThenReplaysJournalTail() throws Exception {
        Path root = Files.createTempDirectory(Path.of("target"), "snapshot-rebuild-");
        Clock clock = Clock.fixed(Instant.parse("2026-09-19T00:00:00Z"), ZoneOffset.UTC);
        QueueEventRecord snapshotEvent = accepted(1, "0123456789abcdef0123456789abcdef");
        QueueEventRecord tailEvent = accepted(2, "fedcba9876543210fedcba9876543210");
        FakeJournal journal = new FakeJournal(100, 200, List.of(tailEvent));
        ProjectionCursorStore cursors = new ProjectionCursorStore(root.resolve("cursor"), clock);
        ProjectionSnapshotStore snapshots = new ProjectionSnapshotStore(root.resolve("snapshots"));
        snapshots.save(new ProjectionSnapshotStore.Snapshot("tenant-a", 0, 100, 1, List.of(snapshotEvent)));
        SqliteMetadataProjectionStore metadata =
                new SqliteMetadataProjectionStore(root.resolve("metadata"), SqliteConnectionFactory.defaults(), clock);
        SqliteQuotaRepository quota = new SqliteQuotaRepository(
                root.resolve("quota"), SqliteConnectionFactory.defaults(), clock, ignored -> 10);
        ProjectionMaintenanceService service = new ProjectionMaintenanceService(
                journal, null, cursors, snapshots, new QueueEventReducer(metadata, quota));

        assertThat(service.rebuild("tenant-a").projectedOffset()).isEqualTo(200);
        assertThat(metadata.activeFiles("tenant-a"))
                .extracting(SqliteMetadataProjectionStore.FileRow::fileKey)
                .containsExactly(snapshotEvent.fileKey(), tailEvent.fileKey());
    }

    private static QueueEventRecord accepted(long sequence, String fileKey) {
        return new QueueEventRecord(
                1,
                UUID.randomUUID(),
                "tenant-a",
                fileKey,
                QueueEventType.ACCEPTED,
                Instant.parse("2026-09-19T00:00:00Z"),
                sequence,
                "volume-a",
                Path.of("/tmp/" + fileKey),
                "/",
                12,
                FileProcessingStatus.PENDING,
                null,
                null,
                0,
                null,
                null,
                fileKey + ".bin",
                ".bin");
    }

    private static final class FakeJournal implements QueueEventJournal {
        private final long base;
        private final long tail;
        private final List<QueueEventRecord> events;
        int compactions;

        private FakeJournal() {
            this(0, 10, List.of());
        }

        private FakeJournal(long base, long tail, List<QueueEventRecord> events) {
            this.base = base;
            this.tail = tail;
            this.events = List.copyOf(events);
        }

        @Override
        public long append(QueueEventRecord event) {
            return 0;
        }

        @Override
        public JournalReadBatch readBatch(String tenantId, long offset, int maxRecords) {
            if (offset >= tail || events.isEmpty()) {
                return new JournalReadBatch(tenantId, offset, Math.max(offset, tail), 0, List.of());
            }
            return new JournalReadBatch(
                    tenantId,
                    offset,
                    tail,
                    events.get(events.size() - 1).sequenceNumber(),
                    events.subList(0, Math.min(maxRecords, events.size())));
        }

        @Override
        public long tailOffset(String tenantId) {
            return tail;
        }

        @Override
        public long baseOffset(String tenantId) {
            return base;
        }

        @Override
        public Set<String> tenantIds() {
            return Set.of("tenant-a");
        }

        @Override
        public void compact(String tenantId, long throughOffset) {
            compactions++;
        }

        @Override
        public void flush() {}

        @Override
        public void close() {}
    }
}
