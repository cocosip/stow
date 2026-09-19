package io.github.cocosip.stow.internal.projection;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.github.cocosip.stow.internal.journal.JournalReadBatch;
import io.github.cocosip.stow.model.QueueEventRecord;
import io.github.cocosip.stow.spi.QueueEventJournal;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Set;
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

    private static final class FakeJournal implements QueueEventJournal {
        int compactions;

        @Override
        public long append(QueueEventRecord event) {
            return 0;
        }

        @Override
        public JournalReadBatch readBatch(String tenantId, long offset, int maxRecords) {
            return new JournalReadBatch(tenantId, offset, offset, 0, List.of());
        }

        @Override
        public long tailOffset(String tenantId) {
            return 10;
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
        public void compact(String tenantId, long throughOffset) {
            compactions++;
        }

        @Override
        public void flush() {}

        @Override
        public void close() {}
    }
}
