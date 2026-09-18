package io.github.cocosip.stow.internal.projection;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.cocosip.stow.config.JournalAckMode;
import io.github.cocosip.stow.config.JournalConfiguration;
import io.github.cocosip.stow.config.JournalFormat;
import io.github.cocosip.stow.internal.journal.BinaryV1JournalCodec;
import io.github.cocosip.stow.internal.journal.FileQueueEventJournal;
import io.github.cocosip.stow.internal.quota.SqliteQuotaRepository;
import io.github.cocosip.stow.internal.sqlite.SqliteConnectionFactory;
import io.github.cocosip.stow.model.FileProcessingStatus;
import io.github.cocosip.stow.model.QueueEventRecord;
import io.github.cocosip.stow.model.QueueEventType;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.UUID;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class QueueProjectionServiceTest {

    Path temp;

    @BeforeEach
    void setUp() throws Exception {
        temp = Files.createTempDirectory(Path.of("target"), "projection-service-");
    }

    @Test
    void advancesCursorOnlyAfterBatchProjection() {
        Clock clock = Clock.fixed(Instant.parse("2026-09-18T00:00:00Z"), ZoneOffset.UTC);
        SqliteQuotaRepository quota = new SqliteQuotaRepository(
                temp.resolve("quota"), SqliteConnectionFactory.defaults(), clock, ignored -> 10);
        SqliteMetadataProjectionStore metadata =
                new SqliteMetadataProjectionStore(temp.resolve("metadata"), SqliteConnectionFactory.defaults(), clock);
        quota.reserve("tenant-a", "0123456789abcdef0123456789abcdef", "/");
        try (FileQueueEventJournal journal =
                new FileQueueEventJournal(temp.resolve("journal"), configuration(), new BinaryV1JournalCodec())) {
            QueueEventRecord event = new QueueEventRecord(
                    1,
                    UUID.randomUUID(),
                    "tenant-a",
                    "0123456789abcdef0123456789abcdef",
                    QueueEventType.ACCEPTED,
                    clock.instant(),
                    1,
                    "v",
                    Path.of("/tmp/f"),
                    "/",
                    1,
                    FileProcessingStatus.PENDING,
                    null,
                    null,
                    0,
                    null,
                    null,
                    null,
                    null);
            journal.append(event);
            ProjectionCursorStore cursors = new ProjectionCursorStore(temp.resolve("cursor"), clock);
            QueueProjectionService service =
                    new QueueProjectionService(journal, new QueueEventReducer(metadata, quota), cursors);
            service.projectTenant("tenant-a", 16);
            assertThat(cursors.load("tenant-a").lastSequenceNumber()).isEqualTo(1);
            assertThat(metadata.activeFiles("tenant-a")).hasSize(1);
        }
    }

    @Test
    void cursorIsPersistedAndLoadedOnFirstAccess() {
        Clock clock = Clock.fixed(Instant.parse("2026-09-18T00:00:00Z"), ZoneOffset.UTC);
        ProjectionCursorStore first = new ProjectionCursorStore(temp.resolve("cursor"), clock);
        ProjectionCursorStore.Cursor expected =
                new ProjectionCursorStore.Cursor("tenant-a", 42, 7, UUID.randomUUID(), clock.instant());
        first.save(expected);
        ProjectionCursorStore reopened = new ProjectionCursorStore(temp.resolve("cursor"), clock);
        assertThat(reopened.load("tenant-a")).isEqualTo(expected);
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
}
