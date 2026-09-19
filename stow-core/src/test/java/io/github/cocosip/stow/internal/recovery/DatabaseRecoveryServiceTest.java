package io.github.cocosip.stow.internal.recovery;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.cocosip.stow.config.JournalAckMode;
import io.github.cocosip.stow.config.JournalConfiguration;
import io.github.cocosip.stow.config.JournalFormat;
import io.github.cocosip.stow.internal.journal.BinaryV1JournalCodec;
import io.github.cocosip.stow.internal.journal.FileQueueEventJournal;
import io.github.cocosip.stow.internal.projection.QueueEventReducer;
import io.github.cocosip.stow.internal.projection.SqliteMetadataProjectionStore;
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
import org.junit.jupiter.api.Test;

class DatabaseRecoveryServiceTest {

    @Test
    void rebuildsMetadataFromJournalAfterMetadataCorruption() throws Exception {
        Path root = Files.createTempDirectory(Path.of("target"), "recovery-");
        Clock clock = Clock.fixed(Instant.parse("2026-09-18T00:00:00Z"), ZoneOffset.UTC);
        Path metadataRoot = root.resolve("metadata");
        Path quotaRoot = root.resolve("quota");
        String key = "0123456789abcdef0123456789abcdef";
        try (FileQueueEventJournal journal =
                new FileQueueEventJournal(root.resolve("journal"), journalConfig(), new BinaryV1JournalCodec())) {
            SqliteQuotaRepository quota =
                    new SqliteQuotaRepository(quotaRoot, SqliteConnectionFactory.defaults(), clock, ignored -> 10);
            quota.reserve("tenant-a", key, "/");
            journal.append(event(key));
            new SqliteMetadataProjectionStore(metadataRoot, SqliteConnectionFactory.defaults(), clock)
                    .activeFiles("tenant-a");
            Files.write(metadataRoot.resolve("tenant-a").resolve("metadata.db"), new byte[] {1, 2, 3});

            DatabaseRecoveryService recovery = new DatabaseRecoveryService(
                    metadataRoot, quotaRoot, journal, SqliteConnectionFactory.defaults(), clock, ignored -> 10);
            recovery.rebuildMetadata("tenant-a");
            SqliteMetadataProjectionStore metadata =
                    new SqliteMetadataProjectionStore(metadataRoot, SqliteConnectionFactory.defaults(), clock);
            assertThat(metadata.activeFiles("tenant-a"))
                    .extracting(SqliteMetadataProjectionStore.FileRow::fileKey)
                    .containsExactly(key);
        }
    }

    @Test
    void rebuildsQuotaCountsFromActiveMetadataWithoutJournalQuotaSideEffects() throws Exception {
        Path root = Files.createTempDirectory(Path.of("target"), "quota-recovery-");
        Clock clock = Clock.fixed(Instant.parse("2026-09-18T00:00:00Z"), ZoneOffset.UTC);
        SqliteQuotaRepository quota = new SqliteQuotaRepository(
                root.resolve("quota"), SqliteConnectionFactory.defaults(), clock, ignored -> 10);
        SqliteMetadataProjectionStore metadata =
                new SqliteMetadataProjectionStore(root.resolve("metadata"), SqliteConnectionFactory.defaults(), clock);
        QueueEventReducer reducer = new QueueEventReducer(metadata, quota);
        String key = "0123456789abcdef0123456789abcdef";
        quota.reserve("tenant-a", key, "/");
        reducer.apply(event(key));
        quota.setTenantLimit("tenant-a", 10);
        DatabaseRecoveryService.rebuildQuotaFromMetadata(quota, metadata, "tenant-a");
        assertThat(quota.tenantCurrentCount("tenant-a")).isEqualTo(1);
    }

    private static QueueEventRecord event(String key) {
        return new QueueEventRecord(
                1,
                UUID.randomUUID(),
                "tenant-a",
                key,
                QueueEventType.ACCEPTED,
                Instant.parse("2026-09-18T00:00:00Z"),
                1,
                "volume-a",
                Path.of("/tmp/file"),
                "/",
                12,
                FileProcessingStatus.PENDING,
                null,
                null,
                0,
                null,
                null,
                "file.bin",
                ".bin");
    }

    private static JournalConfiguration journalConfig() {
        return new JournalConfiguration(
                true,
                true,
                JournalFormat.BINARY_V1,
                JournalAckMode.DURABLE,
                Duration.ZERO,
                Duration.ZERO,
                16,
                262144,
                Duration.ofSeconds(30),
                16,
                Duration.ZERO);
    }
}
