package io.github.cocosip.stow.internal.projection;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.github.cocosip.stow.exception.ProjectionException;
import io.github.cocosip.stow.internal.quota.QuotaReservation;
import io.github.cocosip.stow.internal.quota.SqliteQuotaRepository;
import io.github.cocosip.stow.internal.sqlite.SqliteConnectionFactory;
import io.github.cocosip.stow.model.FileProcessingStatus;
import io.github.cocosip.stow.model.QueueEventRecord;
import io.github.cocosip.stow.model.QueueEventType;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.UUID;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class QueueEventReducerTest {

    private static final Instant NOW = Instant.parse("2026-09-18T00:00:00Z");
    private static final Clock CLOCK = Clock.fixed(NOW, ZoneOffset.UTC);
    private static final String TENANT = "tenant-a";
    private static final String KEY = "0123456789abcdef0123456789abcdef";
    private static final String DIR = "/incoming";

    Path temp;

    @BeforeEach
    void setUp() throws Exception {
        temp = Files.createTempDirectory(Path.of("target"), "projection-reducer-");
    }

    @Test
    void appliesAcceptedAndProcessingLifecycle() {
        SqliteQuotaRepository quota =
                new SqliteQuotaRepository(temp, SqliteConnectionFactory.defaults(), CLOCK, ignored -> 10);
        SqliteMetadataProjectionStore metadata =
                new SqliteMetadataProjectionStore(temp, SqliteConnectionFactory.defaults(), CLOCK);
        QueueEventReducer reducer = new QueueEventReducer(metadata, quota);
        QuotaReservation reservation = quota.reserve(TENANT, KEY, DIR);

        reducer.apply(event(1, QueueEventType.ACCEPTED, FileProcessingStatus.PENDING, null, null, 0));
        reducer.apply(event(
                2, QueueEventType.PROCESSING_STARTED, FileProcessingStatus.PROCESSING, UUID.randomUUID(), NOW, 0));
        assertThat(metadata.find(TENANT, KEY))
                .get()
                .extracting(SqliteMetadataProjectionStore.FileRow::status)
                .isEqualTo(FileProcessingStatus.PROCESSING);
        assertThat(quota.reservation(TENANT, KEY)).isEmpty();
        assertThat(reservation).isNotNull();
    }

    @Test
    void rejectsMalformedAcceptedStatusBeforeQuotaConsumption() {
        SqliteQuotaRepository quota =
                new SqliteQuotaRepository(temp, SqliteConnectionFactory.defaults(), CLOCK, ignored -> 10);
        SqliteMetadataProjectionStore metadata =
                new SqliteMetadataProjectionStore(temp, SqliteConnectionFactory.defaults(), CLOCK);
        QueueEventReducer reducer = new QueueEventReducer(metadata, quota);
        quota.reserve(TENANT, KEY, DIR);

        assertThatThrownBy(() -> reducer.apply(
                        event(1, QueueEventType.ACCEPTED, FileProcessingStatus.PROCESSING, null, null, 0)))
                .isInstanceOf(ProjectionException.class);
        assertThat(metadata.find(TENANT, KEY)).isEmpty();
        assertThat(quota.reservation(TENANT, KEY)).isPresent();
    }

    @Test
    void rejectsFreshCompletionAfterFileWasAlreadyCompleted() {
        SqliteQuotaRepository quota =
                new SqliteQuotaRepository(temp, SqliteConnectionFactory.defaults(), CLOCK, ignored -> 10);
        SqliteMetadataProjectionStore metadata =
                new SqliteMetadataProjectionStore(temp, SqliteConnectionFactory.defaults(), CLOCK);
        QueueEventReducer reducer = new QueueEventReducer(metadata, quota);
        quota.reserve(TENANT, KEY, DIR);
        UUID lease = UUID.randomUUID();
        reducer.apply(event(1, QueueEventType.ACCEPTED, FileProcessingStatus.PENDING, null, null, 0));
        reducer.apply(event(2, QueueEventType.PROCESSING_STARTED, FileProcessingStatus.PROCESSING, lease, NOW, 0));
        reducer.apply(event(3, QueueEventType.PROCESSING_COMPLETED, FileProcessingStatus.COMPLETED, lease, null, 0));

        assertThatThrownBy(() -> reducer.apply(event(
                        4,
                        QueueEventType.PROCESSING_COMPLETED,
                        FileProcessingStatus.COMPLETED,
                        UUID.randomUUID(),
                        null,
                        0)))
                .isInstanceOf(ProjectionException.class);
    }

    private QueueEventRecord event(
            long sequence, QueueEventType type, FileProcessingStatus status, UUID lease, Instant started, int retries) {
        return new QueueEventRecord(
                1,
                UUID.randomUUID(),
                TENANT,
                KEY,
                type,
                NOW,
                sequence,
                "volume-a",
                Path.of("/tmp/file"),
                DIR,
                12,
                status,
                lease,
                started,
                retries,
                null,
                null,
                "file.bin",
                ".bin");
    }
}
