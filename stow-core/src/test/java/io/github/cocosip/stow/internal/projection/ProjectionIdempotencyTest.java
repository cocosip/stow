package io.github.cocosip.stow.internal.projection;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.github.cocosip.stow.exception.ProjectionException;
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

class ProjectionIdempotencyTest {

    Path temp;

    @BeforeEach
    void setUp() throws Exception {
        temp = Files.createTempDirectory(Path.of("target"), "projection-idempotency-");
    }

    @Test
    void duplicateEventIsSuccessfulAndDoesNotDuplicateRows() {
        Clock clock = Clock.fixed(Instant.parse("2026-09-18T00:00:00Z"), ZoneOffset.UTC);
        SqliteQuotaRepository quota =
                new SqliteQuotaRepository(temp, SqliteConnectionFactory.defaults(), clock, ignored -> 10);
        SqliteMetadataProjectionStore metadata =
                new SqliteMetadataProjectionStore(temp, SqliteConnectionFactory.defaults(), clock);
        QueueEventReducer reducer = new QueueEventReducer(metadata, quota);
        quota.reserve("tenant-a", "0123456789abcdef0123456789abcdef", "/");
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
        reducer.apply(event);
        reducer.apply(event);
        assertThat(metadata.activeFiles("tenant-a")).hasSize(1);
        assertThat(metadata.appliedEventCount("tenant-a")).isEqualTo(1);
        assertThat(quota.tenantCurrentCount("tenant-a")).isEqualTo(1);
    }

    @Test
    void rejectsSequenceCollisionWithoutChangingProjection() {
        Clock clock = Clock.fixed(Instant.parse("2026-09-18T00:00:00Z"), ZoneOffset.UTC);
        SqliteQuotaRepository quota =
                new SqliteQuotaRepository(temp, SqliteConnectionFactory.defaults(), clock, ignored -> 10);
        SqliteMetadataProjectionStore metadata =
                new SqliteMetadataProjectionStore(temp, SqliteConnectionFactory.defaults(), clock);
        QueueEventReducer reducer = new QueueEventReducer(metadata, quota);
        quota.reserve("tenant-a", "0123456789abcdef0123456789abcdef", "/");
        QueueEventRecord first = event(clock, UUID.randomUUID(), 1);
        reducer.apply(first);
        QueueEventRecord collision = event(clock, UUID.randomUUID(), 1);
        assertThatThrownBy(() -> reducer.apply(collision)).isInstanceOf(ProjectionException.class);
        assertThat(metadata.appliedEventCount("tenant-a")).isEqualTo(1);
    }

    private static QueueEventRecord event(Clock clock, UUID id, long sequence) {
        return new QueueEventRecord(
                1,
                id,
                "tenant-a",
                "0123456789abcdef0123456789abcdef",
                QueueEventType.ACCEPTED,
                clock.instant(),
                sequence,
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
    }
}
