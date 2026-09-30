package io.github.cocosip.stow.internal.cleanup;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.cocosip.stow.internal.projection.QueueEventReducer;
import io.github.cocosip.stow.internal.projection.SqliteMetadataProjectionStore;
import io.github.cocosip.stow.internal.quota.SqliteQuotaRepository;
import io.github.cocosip.stow.internal.sqlite.SqliteConnectionFactory;
import io.github.cocosip.stow.model.CleanupStatistics;
import io.github.cocosip.stow.model.FileProcessingStatus;
import io.github.cocosip.stow.model.QueueEventRecord;
import io.github.cocosip.stow.model.QueueEventType;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.Set;
import java.util.UUID;
import org.junit.jupiter.api.Test;

class RetiredVolumeCleanerTest {

    private static final Clock CLOCK = Clock.fixed(Instant.parse("2026-09-18T00:00:00Z"), ZoneOffset.UTC);
    private static final String TENANT = "tenant-a";
    private static final String KEY = "0123456789abcdef0123456789abcdef";

    @Test
    void purgeMetadataOnlyRemovesRowsOnRetiredVolumesAndReleasesQuota() throws Exception {
        Path root = Files.createTempDirectory(Path.of("target"), "retired-volume-");
        SqliteQuotaRepository quota = new SqliteQuotaRepository(
                root.resolve("quota"), SqliteConnectionFactory.defaults(), CLOCK, ignored -> 10);
        SqliteMetadataProjectionStore metadata =
                new SqliteMetadataProjectionStore(root.resolve("metadata"), SqliteConnectionFactory.defaults(), CLOCK);
        QueueEventReducer reducer = new QueueEventReducer(metadata, quota);
        quota.reserve(TENANT, KEY, "/");
        reducer.apply(new QueueEventRecord(
                1,
                UUID.randomUUID(),
                TENANT,
                KEY,
                QueueEventType.ACCEPTED,
                CLOCK.instant(),
                1,
                "retired-volume",
                Path.of("/mnt/retired/" + KEY),
                "/",
                12,
                FileProcessingStatus.PENDING,
                null,
                null,
                0,
                null,
                null,
                "file.bin",
                ".bin"));

        RetiredVolumeCleaner cleaner = new RetiredVolumeCleaner(metadata, quota, CLOCK);
        CleanupStatistics keep = cleaner.clean(Set.of("retired-volume"), false, 100);
        assertThat(keep.succeededCount()).isZero();
        assertThat(metadata.find(TENANT, KEY)).isPresent();

        CleanupStatistics purge = cleaner.clean(Set.of("retired-volume"), true, 100);
        assertThat(purge.succeededCount()).isEqualTo(1);
        assertThat(metadata.find(TENANT, KEY)).isEmpty();
        assertThat(quota.tenantCurrentCount(TENANT)).isZero();

        // A second purge is idempotent: the row is already gone.
        CleanupStatistics again = cleaner.clean(Set.of("retired-volume"), true, 100);
        assertThat(again.succeededCount()).isZero();
    }
}
