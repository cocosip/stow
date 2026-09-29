package io.github.cocosip.stow.internal.statistics;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.cocosip.stow.config.StatisticsConfiguration;
import io.github.cocosip.stow.model.StatisticsQuery;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import org.junit.jupiter.api.Test;

class DefaultStatisticsReaderTest {

    private static StatisticsQuery query(String tenantId, String volumeId, String watcherId) {
        Instant now = Instant.now();
        return new StatisticsQuery(now.minusSeconds(60), now.plusSeconds(60), tenantId, volumeId, watcherId, null);
    }

    @Test
    void retainsVolumeAndWatcherDimensionsForFilteredQueries() {
        DefaultStatisticsReader reader = new DefaultStatisticsReader(
                new StatisticsConfiguration(true, Duration.ofMinutes(5), Duration.ofHours(1), 1_024),
                Clock.systemUTC());
        reader.recordWrite("tenant-a", "volume-a", 100);
        reader.recordRead("tenant-a", "volume-a");
        reader.recordWatcherImport("watcher-a", "tenant-a", 50);

        var byVolume = reader.snapshot(query(null, "volume-a", null));
        assertThat(byVolume.writtenFileCount()).isEqualTo(1);
        assertThat(byVolume.readCount()).isEqualTo(1);
        assertThat(byVolume.watcherImportedCount()).isZero();

        var byWatcher = reader.snapshot(query(null, null, "watcher-a"));
        assertThat(byWatcher.watcherImportedCount()).isEqualTo(1);
        assertThat(byWatcher.watcherImportedBytes()).isEqualTo(50);
        assertThat(byWatcher.writtenFileCount()).isZero();

        var unfiltered = reader.snapshot(query(null, null, null));
        assertThat(unfiltered.writtenFileCount()).isEqualTo(1);
        assertThat(unfiltered.watcherImportedCount()).isEqualTo(1);
    }

    @Test
    void tenantDimensionIsNotRetainedByDefault() {
        DefaultStatisticsReader reader = new DefaultStatisticsReader(
                new StatisticsConfiguration(true, Duration.ofMinutes(5), Duration.ofHours(1), 1_024),
                Clock.systemUTC());
        reader.recordWrite("tenant-a", "volume-a", 100);

        assertThat(reader.snapshot(query("tenant-a", null, null)).writtenFileCount())
                .isZero();
    }
}
