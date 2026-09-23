package io.github.cocosip.stow.internal.statistics;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.github.cocosip.stow.model.StatisticsQuery;
import io.github.cocosip.stow.model.StatisticsSnapshot;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.EnumSet;
import org.junit.jupiter.api.Test;

class WindowedStatisticsRecorderTest {

    @Test
    void aggregatesFixedBucketsAndRetainsOnlyConfiguredWindow() {
        MutableClock clock = new MutableClock(Instant.parse("2026-09-19T00:04:59Z"));
        WindowedStatisticsRecorder recorder = new WindowedStatisticsRecorder(
                clock,
                Duration.ofMinutes(5),
                Duration.ofMinutes(10),
                32,
                EnumSet.of(StatisticDimension.TENANT, StatisticDimension.OPERATION));

        assertThat(recorder.recordWrite("tenant-a", "volume-a", 1024)).isTrue();
        clock.advance(Duration.ofSeconds(1));
        recorder.recordWrite("tenant-a", "volume-a", 2048);
        StatisticsSnapshot first = recorder.snapshot(new StatisticsQuery(
                Instant.parse("2026-09-19T00:00:00Z"), Instant.parse("2026-09-19T00:10:00Z"), null, null, null, null));
        assertThat(first.writtenFileCount()).isEqualTo(2);
        assertThat(first.writtenBytes()).isEqualTo(3072);
        assertThat(first.series()).isNotEmpty();

        clock.advance(Duration.ofMinutes(11));
        StatisticsSnapshot retained = recorder.snapshot(
                new StatisticsQuery(Instant.parse("2026-09-19T00:00:00Z"), clock.instant(), null, null, null, null));
        assertThat(retained.writtenFileCount()).isZero();
    }

    @Test
    void rejectsNewSeriesAfterBoundAndSupportsDimensionSwitches() {
        Clock clock = Clock.fixed(Instant.parse("2026-09-19T00:00:00Z"), ZoneOffset.UTC);
        WindowedStatisticsRecorder recorder = new WindowedStatisticsRecorder(
                clock, Duration.ofMinutes(1), Duration.ofMinutes(5), 1, EnumSet.of(StatisticDimension.TENANT));
        assertThat(recorder.recordRead("tenant-a", "volume-a")).isTrue();
        assertThat(recorder.recordRead("tenant-b", "volume-a")).isFalse();
        assertThat(recorder.snapshot(new StatisticsQuery(
                                Instant.EPOCH, clock.instant().plusSeconds(60), null, null, null, null))
                        .series())
                .hasSize(1);

        WindowedStatisticsRecorder noDimensions = new WindowedStatisticsRecorder(
                clock, Duration.ofMinutes(1), Duration.ofMinutes(5), 1, EnumSet.noneOf(StatisticDimension.class));
        assertThat(noDimensions.recordWatcherImport("watcher-a", "tenant-a", 10))
                .isTrue();
        assertThat(noDimensions.recordWatcherImport("watcher-b", "tenant-b", 20))
                .isTrue();
        assertThat(noDimensions
                        .snapshot(new StatisticsQuery(
                                Instant.EPOCH, clock.instant().plusSeconds(60), null, null, null, null))
                        .series())
                .hasSize(1);
    }

    @Test
    void noopRecorderReturnsEmptyImmutableSnapshot() {
        NoopStatisticsRecorder recorder = new NoopStatisticsRecorder(Clock.systemUTC());
        recorder.recordWrite("tenant-a", "volume-a", 10);
        StatisticsSnapshot snapshot =
                recorder.snapshot(new StatisticsQuery(Instant.EPOCH, Instant.now(), null, null, null, null));
        assertThat(snapshot.writtenFileCount()).isZero();
        assertThat(snapshot.series()).isEmpty();
    }

    @Test
    void refusesPathLikeDimensionValues() {
        WindowedStatisticsRecorder recorder =
                new WindowedStatisticsRecorder(Clock.systemUTC(), Duration.ofMinutes(1), Duration.ofMinutes(5), 4);
        assertThatThrownBy(() -> recorder.recordWrite("tenant/a", "volume-a", 1))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void unfilteredSnapshotCountsEachSeriesOnce() {
        Clock clock = Clock.fixed(Instant.parse("2026-09-19T00:00:00Z"), ZoneOffset.UTC);
        WindowedStatisticsRecorder recorder = new WindowedStatisticsRecorder(
                clock, Duration.ofMinutes(1), Duration.ofMinutes(5), 32, EnumSet.of(StatisticDimension.OPERATION));
        recorder.recordWrite("tenant-a", "volume-a", 100);
        recorder.recordWrite("tenant-a", "volume-a", 100);
        recorder.recordRead("tenant-a", "volume-a");
        StatisticsSnapshot snapshot = recorder.snapshot(
                new StatisticsQuery(Instant.EPOCH, clock.instant().plusSeconds(60), null, null, null, null));
        assertThat(snapshot.series().get("operation=write")).isEqualTo(2);
        assertThat(snapshot.series().get("operation=read")).isEqualTo(1);
        assertThat(snapshot.writtenFileCount()).isEqualTo(2);
        assertThat(snapshot.readCount()).isEqualTo(1);
    }

    @Test
    void filteredQueryMatchesWholeDimensionValuesOnly() {
        Clock clock = Clock.fixed(Instant.parse("2026-09-19T00:00:00Z"), ZoneOffset.UTC);
        WindowedStatisticsRecorder recorder = new WindowedStatisticsRecorder(
                clock,
                Duration.ofMinutes(1),
                Duration.ofMinutes(5),
                32,
                EnumSet.of(StatisticDimension.OPERATION, StatisticDimension.TENANT));
        recorder.recordWrite("tenant-a", "volume-a", 10);
        recorder.recordWrite("tenant-a10", "volume-a", 10);
        StatisticsSnapshot snapshot = recorder.snapshot(
                new StatisticsQuery(Instant.EPOCH, clock.instant().plusSeconds(60), "tenant-a", null, null, "write"));
        assertThat(snapshot.series()).hasSize(1);
        assertThat(snapshot.series().get("operation=write|tenant=tenant-a")).isEqualTo(1);
        assertThat(snapshot.writtenFileCount()).isEqualTo(1);
    }

    @Test
    void rejectedSeriesRecordKeepsTotalsConsistentWithSeries() {
        Clock clock = Clock.fixed(Instant.parse("2026-09-19T00:00:00Z"), ZoneOffset.UTC);
        WindowedStatisticsRecorder recorder = new WindowedStatisticsRecorder(
                clock, Duration.ofMinutes(1), Duration.ofMinutes(5), 1, EnumSet.of(StatisticDimension.TENANT));
        assertThat(recorder.recordRead("tenant-a", "volume-a")).isTrue();
        assertThat(recorder.recordRead("tenant-b", "volume-a")).isFalse();
        StatisticsSnapshot snapshot = recorder.snapshot(
                new StatisticsQuery(Instant.EPOCH, clock.instant().plusSeconds(60), null, null, null, null));
        assertThat(snapshot.readCount()).isEqualTo(1);
        assertThat(snapshot.series().get("tenant=tenant-a")).isEqualTo(1);
    }

    private static final class MutableClock extends Clock {
        private Instant instant;

        private MutableClock(Instant instant) {
            this.instant = instant;
        }

        private void advance(Duration duration) {
            instant = instant.plus(duration);
        }

        @Override
        public ZoneOffset getZone() {
            return ZoneOffset.UTC;
        }

        @Override
        public Clock withZone(java.time.ZoneId zone) {
            return this;
        }

        @Override
        public Instant instant() {
            return instant;
        }
    }
}
