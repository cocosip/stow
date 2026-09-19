package io.github.cocosip.stow.internal.statistics;

import io.github.cocosip.stow.model.StatisticsQuery;
import io.github.cocosip.stow.model.StatisticsSnapshot;
import java.time.Clock;
import java.time.Instant;
import java.util.Map;
import java.util.Objects;

public final class NoopStatisticsRecorder implements StatisticsRecorder {

    private final Clock clock;

    public NoopStatisticsRecorder(Clock clock) {
        this.clock = Objects.requireNonNull(clock, "clock");
    }

    @Override
    public boolean recordWrite(String tenantId, String volumeId, long bytes) {
        requireBytes(bytes);
        return true;
    }

    @Override
    public boolean recordRead(String tenantId, String volumeId) {
        return true;
    }

    @Override
    public boolean recordClaim(String tenantId) {
        return true;
    }

    @Override
    public boolean recordCompleted(String tenantId) {
        return true;
    }

    @Override
    public boolean recordSqlitePersistence(String tenantId) {
        return true;
    }

    @Override
    public boolean recordWatcherImport(String watcherId, String tenantId, long bytes) {
        requireBytes(bytes);
        return true;
    }

    @Override
    public StatisticsSnapshot snapshot(StatisticsQuery query) {
        Objects.requireNonNull(query, "query");
        Instant from = query.from();
        Instant to = query.to();
        if (to.isBefore(from)) throw new IllegalArgumentException("statistics query range is invalid");
        return new StatisticsSnapshot(from, to, 0, 0, 0, 0, 0, 0, 0, 0, 0, Map.of());
    }

    private static void requireBytes(long bytes) {
        if (bytes < 0) throw new IllegalArgumentException("bytes must not be negative");
    }
}
