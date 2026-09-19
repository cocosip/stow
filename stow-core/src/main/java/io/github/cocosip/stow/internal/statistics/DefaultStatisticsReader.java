package io.github.cocosip.stow.internal.statistics;

import io.github.cocosip.stow.config.StatisticsConfiguration;
import io.github.cocosip.stow.model.StatisticsQuery;
import io.github.cocosip.stow.model.StatisticsSnapshot;
import java.time.Clock;
import java.util.EnumSet;
import java.util.Objects;

public final class DefaultStatisticsReader implements StatisticsRecorder {

    private final StatisticsRecorder delegate;

    public DefaultStatisticsReader(StatisticsConfiguration configuration, Clock clock) {
        Objects.requireNonNull(configuration, "configuration");
        Objects.requireNonNull(clock, "clock");
        delegate = configuration.enabled()
                ? new WindowedStatisticsRecorder(
                        clock,
                        configuration.windowSize(),
                        configuration.retention(),
                        configuration.maxSeries(),
                        EnumSet.of(StatisticDimension.OPERATION))
                : new NoopStatisticsRecorder(clock);
    }

    public DefaultStatisticsReader(StatisticsRecorder delegate) {
        this.delegate = Objects.requireNonNull(delegate, "delegate");
    }

    public StatisticsRecorder recorder() {
        return delegate;
    }

    @Override
    public StatisticsSnapshot snapshot(StatisticsQuery query) {
        return delegate.snapshot(query);
    }

    @Override
    public boolean recordWrite(String tenantId, String volumeId, long bytes) {
        return delegate.recordWrite(tenantId, volumeId, bytes);
    }

    @Override
    public boolean recordRead(String tenantId, String volumeId) {
        return delegate.recordRead(tenantId, volumeId);
    }

    @Override
    public boolean recordClaim(String tenantId) {
        return delegate.recordClaim(tenantId);
    }

    @Override
    public boolean recordCompleted(String tenantId) {
        return delegate.recordCompleted(tenantId);
    }

    @Override
    public boolean recordSqlitePersistence(String tenantId) {
        return delegate.recordSqlitePersistence(tenantId);
    }

    @Override
    public boolean recordWatcherImport(String watcherId, String tenantId, long bytes) {
        return delegate.recordWatcherImport(watcherId, tenantId, bytes);
    }
}
