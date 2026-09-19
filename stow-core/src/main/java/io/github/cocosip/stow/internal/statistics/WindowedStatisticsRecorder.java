package io.github.cocosip.stow.internal.statistics;

import io.github.cocosip.stow.model.StatisticsQuery;
import io.github.cocosip.stow.model.StatisticsSnapshot;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.LongAdder;
import java.util.regex.Pattern;

/** Bounded, in-memory, fixed-bucket statistics aggregation. */
public final class WindowedStatisticsRecorder implements StatisticsRecorder {

    private static final Pattern DIMENSION_VALUE = Pattern.compile("[A-Za-z0-9._-]{1,128}");
    private final Clock clock;
    private final long windowMillis;
    private final Duration retention;
    private final int maxSeries;
    private final Set<StatisticDimension> dimensions;
    private final ConcurrentHashMap<Long, Bucket> buckets = new ConcurrentHashMap<>();

    public WindowedStatisticsRecorder(
            Clock clock, Duration windowSize, Duration retention, int maxSeries, Set<StatisticDimension> dimensions) {
        this.clock = Objects.requireNonNull(clock, "clock");
        Objects.requireNonNull(windowSize, "windowSize");
        if (windowSize.isZero() || windowSize.isNegative())
            throw new IllegalArgumentException("windowSize must be positive");
        this.windowMillis = Math.max(1, windowSize.toMillis());
        this.retention = Objects.requireNonNull(retention, "retention");
        if (retention.isNegative() || retention.compareTo(windowSize) < 0) {
            throw new IllegalArgumentException("retention must not be shorter than windowSize");
        }
        if (maxSeries <= 0) throw new IllegalArgumentException("maxSeries must be positive");
        this.maxSeries = maxSeries;
        this.dimensions = Set.copyOf(Objects.requireNonNull(dimensions, "dimensions"));
    }

    public WindowedStatisticsRecorder(Clock clock, Duration windowSize, Duration retention, int maxSeries) {
        this(clock, windowSize, retention, maxSeries, EnumSet.noneOf(StatisticDimension.class));
    }

    @Override
    public boolean recordWrite(String tenantId, String volumeId, long bytes) {
        requireBytes(bytes);
        return record("write", tenantId, volumeId, null, bytes, Metric.WRITE);
    }

    @Override
    public boolean recordRead(String tenantId, String volumeId) {
        return record("read", tenantId, volumeId, null, 0, Metric.READ);
    }

    @Override
    public boolean recordClaim(String tenantId) {
        return record("claim", tenantId, null, null, 0, Metric.CLAIM);
    }

    @Override
    public boolean recordCompleted(String tenantId) {
        return record("completed", tenantId, null, null, 0, Metric.COMPLETED);
    }

    @Override
    public boolean recordSqlitePersistence(String tenantId) {
        return record("sqlite-persistence", tenantId, null, null, 0, Metric.SQLITE);
    }

    @Override
    public boolean recordWatcherImport(String watcherId, String tenantId, long bytes) {
        requireBytes(bytes);
        return record("watcher-import", tenantId, null, watcherId, bytes, Metric.WATCHER);
    }

    @Override
    public StatisticsSnapshot snapshot(StatisticsQuery query) {
        Objects.requireNonNull(query, "query");
        prune(clock.instant());
        List<Bucket> selected = buckets.values().stream()
                .filter(bucket -> bucket.overlaps(query.from(), query.to(), windowMillis))
                .toList();
        Counters aggregate = new Counters();
        Map<String, Long> series = new java.util.LinkedHashMap<>();
        for (Bucket bucket : selected) {
            if (matchesQuery(bucket.totalLabels, query)) aggregate.add(bucket.total);
            for (Map.Entry<String, Series> entry : bucket.series.entrySet()) {
                if (!matchesQuery(entry.getKey(), query)) continue;
                Series value = entry.getValue();
                series.merge(entry.getKey(), value.operations.sum(), Long::sum);
                if (queryHasFilter(query)) aggregate.add(value.counters);
            }
        }
        if (!queryHasFilter(query)) {
            for (Bucket bucket : selected) {
                for (Map.Entry<String, Series> entry : bucket.series.entrySet()) {
                    series.merge(entry.getKey(), entry.getValue().operations.sum(), Long::sum);
                }
            }
        }
        double seconds = Duration.between(query.from(), query.to()).toNanos() / 1_000_000_000d;
        double mibPerSecond = seconds <= 0 ? 0 : aggregate.writtenBytes.sum() / (1024d * 1024d) / seconds;
        return new StatisticsSnapshot(
                query.from(),
                query.to(),
                aggregate.writtenFiles.sum(),
                aggregate.writtenBytes.sum(),
                mibPerSecond,
                aggregate.reads.sum(),
                aggregate.claims.sum(),
                aggregate.completed.sum(),
                aggregate.sqlite.sum(),
                aggregate.watcherImports.sum(),
                aggregate.watcherBytes.sum(),
                Map.copyOf(series));
    }

    private boolean record(
            String operation, String tenantId, String volumeId, String watcherId, long bytes, Metric metric) {
        validateValue(tenantId, "tenantId");
        validateValue(volumeId, "volumeId");
        validateValue(watcherId, "watcherId");
        long now = clock.instant().toEpochMilli();
        prune(Instant.ofEpochMilli(now));
        Bucket bucket = buckets.computeIfAbsent(bucketStart(now), Bucket::new);
        String label = label(operation, tenantId, volumeId, watcherId);
        synchronized (bucket) {
            metric.add(bucket.total, bytes);
            Series series = bucket.series.get(label);
            if (series == null) {
                if (bucket.series.size() >= maxSeries) return false;
                series = new Series();
                bucket.series.put(label, series);
            }
            metric.add(series.counters, bytes);
            series.operations.increment();
            return true;
        }
    }

    private String label(String operation, String tenantId, String volumeId, String watcherId) {
        List<String> parts = new ArrayList<>();
        if (dimensions.contains(StatisticDimension.OPERATION)) parts.add("operation=" + operation);
        if (dimensions.contains(StatisticDimension.TENANT)) parts.add("tenant=" + valueOrNone(tenantId));
        if (dimensions.contains(StatisticDimension.VOLUME)) parts.add("volume=" + valueOrNone(volumeId));
        if (dimensions.contains(StatisticDimension.WATCHER)) parts.add("watcher=" + valueOrNone(watcherId));
        if (parts.isEmpty()) return "all";
        return String.join("|", parts);
    }

    private boolean matchesQuery(String label, StatisticsQuery query) {
        if (!queryHasFilter(query)) return true;
        if (query.tenantId() != null && !label.contains("tenant=" + query.tenantId())) return false;
        if (query.volumeId() != null && !label.contains("volume=" + query.volumeId())) return false;
        if (query.watcherId() != null && !label.contains("watcher=" + query.watcherId())) return false;
        if (query.operation() != null && !label.contains("operation=" + query.operation())) return false;
        return true;
    }

    private static boolean queryHasFilter(StatisticsQuery query) {
        return query.tenantId() != null
                || query.volumeId() != null
                || query.watcherId() != null
                || query.operation() != null;
    }

    private void prune(Instant now) {
        long cutoff = now.minus(retention).toEpochMilli();
        buckets.entrySet().removeIf(entry -> entry.getKey() < cutoff);
    }

    private long bucketStart(long epochMillis) {
        return Math.floorDiv(epochMillis, windowMillis) * windowMillis;
    }

    private static String valueOrNone(String value) {
        return value == null ? "none" : value;
    }

    private static void validateValue(String value, String name) {
        if (value != null && !DIMENSION_VALUE.matcher(value).matches()) {
            throw new IllegalArgumentException(name + " is not a safe statistics dimension");
        }
    }

    private static void requireBytes(long bytes) {
        if (bytes < 0) throw new IllegalArgumentException("bytes must not be negative");
    }

    private enum Metric {
        WRITE,
        READ,
        CLAIM,
        COMPLETED,
        SQLITE,
        WATCHER;

        private void add(Counters counters, long bytes) {
            switch (this) {
                case WRITE -> {
                    counters.writtenFiles.increment();
                    counters.writtenBytes.add(bytes);
                }
                case READ -> counters.reads.increment();
                case CLAIM -> counters.claims.increment();
                case COMPLETED -> counters.completed.increment();
                case SQLITE -> counters.sqlite.increment();
                case WATCHER -> {
                    counters.watcherImports.increment();
                    counters.watcherBytes.add(bytes);
                }
            }
        }
    }

    private static final class Bucket {
        private final long start;
        private final String totalLabels = "all";
        private final Counters total = new Counters();
        private final Map<String, Series> series = new java.util.HashMap<>();

        private Bucket(long start) {
            this.start = start;
        }

        private boolean overlaps(Instant from, Instant to, long windowMillis) {
            return start < to.toEpochMilli() && start + windowMillis > from.toEpochMilli();
        }
    }

    private static final class Series {
        private final Counters counters = new Counters();
        private final LongAdder operations = new LongAdder();
    }

    private static final class Counters {
        private final LongAdder writtenFiles = new LongAdder();
        private final LongAdder writtenBytes = new LongAdder();
        private final LongAdder reads = new LongAdder();
        private final LongAdder claims = new LongAdder();
        private final LongAdder completed = new LongAdder();
        private final LongAdder sqlite = new LongAdder();
        private final LongAdder watcherImports = new LongAdder();
        private final LongAdder watcherBytes = new LongAdder();

        private void add(Counters other) {
            writtenFiles.add(other.writtenFiles.sum());
            writtenBytes.add(other.writtenBytes.sum());
            reads.add(other.reads.sum());
            claims.add(other.claims.sum());
            completed.add(other.completed.sum());
            sqlite.add(other.sqlite.sum());
            watcherImports.add(other.watcherImports.sum());
            watcherBytes.add(other.watcherBytes.sum());
        }
    }
}
