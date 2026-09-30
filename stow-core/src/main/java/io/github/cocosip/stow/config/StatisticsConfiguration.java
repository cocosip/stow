package io.github.cocosip.stow.config;

import java.time.Duration;

/**
 * In-process statistics aggregation. Bounds mirror Locus: maxSeries stays within
 * [1024, 262144]; optional periodic output writes snapshots through the logging sink.
 */
public record StatisticsConfiguration(
        boolean enabled,
        Duration windowSize,
        Duration retention,
        int maxSeries,
        boolean outputEnabled,
        Duration outputInterval,
        Duration outputQueryWindow) {

    private static final int MIN_MAX_SERIES = 1_024;

    private static final int MAX_MAX_SERIES = 262_144;

    public StatisticsConfiguration {
        ConfigurationValidation.positive("statistics.windowSize", windowSize);
        ConfigurationValidation.positive("statistics.retention", retention);
        if (retention.compareTo(windowSize) < 0) {
            throw ConfigurationValidation.invalid("statistics.retention", "must not be shorter than windowSize");
        }
        ConfigurationValidation.positive("statistics.maxSeries", maxSeries);
        if (maxSeries < MIN_MAX_SERIES || maxSeries > MAX_MAX_SERIES) {
            throw ConfigurationValidation.invalid(
                    "statistics.maxSeries", "must be between " + MIN_MAX_SERIES + " and " + MAX_MAX_SERIES);
        }
        if (outputEnabled) {
            ConfigurationValidation.positive("statistics.outputInterval", outputInterval);
            ConfigurationValidation.positive("statistics.outputQueryWindow", outputQueryWindow);
        }
    }

    public StatisticsConfiguration(boolean enabled, Duration windowSize, Duration retention, int maxSeries) {
        this(enabled, windowSize, retention, maxSeries, false, Duration.ofMinutes(1), Duration.ofMinutes(15));
    }
}
