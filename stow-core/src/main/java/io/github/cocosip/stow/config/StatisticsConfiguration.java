package io.github.cocosip.stow.config;

import java.time.Duration;

public record StatisticsConfiguration(boolean enabled, Duration windowSize, Duration retention, int maxSeries) {

    public StatisticsConfiguration {
        ConfigurationValidation.positive("statistics.windowSize", windowSize);
        ConfigurationValidation.positive("statistics.retention", retention);
        if (retention.compareTo(windowSize) < 0) {
            throw ConfigurationValidation.invalid("statistics.retention", "must not be shorter than windowSize");
        }
        ConfigurationValidation.positive("statistics.maxSeries", maxSeries);
    }
}
