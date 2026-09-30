package io.github.cocosip.stow.model;

import java.time.Duration;

public record WatcherOptions(
        boolean enabled,
        int maxParallelScans,
        Duration scanInterval,
        Duration historyFlushDebounce,
        Duration historyRetention) {

    public WatcherOptions {
        ModelValidation.positive("maxParallelScans", maxParallelScans);
        ModelValidation.nonNegative("historyFlushDebounce", historyFlushDebounce);
        ModelValidation.nonNegative("historyRetention", historyRetention);
        // Locus clamps the polling interval into [MinimumPollingInterval, MaximumPollingInterval].
        scanInterval = clamp(scanInterval, MIN_SCAN_INTERVAL, MAX_SCAN_INTERVAL);
    }

    /** Polling bounds mirroring Locus FileWatcherOptions. */
    private static Duration clamp(Duration value, Duration minimum, Duration maximum) {
        if (value.compareTo(minimum) < 0) return minimum;
        return value.compareTo(maximum) > 0 ? maximum : value;
    }

    private static final Duration MIN_SCAN_INTERVAL = Duration.ofSeconds(5);

    private static final Duration MAX_SCAN_INTERVAL = Duration.ofHours(1);
}
