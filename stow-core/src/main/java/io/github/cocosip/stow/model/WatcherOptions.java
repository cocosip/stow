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
        ModelValidation.nonNegative("scanInterval", scanInterval);
        ModelValidation.nonNegative("historyFlushDebounce", historyFlushDebounce);
        ModelValidation.nonNegative("historyRetention", historyRetention);
    }
}
