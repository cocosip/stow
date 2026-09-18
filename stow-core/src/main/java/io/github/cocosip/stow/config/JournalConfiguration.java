package io.github.cocosip.stow.config;

import java.time.Duration;

public record JournalConfiguration(
        boolean enabled,
        boolean projectionEnabled,
        JournalFormat format,
        JournalAckMode ackMode,
        Duration stateFlushDebounce,
        Duration linger,
        int maxBatchRecords,
        int maxBatchBytes,
        Duration writerIdleTimeout,
        int asyncQueueCapacityPerTenant,
        Duration balancedFlushWindow) {

    public JournalConfiguration {
        if (!enabled) {
            throw ConfigurationValidation.invalid("journal.enabled", "cannot be disabled");
        }
        ConfigurationValidation.nonNull("journal.format", format);
        ConfigurationValidation.nonNull("journal.ackMode", ackMode);
        ConfigurationValidation.nonNegative("journal.stateFlushDebounce", stateFlushDebounce);
        ConfigurationValidation.nonNegative("journal.linger", linger);
        ConfigurationValidation.positive("journal.maxBatchRecords", maxBatchRecords);
        ConfigurationValidation.positive("journal.maxBatchBytes", maxBatchBytes);
        ConfigurationValidation.positive("journal.writerIdleTimeout", writerIdleTimeout);
        ConfigurationValidation.positive("journal.asyncQueueCapacityPerTenant", asyncQueueCapacityPerTenant);
        ConfigurationValidation.nonNegative("journal.balancedFlushWindow", balancedFlushWindow);
    }
}
