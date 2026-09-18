package io.github.cocosip.stow.config;

import java.time.Duration;

public record CleanupConfiguration(
        boolean enabled,
        Duration interval,
        Duration initialDelay,
        Duration processingTimeout,
        Duration completedRetention,
        Duration failedRetention,
        PermanentlyFailedDisposition permanentlyFailedDisposition,
        int batchSizePerTenant) {

    public CleanupConfiguration {
        ConfigurationValidation.positive("cleanup.interval", interval);
        ConfigurationValidation.nonNegative("cleanup.initialDelay", initialDelay);
        ConfigurationValidation.positive("cleanup.processingTimeout", processingTimeout);
        ConfigurationValidation.nonNegative("cleanup.completedRetention", completedRetention);
        ConfigurationValidation.nonNegative("cleanup.failedRetention", failedRetention);
        ConfigurationValidation.nonNull("cleanup.permanentlyFailedDisposition", permanentlyFailedDisposition);
        ConfigurationValidation.positive("cleanup.batchSizePerTenant", batchSizePerTenant);
    }
}
