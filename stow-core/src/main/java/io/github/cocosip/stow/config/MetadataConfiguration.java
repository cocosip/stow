package io.github.cocosip.stow.config;

import java.time.Duration;

public record MetadataConfiguration(
        boolean backgroundPersistence,
        int maxQueueSize,
        int drainBatchSize,
        int softMergeThresholdPercent,
        int startupLoadBatchSize,
        Duration shutdownDrainTimeout,
        Duration persistenceInterval) {

    public MetadataConfiguration {
        ConfigurationValidation.positive("maxQueueSize", maxQueueSize);
        ConfigurationValidation.positive("drainBatchSize", drainBatchSize);
        if (softMergeThresholdPercent < 1 || softMergeThresholdPercent > 100) {
            throw ConfigurationValidation.invalid("softMergeThresholdPercent", "must be between 1 and 100");
        }
        ConfigurationValidation.positive("startupLoadBatchSize", startupLoadBatchSize);
        ConfigurationValidation.positive("shutdownDrainTimeout", shutdownDrainTimeout);
        ConfigurationValidation.positive("persistenceInterval", persistenceInterval);
    }
}
