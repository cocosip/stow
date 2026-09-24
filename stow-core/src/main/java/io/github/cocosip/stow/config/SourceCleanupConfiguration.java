package io.github.cocosip.stow.config;

import java.nio.file.Path;
import java.time.Duration;

public record SourceCleanupConfiguration(
        boolean enabled,
        Path databasePath,
        Duration pollInterval,
        int maxConcurrentActions,
        int maxActiveJobs,
        Duration terminalRetention,
        Duration importReservationTimeout,
        boolean databaseOptimizationEnabled,
        Duration databaseOptimizationInterval,
        int terminalPruneBatchSize) {

    public SourceCleanupConfiguration {
        databasePath = ConfigurationValidation.normalizedPath("sourceCleanup.databasePath", databasePath);
        ConfigurationValidation.positive("sourceCleanup.pollInterval", pollInterval);
        ConfigurationValidation.positive("sourceCleanup.maxConcurrentActions", maxConcurrentActions);
        ConfigurationValidation.positive("sourceCleanup.maxActiveJobs", maxActiveJobs);
        ConfigurationValidation.positive("sourceCleanup.terminalRetention", terminalRetention);
        ConfigurationValidation.positive("sourceCleanup.importReservationTimeout", importReservationTimeout);
        ConfigurationValidation.positive("sourceCleanup.databaseOptimizationInterval", databaseOptimizationInterval);
        ConfigurationValidation.positive("sourceCleanup.terminalPruneBatchSize", terminalPruneBatchSize);
    }
}
