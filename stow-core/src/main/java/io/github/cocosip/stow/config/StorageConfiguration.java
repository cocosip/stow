package io.github.cocosip.stow.config;

import java.time.Duration;

public record StorageConfiguration(
        int completionGuardStripes,
        int emptyQueueReclaimBatchSize,
        int backgroundReclaimBatchSize,
        Duration reclaimCooldown,
        boolean backgroundReclaimEnabled) {

    public StorageConfiguration {
        ConfigurationValidation.positive("completionGuardStripes", completionGuardStripes);
        if ((completionGuardStripes & (completionGuardStripes - 1)) != 0) {
            throw ConfigurationValidation.invalid("completionGuardStripes", "must be a power of two");
        }
        ConfigurationValidation.positive("emptyQueueReclaimBatchSize", emptyQueueReclaimBatchSize);
        ConfigurationValidation.positive("backgroundReclaimBatchSize", backgroundReclaimBatchSize);
        ConfigurationValidation.nonNegative("reclaimCooldown", reclaimCooldown);
    }
}
