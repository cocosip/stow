package io.github.cocosip.stow.config;

import java.time.Duration;

public record OrphanRecoveryConfiguration(boolean enabled, boolean runOnStartup, Duration interval) {

    public OrphanRecoveryConfiguration {
        ConfigurationValidation.positive("orphanRecovery.interval", interval);
    }
}
