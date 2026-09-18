package io.github.cocosip.stow.config;

import java.time.Duration;

public record SnapshotConfiguration(boolean enabled, Duration interval, long minimumProgressBytes) {

    public SnapshotConfiguration {
        ConfigurationValidation.positive("snapshot.interval", interval);
        ConfigurationValidation.positive("snapshot.minimumProgressBytes", minimumProgressBytes);
    }
}
