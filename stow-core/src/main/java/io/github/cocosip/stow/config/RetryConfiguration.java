package io.github.cocosip.stow.config;

import java.time.Duration;

public record RetryConfiguration(
        int maxRetryCount, Duration initialDelay, boolean exponentialBackoff, Duration maxDelay) {

    public RetryConfiguration {
        ConfigurationValidation.nonNegative("maxRetryCount", maxRetryCount);
        ConfigurationValidation.nonNegative("initialDelay", initialDelay);
        ConfigurationValidation.nonNegative("maxDelay", maxDelay);
        if (maxDelay.compareTo(initialDelay) < 0) {
            throw ConfigurationValidation.invalid("maxDelay", "must not be shorter than initialDelay");
        }
    }
}
