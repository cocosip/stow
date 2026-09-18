package io.github.cocosip.stow.config;

import java.time.Duration;

public record SqliteConfiguration(
        SqliteJournalMode journalMode,
        SqliteSynchronousMode synchronousMode,
        int cacheSizeKb,
        Duration busyTimeout,
        boolean checkpointAfterBatch) {

    public SqliteConfiguration {
        ConfigurationValidation.nonNull("journalMode", journalMode);
        ConfigurationValidation.nonNull("synchronousMode", synchronousMode);
        if (cacheSizeKb == 0) {
            throw ConfigurationValidation.invalid("cacheSizeKb", "must not be zero");
        }
        ConfigurationValidation.positive("busyTimeout", busyTimeout);
    }
}
