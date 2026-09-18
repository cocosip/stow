package io.github.cocosip.stow.config;

public record CompactionConfiguration(boolean enabled, long minimumProcessedBytes) {

    public CompactionConfiguration {
        ConfigurationValidation.positive("compaction.minimumProcessedBytes", minimumProcessedBytes);
    }
}
