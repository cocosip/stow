package io.github.cocosip.stow.config;

import java.nio.file.Path;

public record VolumeConfiguration(
        String id, Path mountPath, int shardingDepth, int bufferSize, boolean forceFlushAfterWrite) {

    public VolumeConfiguration {
        if (id == null || id.isBlank()) {
            throw ConfigurationValidation.invalid("volume.id", "must not be blank");
        }
        mountPath = ConfigurationValidation.normalizedPath("volume.mountPath", mountPath);
        if (shardingDepth < 0 || shardingDepth > 3) {
            throw ConfigurationValidation.invalid("volume.shardingDepth", "must be between 0 and 3");
        }
        ConfigurationValidation.positive("volume.bufferSize", bufferSize);
    }
}
