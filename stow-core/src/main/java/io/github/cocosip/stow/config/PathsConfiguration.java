package io.github.cocosip.stow.config;

import java.nio.file.Path;
import java.util.List;

public record PathsConfiguration(
        Path metadataDirectory, Path quotaDirectory, Path queueDirectory, Path watcherDirectory) {

    public PathsConfiguration {
        metadataDirectory = ConfigurationValidation.normalizedPath("metadataDirectory", metadataDirectory);
        quotaDirectory = ConfigurationValidation.normalizedPath("quotaDirectory", quotaDirectory);
        queueDirectory = ConfigurationValidation.normalizedPath("queueDirectory", queueDirectory);
        watcherDirectory = ConfigurationValidation.normalizedPath("watcherDirectory", watcherDirectory);

        List<Path> roots = List.of(metadataDirectory, quotaDirectory, queueDirectory, watcherDirectory);
        for (int left = 0; left < roots.size(); left++) {
            for (int right = left + 1; right < roots.size(); right++) {
                Path first = roots.get(left);
                Path second = roots.get(right);
                if (first.startsWith(second) || second.startsWith(first)) {
                    throw ConfigurationValidation.invalid("paths", "must be distinct and must not contain one another");
                }
            }
        }
    }
}
