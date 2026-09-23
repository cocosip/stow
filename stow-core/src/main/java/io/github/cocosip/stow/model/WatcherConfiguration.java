package io.github.cocosip.stow.model;

import java.nio.file.Path;
import java.time.Duration;
import java.util.List;

public record WatcherConfiguration(
        String watcherId,
        String tenantId,
        WatcherTenantMode tenantMode,
        boolean autoCreateTenantDirectories,
        Path watchPath,
        boolean enabled,
        boolean recursive,
        List<String> globs,
        PostImportAction postImportAction,
        Path moveDirectory,
        Duration pollInterval,
        long maxFileSize,
        Duration minimumFileAge,
        Duration stabilityCheckInterval,
        int stabilityCheckCount,
        int concurrentImports,
        Duration historyRetention,
        Duration historyFlushInterval) {

    public WatcherConfiguration {
        watcherId = ModelValidation.identifier("watcherId", watcherId);
        ModelValidation.required("tenantMode", tenantMode);
        if (tenantMode == WatcherTenantMode.SINGLE_TENANT) {
            tenantId = ModelValidation.identifier("tenantId", tenantId);
        } else if (tenantId != null) {
            tenantId = ModelValidation.identifier("tenantId", tenantId);
        }
        ModelValidation.required("watchPath", watchPath);
        watchPath = watchPath.toAbsolutePath().normalize();
        // validate before the defensive copy: List.copyOf would throw a bare NPE on null entries
        if (ModelValidation.required("globs", globs).stream().anyMatch(glob -> glob == null || glob.isBlank())) {
            throw ModelValidation.invalid("globs", "must not contain blank entries");
        }
        globs = List.copyOf(globs);
        ModelValidation.required("postImportAction", postImportAction);
        if (postImportAction == PostImportAction.MOVE && moveDirectory == null) {
            throw ModelValidation.invalid("moveDirectory", "is required for MOVE");
        }
        if (moveDirectory != null) {
            moveDirectory = moveDirectory.toAbsolutePath().normalize();
        }
        ModelValidation.nonNegative("pollInterval", pollInterval);
        ModelValidation.nonNegative("maxFileSize", maxFileSize);
        ModelValidation.nonNegative("minimumFileAge", minimumFileAge);
        ModelValidation.nonNegative("stabilityCheckInterval", stabilityCheckInterval);
        ModelValidation.positive("stabilityCheckCount", stabilityCheckCount);
        ModelValidation.positive("concurrentImports", concurrentImports);
        ModelValidation.nonNegative("historyRetention", historyRetention);
        ModelValidation.nonNegative("historyFlushInterval", historyFlushInterval);
    }
}
