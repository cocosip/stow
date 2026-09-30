package io.github.cocosip.stow.model;

import java.nio.file.Path;
import java.time.Duration;
import java.util.List;

public record WatcherRootConfiguration(
        Path rootPath,
        boolean enabled,
        boolean autoCreateTenantDirectories,
        boolean recursive,
        List<String> globs,
        PostImportAction postImportAction,
        Path moveDirectory,
        Path sourceCleanupFailureDirectory,
        Duration pollInterval,
        long maxFileSize,
        Duration minimumFileAge,
        Duration stabilityCheckInterval,
        int stabilityCheckCount,
        int concurrentImports) {

    private static final Path DEFAULT_FAILURE_DIRECTORY = Path.of("stow-source-failed");

    public WatcherRootConfiguration {
        ModelValidation.required("rootPath", rootPath);
        rootPath = rootPath.toAbsolutePath().normalize();
        globs = List.copyOf(ModelValidation.required("globs", globs));
        ModelValidation.required("postImportAction", postImportAction);
        if (postImportAction == PostImportAction.MOVE && moveDirectory == null) {
            throw ModelValidation.invalid("moveDirectory", "is required for MOVE");
        }
        if (moveDirectory != null) {
            moveDirectory = moveDirectory.toAbsolutePath().normalize();
        }
        if (sourceCleanupFailureDirectory != null) {
            sourceCleanupFailureDirectory =
                    sourceCleanupFailureDirectory.toAbsolutePath().normalize();
        }
        ModelValidation.nonNegative("pollInterval", pollInterval);
        ModelValidation.nonNegative("maxFileSize", maxFileSize);
        ModelValidation.nonNegative("minimumFileAge", minimumFileAge);
        ModelValidation.nonNegative("stabilityCheckInterval", stabilityCheckInterval);
        ModelValidation.positive("stabilityCheckCount", stabilityCheckCount);
        ModelValidation.positive("concurrentImports", concurrentImports);
    }

    public WatcherRootConfiguration(
            Path rootPath,
            boolean enabled,
            boolean autoCreateTenantDirectories,
            boolean recursive,
            List<String> globs,
            PostImportAction postImportAction,
            Path moveDirectory,
            Path sourceCleanupFailureDirectory) {
        this(
                rootPath,
                enabled,
                autoCreateTenantDirectories,
                recursive,
                globs,
                postImportAction,
                moveDirectory,
                sourceCleanupFailureDirectory,
                Duration.ofSeconds(30),
                0,
                Duration.ofSeconds(5),
                Duration.ofMillis(100),
                2,
                4);
    }

    public WatcherRootConfiguration(
            Path rootPath,
            boolean enabled,
            boolean autoCreateTenantDirectories,
            boolean recursive,
            List<String> globs,
            PostImportAction postImportAction,
            Path moveDirectory) {
        this(
                rootPath,
                enabled,
                autoCreateTenantDirectories,
                recursive,
                globs,
                postImportAction,
                moveDirectory,
                DEFAULT_FAILURE_DIRECTORY);
    }
}
