package io.github.cocosip.stow.model;

import java.nio.file.Path;
import java.util.List;

public record WatcherRootConfiguration(
        Path rootPath,
        boolean enabled,
        boolean autoCreateTenantDirectories,
        boolean recursive,
        List<String> globs,
        PostImportAction postImportAction,
        Path moveDirectory,
        Path sourceCleanupFailureDirectory) {

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
