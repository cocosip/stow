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
        Path moveDirectory) {

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
    }
}
