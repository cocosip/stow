package io.github.cocosip.stow.api;

import io.github.cocosip.stow.model.WatcherRootConfiguration;
import java.util.Optional;

public interface FileWatcherAutoManager {

    int apply(WatcherRootConfiguration configuration);

    int discoverAndCreate();

    void removeManagedWatchers();

    Optional<WatcherRootConfiguration> currentRoot();
}
