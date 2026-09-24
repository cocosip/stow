package io.github.cocosip.stow.internal.watcher;

import io.github.cocosip.stow.api.FileWatcherManager;
import io.github.cocosip.stow.api.FileWatcherOptionsManager;
import io.github.cocosip.stow.config.SourceCleanupConfiguration;
import io.github.cocosip.stow.model.WatcherConfiguration;
import java.util.Objects;

final class SourceCleanupGate {

    private final SourceCleanupConfiguration configuration;
    private final FileWatcherOptionsManager options;
    private final FileWatcherManager watchers;

    SourceCleanupGate(
            SourceCleanupConfiguration configuration, FileWatcherOptionsManager options, FileWatcherManager watchers) {
        this.configuration = Objects.requireNonNull(configuration, "configuration");
        this.options = Objects.requireNonNull(options, "options");
        this.watchers = Objects.requireNonNull(watchers, "watchers");
    }

    boolean enabled() {
        return configuration.enabled()
                && options.get().enabled()
                && watchers.list().stream().anyMatch(WatcherConfiguration::enabled);
    }
}
