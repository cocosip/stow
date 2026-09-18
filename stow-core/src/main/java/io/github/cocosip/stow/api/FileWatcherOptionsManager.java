package io.github.cocosip.stow.api;

import io.github.cocosip.stow.model.WatcherOptions;

public interface FileWatcherOptionsManager {

    WatcherOptions get();

    void update(WatcherOptions options);

    void enable();

    void disable();
}
