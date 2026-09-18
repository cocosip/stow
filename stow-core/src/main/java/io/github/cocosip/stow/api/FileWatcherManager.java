package io.github.cocosip.stow.api;

import io.github.cocosip.stow.model.WatcherConfiguration;
import io.github.cocosip.stow.model.WatcherScanResult;
import java.util.List;
import java.util.Optional;

public interface FileWatcherManager {

    WatcherConfiguration register(WatcherConfiguration configuration);

    WatcherConfiguration update(WatcherConfiguration configuration);

    void remove(String watcherId);

    void enable(String watcherId);

    void disable(String watcherId);

    Optional<WatcherConfiguration> find(String watcherId);

    List<WatcherConfiguration> list();

    List<WatcherConfiguration> listForTenant(String tenantId);

    WatcherScanResult scanNow(String watcherId);
}
