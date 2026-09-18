package io.github.cocosip.stow;

import io.github.cocosip.stow.api.DirectoryQuotaManager;
import io.github.cocosip.stow.api.FileWatcherAutoManager;
import io.github.cocosip.stow.api.FileWatcherManager;
import io.github.cocosip.stow.api.FileWatcherOptionsManager;
import io.github.cocosip.stow.api.QueueProjectionMaintenance;
import io.github.cocosip.stow.api.StatisticsReader;
import io.github.cocosip.stow.api.StorageMaintenance;
import io.github.cocosip.stow.api.StoragePool;
import io.github.cocosip.stow.api.TenantManager;
import io.github.cocosip.stow.api.TenantQuotaManager;
import io.github.cocosip.stow.model.RuntimeHealth;

public interface StowRuntime extends AutoCloseable {

    RuntimeState state();

    void start();

    StoragePool storagePool();

    TenantManager tenantManager();

    TenantQuotaManager tenantQuotaManager();

    DirectoryQuotaManager directoryQuotaManager();

    StorageMaintenance maintenance();

    QueueProjectionMaintenance projectionMaintenance();

    FileWatcherManager fileWatcherManager();

    FileWatcherOptionsManager fileWatcherOptionsManager();

    FileWatcherAutoManager fileWatcherAutoManager();

    StatisticsReader statisticsReader();

    RuntimeHealth health();

    @Override
    void close();
}
