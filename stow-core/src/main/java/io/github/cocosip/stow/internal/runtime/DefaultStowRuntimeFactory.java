package io.github.cocosip.stow.internal.runtime;

import io.github.cocosip.stow.StowRuntime;
import io.github.cocosip.stow.config.StowConfiguration;
import io.github.cocosip.stow.spi.JournalCodec;
import io.github.cocosip.stow.spi.StorageVolumeProvider;
import java.time.Clock;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;

public final class DefaultStowRuntimeFactory {

    private DefaultStowRuntimeFactory() {}

    public static StowRuntime create(
            StowConfiguration configuration,
            Clock clock,
            ExecutorService workerExecutor,
            ScheduledExecutorService scheduler,
            StorageVolumeProvider storageVolumeProvider,
            JournalCodec journalCodec) {
        return new DefaultStowRuntime(
                configuration, clock, workerExecutor, scheduler, storageVolumeProvider, journalCodec, List.of());
    }
}
