package io.github.cocosip.stow;

import io.github.cocosip.stow.config.StowConfiguration;
import io.github.cocosip.stow.internal.runtime.DefaultStowRuntimeFactory;
import io.github.cocosip.stow.spi.JournalCodec;
import io.github.cocosip.stow.spi.StorageVolumeProvider;
import java.time.Clock;
import java.util.Objects;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;

public final class StowBuilder {

    private StowConfiguration configuration = StowConfiguration.builder().build();
    private Clock clock = Clock.systemUTC();
    private ExecutorService workerExecutor;
    private ScheduledExecutorService scheduler;
    private StorageVolumeProvider storageVolumeProvider;
    private JournalCodec journalCodec;

    StowBuilder() {}

    public StowBuilder configuration(StowConfiguration configuration) {
        this.configuration = Objects.requireNonNull(configuration, "configuration");
        return this;
    }

    public StowBuilder clock(Clock clock) {
        this.clock = Objects.requireNonNull(clock, "clock");
        return this;
    }

    public StowBuilder workerExecutor(ExecutorService executor) {
        workerExecutor = Objects.requireNonNull(executor, "executor");
        return this;
    }

    public StowBuilder scheduler(ScheduledExecutorService scheduler) {
        this.scheduler = Objects.requireNonNull(scheduler, "scheduler");
        return this;
    }

    public StowBuilder storageVolumeProvider(StorageVolumeProvider provider) {
        storageVolumeProvider = Objects.requireNonNull(provider, "provider");
        return this;
    }

    public StowBuilder journalCodec(JournalCodec codec) {
        journalCodec = Objects.requireNonNull(codec, "codec");
        return this;
    }

    public StowRuntime build() {
        return DefaultStowRuntimeFactory.create(
                configuration, clock, workerExecutor, scheduler, storageVolumeProvider, journalCodec);
    }
}
