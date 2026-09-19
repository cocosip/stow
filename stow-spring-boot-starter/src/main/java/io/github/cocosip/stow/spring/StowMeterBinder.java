package io.github.cocosip.stow.spring;

import io.github.cocosip.stow.StowRuntime;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.binder.MeterBinder;
import java.util.Objects;

/** Exposes bounded runtime state gauges without adding metric labels from file data. */
public final class StowMeterBinder implements MeterBinder {

    private final StowRuntime runtime;

    public StowMeterBinder(StowRuntime runtime) {
        this.runtime = Objects.requireNonNull(runtime, "runtime");
    }

    @Override
    public void bindTo(MeterRegistry registry) {
        Gauge.builder("stow.runtime.state", runtime, value -> value.state().ordinal())
                .description("Current Stow runtime state ordinal")
                .register(registry);
        Gauge.builder(
                        "stow.runtime.up",
                        runtime,
                        value -> value.health().status().name().equals("UP") ? 1 : 0)
                .description("Whether the Stow runtime is healthy")
                .register(registry);
    }
}
