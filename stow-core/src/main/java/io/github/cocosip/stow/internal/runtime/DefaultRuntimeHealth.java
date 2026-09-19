package io.github.cocosip.stow.internal.runtime;

import io.github.cocosip.stow.RuntimeState;
import io.github.cocosip.stow.model.ComponentHealth;
import io.github.cocosip.stow.model.HealthStatus;
import io.github.cocosip.stow.model.RuntimeHealth;
import java.time.Clock;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;

public final class DefaultRuntimeHealth {

    private final Clock clock;
    private final Map<String, ComponentHealth> components = new ConcurrentHashMap<>();

    public DefaultRuntimeHealth(Clock clock) {
        this.clock = Objects.requireNonNull(clock, "clock");
    }

    public void update(String component, HealthStatus status, String summary) {
        Objects.requireNonNull(component, "component");
        components.put(component, new ComponentHealth(status, summary, clock.instant()));
    }

    public void remove(String component) {
        components.remove(component);
    }

    public RuntimeHealth snapshot(RuntimeState runtimeState) {
        Objects.requireNonNull(runtimeState, "runtimeState");
        HealthStatus runtimeStatus =
                switch (runtimeState) {
                    case RUNNING -> HealthStatus.UP;
                    case NEW, STARTING, STOPPING -> HealthStatus.DEGRADED;
                    case TERMINATED, FAILED -> HealthStatus.DOWN;
                };
        Map<String, ComponentHealth> result = new java.util.LinkedHashMap<>(components);
        result.put(
                "runtime",
                new ComponentHealth(runtimeStatus, runtimeState.name().toLowerCase(), clock.instant()));
        HealthStatus aggregate = runtimeStatus;
        for (ComponentHealth component : result.values()) {
            if (component.status() == HealthStatus.DOWN) {
                aggregate = HealthStatus.DOWN;
                break;
            }
            if (component.status() == HealthStatus.DEGRADED && aggregate == HealthStatus.UP) {
                aggregate = HealthStatus.DEGRADED;
            }
        }
        return new RuntimeHealth(aggregate, result);
    }

    public Map<String, ComponentHealth> components() {
        return Map.copyOf(components);
    }
}
