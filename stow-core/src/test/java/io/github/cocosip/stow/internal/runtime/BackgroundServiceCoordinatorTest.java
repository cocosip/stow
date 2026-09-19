package io.github.cocosip.stow.internal.runtime;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.github.cocosip.stow.RuntimeState;
import io.github.cocosip.stow.model.HealthStatus;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;

class BackgroundServiceCoordinatorTest {

    @Test
    void startsInDependencyOrderAndClosesInReverseOrder() {
        List<String> events = new ArrayList<>();
        BackgroundServiceCoordinator coordinator = new BackgroundServiceCoordinator(List.of(
                service("projector", events),
                service("timeout", events),
                service("cleanup", events),
                service("orphan", events),
                service("watcher", events),
                service("statistics", events)));

        coordinator.start();
        coordinator.close();

        assertThat(events)
                .containsExactly(
                        "start:projector",
                        "start:timeout",
                        "start:cleanup",
                        "start:orphan",
                        "start:watcher",
                        "start:statistics",
                        "close:statistics",
                        "close:watcher",
                        "close:orphan",
                        "close:cleanup",
                        "close:timeout",
                        "close:projector");
    }

    @Test
    void closesAlreadyStartedServicesWhenStartupFails() {
        List<String> events = new ArrayList<>();
        BackgroundServiceCoordinator coordinator = new BackgroundServiceCoordinator(
                List.of(service("projector", events), new RecordingService("watcher", events, true)));

        assertThatThrownBy(coordinator::start).isInstanceOf(IllegalStateException.class);
        assertThat(events).containsExactly("start:projector", "start:watcher", "close:projector");
    }

    @Test
    void aggregatesNamedHealthComponentsWithoutHidingDegradedState() {
        DefaultRuntimeHealth health =
                new DefaultRuntimeHealth(Clock.fixed(Instant.parse("2026-09-19T00:00:00Z"), ZoneOffset.UTC));
        health.update("watcher", HealthStatus.DEGRADED, "scan failed");
        assertThat(health.snapshot(RuntimeState.RUNNING).status()).isEqualTo(HealthStatus.DEGRADED);
        health.update("watcher", HealthStatus.UP, "ready");
        health.update("cleanup", HealthStatus.DOWN, "database unavailable");
        assertThat(health.snapshot(RuntimeState.RUNNING).status()).isEqualTo(HealthStatus.DOWN);
        assertThat(health.snapshot(RuntimeState.RUNNING).components()).containsKeys("runtime", "watcher", "cleanup");
    }

    private static BackgroundServiceCoordinator.Service service(String name, List<String> events) {
        return new RecordingService(name, events, false);
    }

    private record RecordingService(String name, List<String> events, boolean failStart)
            implements BackgroundServiceCoordinator.Service {
        @Override
        public void start() {
            events.add("start:" + name);
            if (failStart) throw new IllegalStateException("injected");
        }

        @Override
        public void close() {
            events.add("close:" + name);
        }
    }
}
