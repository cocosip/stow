package io.github.cocosip.stow.internal.runtime;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.awaitility.Awaitility.await;

import io.github.cocosip.stow.RuntimeState;
import io.github.cocosip.stow.Stow;
import io.github.cocosip.stow.StowRuntime;
import io.github.cocosip.stow.config.StowConfiguration;
import io.github.cocosip.stow.exception.RuntimeDirectoryLockedException;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Clock;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.stream.Collectors;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class DefaultStowRuntimeTest {

    @TempDir
    Path temporaryDirectory;

    @Test
    void transitionsThroughLifecycleWithoutStartingThreadsDuringBuild() {
        Set<Long> existingStowThreads = stowThreadIds();
        StowRuntime runtime =
                Stow.builder().configuration(configuration("lifecycle")).build();

        assertThat(runtime.state()).isEqualTo(RuntimeState.NEW);
        assertThat(stowThreadIds()).isEqualTo(existingStowThreads);

        runtime.start();
        assertThat(runtime.state()).isEqualTo(RuntimeState.RUNNING);

        runtime.close();
        assertThat(runtime.state()).isEqualTo(RuntimeState.TERMINATED);
        await().atMost(Duration.ofSeconds(2))
                .untilAsserted(() -> assertThat(stowThreadIds()).isEqualTo(existingStowThreads));
    }

    @Test
    void rejectsDuplicateStartAndClosesIdempotently() {
        StowRuntime runtime =
                Stow.builder().configuration(configuration("duplicate")).build();
        runtime.start();

        assertThatThrownBy(runtime::start).isInstanceOf(IllegalStateException.class);

        runtime.close();
        runtime.close();
        assertThat(runtime.state()).isEqualTo(RuntimeState.TERMINATED);
    }

    @Test
    void closesNewRuntimeWithoutStartingIt() {
        StowRuntime runtime =
                Stow.builder().configuration(configuration("never-started")).build();

        runtime.close();
        runtime.close();

        assertThat(runtime.state()).isEqualTo(RuntimeState.TERMINATED);
    }

    @Test
    void entersFailedStateWhenStartupCannotCreateRuntimeDirectory() throws IOException {
        Path metadataFile = temporaryDirectory.resolve("not-a-directory");
        Files.writeString(metadataFile, "occupied");
        StowConfiguration configuration = StowConfiguration.builder()
                .metadataDirectory(metadataFile)
                .quotaDirectory(temporaryDirectory.resolve("failed-quota"))
                .queueDirectory(temporaryDirectory.resolve("failed-queue"))
                .watcherDirectory(temporaryDirectory.resolve("failed-watchers"))
                .build();
        StowRuntime runtime = Stow.builder().configuration(configuration).build();

        assertThatThrownBy(runtime::start).isInstanceOf(RuntimeDirectoryLockedException.class);
        assertThat(runtime.state()).isEqualTo(RuntimeState.FAILED);

        runtime.close();
        assertThat(runtime.state()).isEqualTo(RuntimeState.TERMINATED);
    }

    @Test
    void preventsTwoRunningRuntimesFromSharingDirectories() {
        StowConfiguration configuration = configuration("locked");
        StowRuntime first = Stow.open(configuration);
        StowRuntime second = Stow.builder().configuration(configuration).build();
        try {
            assertThatThrownBy(second::start).isInstanceOf(RuntimeDirectoryLockedException.class);
            assertThat(second.state()).isEqualTo(RuntimeState.FAILED);
        } finally {
            second.close();
            first.close();
        }
    }

    @Test
    void preservesCallerOwnedExecutors() {
        ExecutorService worker = Executors.newSingleThreadExecutor();
        ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();
        try {
            StowRuntime runtime = Stow.builder()
                    .configuration(configuration("owned"))
                    .workerExecutor(worker)
                    .scheduler(scheduler)
                    .build();

            runtime.start();
            runtime.close();

            assertThat(worker.isShutdown()).isFalse();
            assertThat(scheduler.isShutdown()).isFalse();
        } finally {
            worker.shutdownNow();
            scheduler.shutdownNow();
        }
    }

    @Test
    void closesStartedManagedServicesInReverseOrder() {
        List<String> closed = new ArrayList<>();
        List<ManagedBackgroundService> services = List.of(
                new RecordingService("first", closed),
                new RecordingService("second", closed),
                new RecordingService("third", closed));
        DefaultStowRuntime runtime =
                new DefaultStowRuntime(configuration("resources"), Clock.systemUTC(), null, null, services);

        runtime.start();
        runtime.close();

        assertThat(closed).containsExactly("third", "second", "first");
    }

    private StowConfiguration configuration(String name) {
        Path root = temporaryDirectory.resolve(name);
        return StowConfiguration.builder()
                .metadataDirectory(root.resolve("metadata"))
                .quotaDirectory(root.resolve("quota"))
                .queueDirectory(root.resolve("queue"))
                .watcherDirectory(root.resolve("watchers"))
                .build();
    }

    private static Set<Long> stowThreadIds() {
        return Thread.getAllStackTraces().keySet().stream()
                .filter(Thread::isAlive)
                .filter(thread -> thread.getName().startsWith("stow-"))
                .map(Thread::threadId)
                .collect(Collectors.toSet());
    }

    private record RecordingService(String name, List<String> closed) implements ManagedBackgroundService {

        @Override
        public void start() {}

        @Override
        public void close() {
            closed.add(name);
        }
    }
}
