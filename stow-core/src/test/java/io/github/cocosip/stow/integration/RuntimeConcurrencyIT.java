package io.github.cocosip.stow.integration;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.cocosip.stow.Stow;
import io.github.cocosip.stow.StowRuntime;
import io.github.cocosip.stow.api.ContentSources;
import io.github.cocosip.stow.config.StowConfiguration;
import io.github.cocosip.stow.config.VolumeConfiguration;
import io.github.cocosip.stow.model.FileProcessingStatus;
import io.github.cocosip.stow.model.TenantContext;
import io.github.cocosip.stow.model.WriteOptions;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/** Exercises concurrent writes, claims, and completions through the public runtime. */
class RuntimeConcurrencyIT {

    @TempDir
    Path temporaryDirectory;

    @Test
    void completesEveryFileWithMultipleProducersAndConsumers() throws Exception {
        Path root = temporaryDirectory.resolve("runtime");
        int producerCount = 4;
        int filesPerProducer = 4;
        int expected = producerCount * filesPerProducer;
        try (StowRuntime runtime = Stow.open(configuration(root))) {
            TenantContext tenant = runtime.tenantManager().get("concurrency");
            Set<String> keys = ConcurrentHashMap.newKeySet();
            ExecutorService producers = Executors.newVirtualThreadPerTaskExecutor();
            try {
                List<java.util.concurrent.Future<?>> tasks = new ArrayList<>();
                for (int producer = 0; producer < producerCount; producer++) {
                    int producerId = producer;
                    tasks.add(producers.submit(() -> {
                        for (int index = 0; index < filesPerProducer; index++) {
                            String key = runtime.storagePool()
                                    .write(
                                            tenant,
                                            ContentSources.of(
                                                    (producerId + ":" + index).getBytes(StandardCharsets.UTF_8)),
                                            WriteOptions.defaults());
                            keys.add(key);
                        }
                    }));
                }
                for (var task : tasks) {
                    task.get(120, TimeUnit.SECONDS);
                }
            } finally {
                producers.close();
            }
            assertThat(keys).hasSize(expected);

            ExecutorService consumers = Executors.newFixedThreadPool(4);
            CountDownLatch start = new CountDownLatch(1);
            AtomicInteger completed = new AtomicInteger();
            Set<String> claimed = ConcurrentHashMap.newKeySet();
            try {
                List<java.util.concurrent.Future<?>> tasks = new ArrayList<>();
                for (int index = 0; index < 4; index++) {
                    tasks.add(consumers.submit(() -> {
                        start.await();
                        long deadline =
                                System.nanoTime() + Duration.ofSeconds(120).toNanos();
                        while (completed.get() < expected && System.nanoTime() < deadline) {
                            var next = runtime.storagePool().claimNext(tenant);
                            if (next.isEmpty()) {
                                Thread.onSpinWait();
                                continue;
                            }
                            var file = next.orElseThrow();
                            claimed.add(file.lease().fileKey());
                            runtime.storagePool().complete(file.lease());
                            completed.incrementAndGet();
                        }
                        return null;
                    }));
                }
                start.countDown();
                for (var task : tasks) {
                    task.get(135, TimeUnit.SECONDS);
                }
            } finally {
                consumers.shutdownNow();
                consumers.awaitTermination(10, TimeUnit.SECONDS);
            }
            assertThat(completed).hasValue(expected);
            assertThat(claimed).containsExactlyInAnyOrderElementsOf(keys);
            for (String key : keys) {
                assertThat(runtime.storagePool().status(tenant, key)).isEqualTo(FileProcessingStatus.COMPLETED);
            }
        }
    }

    private static StowConfiguration configuration(Path root) {
        return StowConfiguration.builder()
                .metadataDirectory(root.resolve("metadata"))
                .quotaDirectory(root.resolve("quota"))
                .queueDirectory(root.resolve("queue"))
                .watcherDirectory(root.resolve("watchers"))
                .autoCreateTenants(true)
                .volumes(List.of(new VolumeConfiguration("primary", root.resolve("volume"), 1, 8192, true)))
                .build();
    }
}
