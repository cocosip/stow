package io.github.cocosip.stow.benchmarks;

import io.github.cocosip.stow.Stow;
import io.github.cocosip.stow.StowRuntime;
import io.github.cocosip.stow.api.ContentSources;
import io.github.cocosip.stow.api.StoragePool;
import io.github.cocosip.stow.config.StowConfiguration;
import io.github.cocosip.stow.config.VolumeConfiguration;
import io.github.cocosip.stow.model.FileProcessingStatus;
import io.github.cocosip.stow.model.TenantContext;
import io.github.cocosip.stow.model.WriteOptions;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Warmup;

/** JMH baseline for the public storage-pool operations. */
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
@Warmup(iterations = 2, time = 1)
@Measurement(iterations = 3, time = 1)
@Fork(1)
@State(Scope.Benchmark)
public class StoragePoolBenchmark {

    /** Creates the benchmark state holder. */
    public StoragePoolBenchmark() {}

    @Param({"1024"})
    int payloadSize;

    private StowRuntime runtime;
    private StoragePool storagePool;
    private TenantContext tenant;
    private byte[] payload;
    private String readableFileKey;
    private Path root;

    /** Creates an isolated runtime and a completed file for each fork. */
    @Setup(Level.Trial)
    public void setUp() throws IOException {
        root = Files.createTempDirectory(Path.of("benchmarks", "target"), "stow-jmh-");
        runtime = Stow.open(configuration(root));
        storagePool = runtime.storagePool();
        tenant = runtime.tenantManager().get("benchmark");
        payload = new byte[payloadSize];
        for (int index = 0; index < payload.length; index++) {
            payload[index] = (byte) (index & 0x7f);
        }
        readableFileKey = storagePool.write(tenant, ContentSources.of(payload), WriteOptions.defaults());
        complete();
    }

    /** Closes the runtime and removes the fork-local storage tree. */
    @TearDown(Level.Trial)
    public void tearDown() throws IOException {
        if (runtime != null) {
            runtime.close();
        }
        deleteRecursively(root);
    }

    /** Measures durable writes of a 1 KiB payload. */
    @Benchmark
    public String write() {
        return storagePool.write(tenant, ContentSources.of(payload), WriteOptions.defaults());
    }

    /** Measures reads of a completed file. */
    @Benchmark
    public int read() throws IOException {
        try (InputStream input = storagePool.read(tenant, readableFileKey)) {
            return input.readAllBytes().length;
        }
    }

    /** Measures the write, claim, and complete workflow. */
    @Benchmark
    public FileProcessingStatus writeClaimComplete() {
        String fileKey = storagePool.write(tenant, ContentSources.of(payload), WriteOptions.defaults());
        complete();
        return storagePool.status(tenant, fileKey);
    }

    private void complete() {
        var claimed = storagePool.claimNext(tenant);
        if (claimed.isEmpty()) {
            throw new IllegalStateException("The benchmark queue did not expose the written file");
        }
        storagePool.complete(claimed.orElseThrow().lease());
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

    private static void deleteRecursively(Path path) throws IOException {
        if (path == null || !Files.exists(path)) {
            return;
        }
        try (var paths = Files.walk(path)) {
            paths.sorted((left, right) -> right.getNameCount() - left.getNameCount())
                    .forEach(candidate -> {
                        try {
                            Files.deleteIfExists(candidate);
                        } catch (IOException exception) {
                            throw new IllegalStateException("Could not clean benchmark directory", exception);
                        }
                    });
        }
    }
}
