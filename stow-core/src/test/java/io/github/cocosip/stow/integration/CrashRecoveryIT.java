package io.github.cocosip.stow.integration;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;

import io.github.cocosip.stow.Stow;
import io.github.cocosip.stow.StowRuntime;
import io.github.cocosip.stow.api.ContentSources;
import io.github.cocosip.stow.config.StowConfiguration;
import io.github.cocosip.stow.config.VolumeConfiguration;
import io.github.cocosip.stow.exception.RuntimeDirectoryLockedException;
import io.github.cocosip.stow.model.TenantContext;
import io.github.cocosip.stow.model.WriteOptions;
import java.nio.charset.StandardCharsets;
import java.nio.file.FileSystemException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/** Verifies that an abruptly terminated writer can be reopened safely. */
class CrashRecoveryIT {

    @TempDir
    Path temporaryDirectory;

    @Test
    void reopensAfterForkedWriterTermination() throws Exception {
        Path root = temporaryDirectory.resolve("runtime");
        Path marker = root.resolve("child-ready");
        Path childError = temporaryDirectory.resolve("child-error.log");
        Process child = startChild(root, childError);
        try {
            Awaitility.await().atMost(Duration.ofSeconds(15)).until(() -> {
                if (!child.isAlive() && Files.exists(childError)) {
                    throw new IllegalStateException(Files.readString(childError));
                }
                return Files.exists(marker);
            });
            child.destroyForcibly();
            assertThat(child.waitFor(15, TimeUnit.SECONDS)).isTrue();

            Path progress = root.resolve("written.keys");
            assertThat(Files.readAllLines(progress)).isNotEmpty();
            Awaitility.await()
                    .atMost(Duration.ofSeconds(15))
                    .ignoreException(RuntimeDirectoryLockedException.class)
                    .untilAsserted(() ->
                            assertThatCode(() -> reopenAndCheck(root, progress)).doesNotThrowAnyException());
        } finally {
            if (child.isAlive()) {
                child.destroyForcibly();
                child.waitFor(15, TimeUnit.SECONDS);
            }
            deleteRecursively(temporaryDirectory);
        }
    }

    public static void main(String[] args) throws Exception {
        if (args.length != 2 || !"child".equals(args[0])) {
            return;
        }
        runChild(Path.of(args[1]));
    }

    private Process startChild(Path root, Path childError) throws Exception {
        String javaExecutable = Path.of(System.getProperty("java.home"), "bin", isWindows() ? "java.exe" : "java")
                .toString();
        return new ProcessBuilder(
                        javaExecutable,
                        "-cp",
                        System.getProperty("java.class.path"),
                        CrashRecoveryIT.class.getName(),
                        "child",
                        root.toString())
                .redirectError(childError.toFile())
                .redirectOutput(ProcessBuilder.Redirect.DISCARD)
                .start();
    }

    private static void runChild(Path root) throws Exception {
        Files.createDirectories(root);
        Path marker = root.resolve("child-ready");
        Path progress = root.resolve("written.keys");
        StowConfiguration configuration = configuration(root);
        try (StowRuntime runtime = Stow.open(configuration)) {
            TenantContext tenant = runtime.tenantManager().get("crash-test");
            int sequence = 0;
            while (true) {
                String key = runtime.storagePool()
                        .write(
                                tenant,
                                ContentSources.of(("payload-" + sequence).getBytes(StandardCharsets.UTF_8)),
                                WriteOptions.ofOriginalFileName("payload.bin"));
                Files.writeString(
                        progress,
                        key + System.lineSeparator(),
                        StandardCharsets.UTF_8,
                        java.nio.file.StandardOpenOption.CREATE,
                        java.nio.file.StandardOpenOption.APPEND,
                        java.nio.file.StandardOpenOption.WRITE);
                if (sequence == 0) {
                    Files.writeString(marker, "ready", StandardCharsets.UTF_8);
                }
                sequence++;
            }
        }
    }

    private static void reopenAndCheck(Path root, Path progress) throws Exception {
        try (StowRuntime runtime = Stow.open(configuration(root))) {
            TenantContext tenant = runtime.tenantManager().get("crash-test");
            List<String> keys = Files.readAllLines(progress);
            assertThat(keys).isNotEmpty();
            for (String key : keys) {
                assertThat(runtime.storagePool().findFileInfo(tenant, key)).isPresent();
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

    private static void deleteRecursively(Path directory) throws Exception {
        if (Files.notExists(directory)) {
            return;
        }
        List<Path> paths;
        try (Stream<Path> walk = Files.walk(directory)) {
            paths = walk.sorted(Comparator.reverseOrder()).toList();
        }
        for (Path path : paths) {
            deleteWithRetry(path);
        }
    }

    private static void deleteWithRetry(Path path) throws Exception {
        int attempts = isWindows() ? 40 : 1;
        for (int attempt = 1; ; attempt++) {
            try {
                Files.deleteIfExists(path);
                return;
            } catch (FileSystemException exception) {
                if (!isWindows() || attempt >= attempts) {
                    throw exception;
                }
                Thread.sleep(100L);
            }
        }
    }

    private static boolean isWindows() {
        return System.getProperty("os.name", "")
                .toLowerCase(java.util.Locale.ROOT)
                .contains("win");
    }
}
