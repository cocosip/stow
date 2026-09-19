package io.github.cocosip.stow.sample;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.Map;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.boot.WebApplicationType;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.context.ConfigurableApplicationContext;

class StowSampleApplicationTest {

    Path temporaryDirectory;

    @BeforeEach
    void setUp() throws IOException {
        temporaryDirectory = Files.createTempDirectory(Path.of("target"), "spring-sample-");
    }

    @AfterEach
    void tearDown() throws IOException {
        try (var paths = Files.walk(temporaryDirectory)) {
            paths.sorted(Comparator.reverseOrder()).forEach(StowSampleApplicationTest::delete);
        }
    }

    @Test
    void startsClosesAndRestartsTheSpringSampleAgainstTheSameDirectories() {
        String output = captureOutput(() -> {
            runSample();
            runSample();
        });

        assertTrue(output.contains("read back: hello from Spring Boot"));
        assertEquals(
                2,
                output.lines()
                        .filter(line -> line.contains("read back: hello from Spring Boot"))
                        .count());
    }

    private void runSample() {
        Map<String, Object> properties = Map.of(
                "stow.paths.metadata-directory",
                temporaryDirectory.resolve("metadata"),
                "stow.paths.quota-directory",
                temporaryDirectory.resolve("quota"),
                "stow.paths.queue-directory",
                temporaryDirectory.resolve("queue"),
                "stow.paths.watcher-directory",
                temporaryDirectory.resolve("watchers"),
                "stow.volumes[0].id",
                "primary",
                "stow.volumes[0].mount-path",
                temporaryDirectory.resolve("volume"),
                "stow.volumes[0].sharding-depth",
                2,
                "stow.volumes[0].buffer-size",
                65_536,
                "stow.volumes[0].force-flush-after-write",
                true,
                "spring.main.web-application-type",
                "none");
        try (ConfigurableApplicationContext context = new SpringApplicationBuilder(StowSampleApplication.class)
                .web(WebApplicationType.NONE)
                .properties(properties)
                .run()) {
            assertTrue(context.isRunning());
        }
    }

    private static String captureOutput(Runnable operation) {
        ByteArrayOutputStream output = new ByteArrayOutputStream();
        PrintStream original = System.out;
        try (PrintStream capture = new PrintStream(output, true, StandardCharsets.UTF_8)) {
            System.setOut(capture);
            operation.run();
        } finally {
            System.setOut(original);
        }
        return output.toString(StandardCharsets.UTF_8);
    }

    private static void delete(Path path) {
        try {
            Files.deleteIfExists(path);
        } catch (IOException exception) {
            throw new IllegalStateException("Could not clean sample test directory", exception);
        }
    }
}
