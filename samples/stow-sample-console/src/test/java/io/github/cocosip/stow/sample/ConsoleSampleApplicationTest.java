package io.github.cocosip.stow.sample;

import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class ConsoleSampleApplicationTest {

    Path temporaryDirectory;

    @BeforeEach
    void setUp() throws IOException {
        temporaryDirectory = Files.createTempDirectory(Path.of("target"), "console-sample-");
    }

    @AfterEach
    void tearDown() throws IOException {
        try (var paths = Files.walk(temporaryDirectory)) {
            paths.sorted(Comparator.reverseOrder()).forEach(ConsoleSampleApplicationTest::delete);
        }
    }

    @Test
    void runsTheCompleteOperationAndReadsTheSameFileAfterReopen() {
        String output = runSample(temporaryDirectory);

        assertTrue(output.contains("read back: hello from Stow"));
        assertTrue(output.contains("reopened read: hello from Stow"));
        assertTrue(output.contains("COMPLETED"));
    }

    private static String runSample(Path root) {
        ByteArrayOutputStream output = new ByteArrayOutputStream();
        PrintStream original = System.out;
        try (PrintStream capture = new PrintStream(output, true, StandardCharsets.UTF_8)) {
            System.setOut(capture);
            ConsoleSampleApplication.main(new String[] {root.toString()});
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
