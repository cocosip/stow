package io.github.cocosip.stow.internal.filesystem;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.github.cocosip.stow.config.VolumeConfiguration;
import io.github.cocosip.stow.exception.StorageVolumeUnavailableException;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class LocalFileSystemVolumeTest {

    private static final String FILE_KEY = "0123456789abcdef0123456789abcdef";

    @TempDir
    Path temporaryDirectory;

    @ParameterizedTest
    @ValueSource(ints = {0, 1, 2, 3})
    void buildsConfiguredShardDepthsUnderTheTenantDirectory(int depth) {
        Path mount = temporaryDirectory.resolve("depth-" + depth);
        LocalFileSystemVolume volume = volume(mount, depth);

        Path path = volume.buildPath("tenant-a", FILE_KEY, ".txt");

        Path expected = mount.resolve("tenant-a");
        if (depth > 0) {
            expected = expected.resolve("01");
        }
        if (depth > 1) {
            expected = expected.resolve("23");
        }
        if (depth > 2) {
            expected = expected.resolve("45");
        }
        assertThat(path).isEqualTo(expected.resolve(FILE_KEY + ".txt"));
    }

    @Test
    void writesAtomicallyAndCleansUpTheSameDirectoryTempFile() throws Exception {
        LocalFileSystemVolume volume = volume(temporaryDirectory.resolve("volume"), 2);
        Path target = volume.buildPath("tenant-a", FILE_KEY, ".txt");
        BlockingInputStream content = new BlockingInputStream("content".getBytes(StandardCharsets.UTF_8));

        try (var executor = java.util.concurrent.Executors.newVirtualThreadPerTaskExecutor()) {
            var written = executor.submit(() -> volume.write(target, content));
            assertThat(content.writeStarted.await(1, TimeUnit.SECONDS)).isTrue();

            assertThat(target).doesNotExist();
            assertThat(target.getParent()).isDirectory();

            content.allowCompletion.countDown();
            assertThat(written.get(1, TimeUnit.SECONDS)).isEqualTo(7);
        }

        assertThat(Files.readString(target)).isEqualTo("content");
        try (var files = Files.list(target.getParent())) {
            assertThat(files.map(path -> path.getFileName().toString())).containsExactly(FILE_KEY + ".txt");
        }
    }

    @Test
    void cleansTemporaryFileWhenTheInputStreamFails() throws IOException {
        LocalFileSystemVolume volume = volume(temporaryDirectory.resolve("volume"), 1);
        Path target = volume.buildPath("tenant-a", FILE_KEY, ".txt");

        assertThatThrownBy(() -> volume.write(target, new FailingInputStream()))
                .isInstanceOf(StorageVolumeUnavailableException.class);

        assertThat(target).doesNotExist();
        assertThat(target.getParent()).isDirectory();
        try (var files = Files.list(target.getParent())) {
            assertThat(files).isEmpty();
        }
    }

    @Test
    void cachesSuccessfulHealthAndCapacityProbe() throws IOException {
        Path mount = temporaryDirectory.resolve("volume");
        LocalFileSystemVolume volume = volume(mount, 0);

        assertThat(volume.healthy()).isTrue();
        long totalCapacity = volume.totalCapacity();
        long availableCapacity = volume.availableCapacity();
        Files.delete(mount);

        assertThat(volume.healthy()).isTrue();
        assertThat(volume.totalCapacity()).isEqualTo(totalCapacity);
        assertThat(volume.availableCapacity()).isEqualTo(availableCapacity);
    }

    @Test
    void readsMovesAndDeletesOnlyPathsBelongingToTheVolume() throws IOException {
        LocalFileSystemVolume volume = volume(temporaryDirectory.resolve("volume"), 0);
        Path source = volume.buildPath("tenant-a", FILE_KEY, ".txt");
        Path target = volume.buildPath("tenant-a", "fedcba9876543210fedcba9876543210", ".txt");
        volume.write(source, new ByteArrayInputStream("content".getBytes(StandardCharsets.UTF_8)));

        try (InputStream input = volume.read(source)) {
            assertThat(input.readAllBytes()).isEqualTo("content".getBytes(StandardCharsets.UTF_8));
        }
        volume.move(source, target);
        volume.delete(target);

        assertThat(source).doesNotExist();
        assertThat(target).doesNotExist();
        assertThatThrownBy(() -> volume.read(temporaryDirectory.resolve("outside")))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void rejectsExistingSymlinkThatEscapesTheVolume() throws IOException {
        LocalFileSystemVolume volume = volume(temporaryDirectory.resolve("volume"), 1);
        Path outside = temporaryDirectory.resolve("outside");
        Files.createDirectory(outside);
        Path tenant = volume.mountPath().resolve("tenant-a");
        try {
            Files.createSymbolicLink(tenant, outside);
        } catch (UnsupportedOperationException | IOException exception) {
            Assumptions.abort("Symbolic links are unavailable: " + exception.getMessage());
        }

        assertThatThrownBy(() -> volume.buildPath("tenant-a", FILE_KEY, ".txt"))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void generatesLowercase128BitFileKeys() {
        FileKeyGenerator generator = new FileKeyGenerator();

        String first = generator.next();
        String second = generator.next();

        assertThat(first).matches("[0-9a-f]{32}");
        assertThat(second).matches("[0-9a-f]{32}").isNotEqualTo(first);
    }

    private LocalFileSystemVolume volume(Path mount, int depth) {
        return new LocalFileSystemVolume(new VolumeConfiguration("volume-1", mount, depth, 1_024, true));
    }

    private static final class BlockingInputStream extends InputStream {

        private final byte[] bytes;
        private final CountDownLatch writeStarted = new CountDownLatch(1);
        private final CountDownLatch allowCompletion = new CountDownLatch(1);
        private boolean returned;

        private BlockingInputStream(byte[] bytes) {
            this.bytes = bytes;
        }

        @Override
        public int read(byte[] destination, int offset, int length) throws IOException {
            if (returned) {
                return -1;
            }
            writeStarted.countDown();
            try {
                if (!allowCompletion.await(1, TimeUnit.SECONDS)) {
                    throw new IOException("test input was not released");
                }
            } catch (InterruptedException exception) {
                Thread.currentThread().interrupt();
                throw new IOException(exception);
            }
            System.arraycopy(bytes, 0, destination, offset, bytes.length);
            returned = true;
            return bytes.length;
        }

        @Override
        public int read() {
            throw new UnsupportedOperationException();
        }
    }

    private static final class FailingInputStream extends InputStream {

        @Override
        public int read() throws IOException {
            throw new IOException("input failed");
        }
    }
}
