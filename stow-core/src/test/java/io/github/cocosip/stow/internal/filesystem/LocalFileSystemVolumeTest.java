package io.github.cocosip.stow.internal.filesystem;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.github.cocosip.stow.config.VolumeConfiguration;
import io.github.cocosip.stow.exception.StorageVolumeUnavailableException;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.AccessDeniedException;
import java.nio.file.DirectoryStream;
import java.nio.file.FileSystemException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.SecureDirectoryStream;
import java.nio.file.attribute.BasicFileAttributes;
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
    void removesOnlyStorageTemporaryFilesWhenTheVolumeStarts() throws IOException {
        Path mount = temporaryDirectory.resolve("volume");
        Path storageDirectory = mount.resolve("tenant-a").resolve("01");
        Files.createDirectories(storageDirectory);
        Path staleTemporary =
                storageDirectory.resolve("." + FILE_KEY + ".txt.85b55a44-c254-4d86-b09f-69777d73f69e.tmp");
        Path matchingDirectory =
                storageDirectory.resolve("." + FILE_KEY + ".txt.98cc779b-d122-4e8d-a793-b313badcb083.tmp");
        Path unrelated = storageDirectory.resolve(".keep.tmp");
        Files.writeString(staleTemporary, "partial");
        Files.createDirectory(matchingDirectory);
        Files.writeString(unrelated, "keep");

        volume(mount, 1);

        assertThat(staleTemporary).doesNotExist();
        assertThat(matchingDirectory).isDirectory();
        assertThat(unrelated).exists();
    }

    @Test
    void pinsTheValidatedParentDirectoryWhilePublishingAWrite() throws IOException {
        Path mount = temporaryDirectory.resolve("volume");
        Path outside = temporaryDirectory.resolve("outside");
        Path probeLink = temporaryDirectory.resolve("probe-link");
        Files.createDirectory(outside);
        createSymbolicLinkOrSkip(probeLink, outside);
        Files.delete(probeLink);

        LocalFileSystemVolume volume = volume(mount, 0);
        Path target = volume.buildPath("tenant-a", FILE_KEY, ".txt");
        Path displacedParent = temporaryDirectory.resolve("displaced-tenant");
        ReplacingParentInputStream content = new ReplacingParentInputStream(
                target, displacedParent, outside, "content".getBytes(StandardCharsets.UTF_8));

        if (supportsSecureDirectoryStreams(mount)) {
            assertThat(volume.write(target, content)).isEqualTo(7);
            assertThat(displacedParent.resolve(target.getFileName())).hasContent("content");
        } else {
            assertThatThrownBy(() -> volume.write(target, content))
                    .isInstanceOf(StorageVolumeUnavailableException.class);
            assertThat(displacedParent.resolve(target.getFileName())).doesNotExist();
        }
        assertThat(outside.resolve(target.getFileName())).doesNotExist();
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
        createSymbolicLinkOrSkip(tenant, outside);

        assertThatThrownBy(() -> volume.buildPath("tenant-a", FILE_KEY, ".txt"))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void rejectsMountPathWithASymbolicLinkAncestor() throws IOException {
        Path outside = temporaryDirectory.resolve("outside");
        Path linkedRoot = temporaryDirectory.resolve("linked-root");
        Files.createDirectory(outside);
        createSymbolicLinkOrSkip(linkedRoot, outside);

        assertThatThrownBy(() -> volume(linkedRoot.resolve("volume"), 0))
                .isInstanceOf(StorageVolumeUnavailableException.class);
        assertThat(outside.resolve("volume")).doesNotExist();
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

    private static void createSymbolicLinkOrSkip(Path link, Path target) throws IOException {
        try {
            Files.createSymbolicLink(link, target.toAbsolutePath());
        } catch (UnsupportedOperationException exception) {
            Assumptions.abort("Symbolic links are unsupported: " + exception.getMessage());
        } catch (AccessDeniedException exception) {
            if (isWindows()) {
                Assumptions.abort("Symbolic link creation is not permitted: " + exception.getMessage());
            }
            throw exception;
        } catch (FileSystemException exception) {
            if (isWindows() && isMissingWindowsSymbolicLinkPrivilege(exception)) {
                Assumptions.abort("Symbolic link creation is not permitted: " + exception.getMessage());
            }
            throw exception;
        }
        BasicFileAttributes attributes =
                Files.readAttributes(link, BasicFileAttributes.class, java.nio.file.LinkOption.NOFOLLOW_LINKS);
        if (!attributes.isSymbolicLink()) {
            throw new IOException("Symbolic link creation did not create a symbolic link");
        }
    }

    private static boolean isWindows() {
        return System.getProperty("os.name").startsWith("Windows");
    }

    private static boolean supportsSecureDirectoryStreams(Path directory) throws IOException {
        try (DirectoryStream<Path> stream = Files.newDirectoryStream(directory)) {
            return stream instanceof SecureDirectoryStream<?>;
        }
    }

    private static boolean isMissingWindowsSymbolicLinkPrivilege(FileSystemException exception) {
        String reason = exception.getReason();
        return reason != null
                && (reason.contains("privilege")
                        || reason.contains("\u7279\u6743")
                        || reason.contains("A required privilege is not held"));
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

    private static final class ReplacingParentInputStream extends InputStream {

        private final Path target;
        private final Path displacedParent;
        private final Path outside;
        private final byte[] content;
        private boolean returned;

        private ReplacingParentInputStream(Path target, Path displacedParent, Path outside, byte[] content) {
            this.target = target;
            this.displacedParent = displacedParent;
            this.outside = outside;
            this.content = content;
        }

        @Override
        public int read(byte[] destination, int offset, int length) throws IOException {
            if (returned) {
                return -1;
            }
            Path parent = target.getParent();
            Files.move(parent, displacedParent);
            Files.createSymbolicLink(parent, outside);
            Path temporaryName;
            try (var files = Files.list(displacedParent)) {
                temporaryName = files.findFirst().orElseThrow().getFileName();
            }
            Files.writeString(outside.resolve(temporaryName), "attacker");
            System.arraycopy(content, 0, destination, offset, content.length);
            returned = true;
            return content.length;
        }

        @Override
        public int read() {
            throw new UnsupportedOperationException();
        }
    }
}
