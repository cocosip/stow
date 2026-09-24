package io.github.cocosip.stow.internal.watcher;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.file.AtomicMoveNotSupportedException;
import java.nio.file.Files;
import java.nio.file.Path;
import org.junit.jupiter.api.Test;

class SourceFileRelocatorTest {

    @Test
    void rejectsDeleteWhenSourceFingerprintChanged() throws Exception {
        Path root = Files.createTempDirectory(Path.of("target"), "source-delete-");
        Path source = root.resolve("source.bin");
        Files.writeString(source, "one");
        SourceFingerprint expected = SourceFingerprint.capture(source);
        Files.writeString(source, "two");

        SourceFileRelocator.Status status = new SourceFileRelocator().deleteIfMatching(source, expected);

        assertThat(status).isEqualTo(SourceFileRelocator.Status.FINGERPRINT_MISMATCH);
        assertThat(Files.readString(source)).isEqualTo("two");
    }

    @Test
    void treatsMissingSourceAsCompletedWithoutCreatingDestination() throws Exception {
        Path root = Files.createTempDirectory(Path.of("target"), "source-missing-");
        Path source = root.resolve("source.bin");
        Files.writeString(source, "content");
        SourceFingerprint expected = SourceFingerprint.capture(source);
        Files.delete(source);

        SourceFileRelocator.Result result =
                new SourceFileRelocator().moveIfMatching(source, root.resolve("moved.bin"), expected);

        assertThat(result.status()).isEqualTo(SourceFileRelocator.Status.SOURCE_MISSING);
        assertThat(Files.exists(root.resolve("moved.bin"))).isFalse();
    }

    @Test
    void atomicallyMovesMatchingSource() throws Exception {
        Path root = Files.createTempDirectory(Path.of("target"), "source-move-");
        Path source = root.resolve("source.bin");
        Path target = root.resolve("archive").resolve("source.bin");
        Files.writeString(source, "content");

        SourceFileRelocator.Result result =
                new SourceFileRelocator().moveIfMatching(source, target, SourceFingerprint.capture(source));

        assertThat(result.status()).isEqualTo(SourceFileRelocator.Status.COMPLETED);
        assertThat(result.destination()).isEqualTo(target.toAbsolutePath().normalize());
        assertThat(Files.exists(source)).isFalse();
        assertThat(Files.readString(target)).isEqualTo("content");
    }

    @Test
    void copiesFlushesVerifiesAndDeletesWhenAtomicMoveIsUnavailable() throws Exception {
        Path root = Files.createTempDirectory(Path.of("target"), "source-copy-");
        Path source = root.resolve("source.bin");
        Path target = root.resolve("archive").resolve("source.bin");
        Files.writeString(source, "content");
        SourceFileRelocator relocator = new SourceFileRelocator((from, to) -> {
            throw new AtomicMoveNotSupportedException(from.toString(), to.toString(), "forced fallback");
        });

        SourceFileRelocator.Result result = relocator.moveIfMatching(source, target, SourceFingerprint.capture(source));

        assertThat(result.status()).isEqualTo(SourceFileRelocator.Status.COMPLETED);
        assertThat(Files.exists(source)).isFalse();
        assertThat(Files.readString(target)).isEqualTo("content");
        try (var children = Files.list(target.getParent())) {
            assertThat(children.map(path -> path.getFileName().toString()))
                    .containsExactly(target.getFileName().toString());
        }
    }

    @Test
    void usesDeterministicSuffixForDifferentCollisionAndReusesIdenticalTarget() throws Exception {
        Path root = Files.createTempDirectory(Path.of("target"), "source-collision-");
        Path target = root.resolve("archive").resolve("source.bin");
        Files.createDirectories(target.getParent());
        Files.writeString(target, "different");
        Path source = root.resolve("source.bin");
        Files.writeString(source, "content");
        SourceFileRelocator relocator = new SourceFileRelocator();

        SourceFileRelocator.Result moved = relocator.moveIfMatching(source, target, SourceFingerprint.capture(source));

        assertThat(moved.destination())
                .isEqualTo(
                        target.resolveSibling("source.1.bin").toAbsolutePath().normalize());
        assertThat(Files.readString(moved.destination())).isEqualTo("content");
        Path suffixDuplicate = root.resolve("suffix-duplicate.bin");
        Files.writeString(suffixDuplicate, "content");
        SourceFileRelocator.Result suffixReused =
                relocator.moveIfMatching(suffixDuplicate, target, SourceFingerprint.capture(suffixDuplicate));
        assertThat(suffixReused.destination())
                .isEqualTo(
                        target.resolveSibling("source.1.bin").toAbsolutePath().normalize());
        assertThat(Files.exists(suffixDuplicate)).isFalse();
        Path duplicate = root.resolve("duplicate.bin");
        Files.writeString(duplicate, "different");
        SourceFileRelocator.Result reused =
                relocator.moveIfMatching(duplicate, target, SourceFingerprint.capture(duplicate));
        assertThat(reused.destination()).isEqualTo(target.toAbsolutePath().normalize());
        assertThat(Files.exists(duplicate)).isFalse();
    }
}
