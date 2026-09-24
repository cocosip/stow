package io.github.cocosip.stow.internal.watcher;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.FileTime;
import org.junit.jupiter.api.Test;

class SourceFingerprintTest {

    @Test
    void detectsChangedContentWhenPathSizeAndModifiedTimeAreReused() throws Exception {
        Path root = Files.createTempDirectory(Path.of("target"), "source-fingerprint-");
        Path source = root.resolve("source.bin");
        FileTime modified = FileTime.fromMillis(1_700_000_000_000L);
        byte[] original = new byte[196_608];
        original[0] = 1;
        original[original.length - 1] = 2;
        Files.write(source, original);
        Files.setLastModifiedTime(source, modified);
        SourceFingerprint before = SourceFingerprint.capture(source);

        byte[] replacement = original.clone();
        replacement[0] = 3;
        replacement[replacement.length - 1] = 4;
        Files.write(source, replacement);
        Files.setLastModifiedTime(source, modified);
        SourceFingerprint after = SourceFingerprint.capture(source);

        assertThat(after.path()).isEqualTo(before.path());
        assertThat(after.size()).isEqualTo(before.size());
        assertThat(after.lastModifiedMillis()).isEqualTo(before.lastModifiedMillis());
        assertThat(after).isNotEqualTo(before);
    }
}
