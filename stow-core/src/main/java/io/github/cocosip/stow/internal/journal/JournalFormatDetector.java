package io.github.cocosip.stow.internal.journal;

import io.github.cocosip.stow.config.JournalFormat;
import io.github.cocosip.stow.exception.JournalCorruptionException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;

public final class JournalFormatDetector {

    public JournalFormat detect(byte[] bytes) {
        if (bytes == null || bytes.length == 0) {
            throw new JournalCorruptionException("Cannot detect journal format from empty input");
        }
        if (bytes.length >= JournalFrame.MAGIC.length
                && bytes[0] == 'S'
                && bytes[1] == 'T'
                && bytes[2] == 'W'
                && bytes[3] == '1') {
            return JournalFormat.BINARY_V1;
        }
        for (byte value : bytes) {
            if (value == ' ' || value == '\t' || value == '\r' || value == '\n') {
                continue;
            }
            if (value == '{') {
                return JournalFormat.JSON_LINES_V1;
            }
            break;
        }
        throw new JournalCorruptionException("Unknown journal format");
    }

    public JournalFormat detect(Path path) {
        try (InputStream input = Files.newInputStream(path)) {
            int first;
            do {
                first = input.read();
            } while (first == ' ' || first == '\t' || first == '\r' || first == '\n');
            if (first < 0) throw new JournalCorruptionException("Cannot detect journal format from empty input");
            if (first == '{') return JournalFormat.JSON_LINES_V1;
            if (first == 'S' && input.read() == 'T' && input.read() == 'W' && input.read() == '1') {
                return JournalFormat.BINARY_V1;
            }
            throw new JournalCorruptionException("Unknown journal format");
        } catch (IOException exception) {
            throw new JournalCorruptionException("Unable to read journal for format detection", exception);
        }
    }

    public static JournalFormat detectFormat(byte[] bytes) {
        return new JournalFormatDetector().detect(bytes);
    }

    public static JournalFormat detectFormat(Path path) {
        return new JournalFormatDetector().detect(path);
    }
}
