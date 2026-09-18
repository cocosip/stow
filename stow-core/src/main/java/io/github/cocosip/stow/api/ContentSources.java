package io.github.cocosip.stow.api;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Objects;
import java.util.OptionalLong;
import java.util.concurrent.atomic.AtomicBoolean;

public final class ContentSources {

    private ContentSources() {}

    public static ContentSource of(Path path) {
        Path normalized = Objects.requireNonNull(path, "path").toAbsolutePath().normalize();
        return new ContentSource() {
            @Override
            public InputStream openStream() {
                try {
                    return Files.newInputStream(normalized);
                } catch (IOException exception) {
                    throw new UncheckedIOException("Unable to open content path", exception);
                }
            }

            @Override
            public OptionalLong length() {
                try {
                    return OptionalLong.of(Files.size(normalized));
                } catch (IOException exception) {
                    throw new UncheckedIOException("Unable to read content length", exception);
                }
            }

            @Override
            public boolean repeatable() {
                return true;
            }
        };
    }

    public static ContentSource of(byte[] bytes) {
        byte[] snapshot = Objects.requireNonNull(bytes, "bytes").clone();
        return new ContentSource() {
            @Override
            public InputStream openStream() {
                return new ByteArrayInputStream(snapshot);
            }

            @Override
            public OptionalLong length() {
                return OptionalLong.of(snapshot.length);
            }

            @Override
            public boolean repeatable() {
                return true;
            }
        };
    }

    public static ContentSource singleUse(InputStream input, OptionalLong length) {
        Objects.requireNonNull(input, "input");
        Objects.requireNonNull(length, "length");
        if (length.isPresent() && length.getAsLong() < 0) {
            throw new IllegalArgumentException("length must not be negative");
        }
        AtomicBoolean opened = new AtomicBoolean();
        return new ContentSource() {
            @Override
            public InputStream openStream() {
                if (!opened.compareAndSet(false, true)) {
                    throw new IllegalStateException("Single-use content has already been opened");
                }
                return input;
            }

            @Override
            public OptionalLong length() {
                return length;
            }

            @Override
            public boolean repeatable() {
                return false;
            }
        };
    }
}
