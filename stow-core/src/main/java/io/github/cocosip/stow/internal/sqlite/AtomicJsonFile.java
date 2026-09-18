package io.github.cocosip.stow.internal.sqlite;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.WritableByteChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;

public final class AtomicJsonFile<T> {

    private final Path target;
    private final ObjectMapper objectMapper;
    private final Class<T> documentType;
    private final BeforeMove beforeMove;

    public AtomicJsonFile(Path target, ObjectMapper objectMapper, Class<T> documentType) {
        this(target, objectMapper, documentType, (temporary, destination) -> {});
    }

    AtomicJsonFile(Path target, ObjectMapper objectMapper, Class<T> documentType, BeforeMove beforeMove) {
        this.target = Objects.requireNonNull(target, "target").toAbsolutePath().normalize();
        this.objectMapper = Objects.requireNonNull(objectMapper, "objectMapper");
        this.documentType = Objects.requireNonNull(documentType, "documentType");
        this.beforeMove = Objects.requireNonNull(beforeMove, "beforeMove");
    }

    public Optional<T> read() throws IOException {
        if (!Files.exists(target)) {
            return Optional.empty();
        }
        return Optional.of(objectMapper.readValue(target.toFile(), documentType));
    }

    public void write(T document) throws IOException {
        Objects.requireNonNull(document, "document");
        Path parent = target.getParent();
        if (parent == null) {
            throw new IOException("Atomic JSON target must have a parent directory");
        }
        Files.createDirectories(parent);
        Path temporary = parent.resolve(
                "." + target.getFileName() + "." + UUID.randomUUID().toString().toLowerCase() + ".tmp");
        try {
            byte[] json = objectMapper.writeValueAsBytes(document);
            try (FileChannel channel =
                    FileChannel.open(temporary, StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE)) {
                writeFully(channel, ByteBuffer.wrap(json));
                writeFully(channel, ByteBuffer.wrap(new byte[] {'\n'}));
                channel.force(true);
            }
            beforeMove.run(temporary, target);
            Files.move(temporary, target, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
            forceDirectory(parent);
        } finally {
            Files.deleteIfExists(temporary);
        }
    }

    static void writeFully(WritableByteChannel channel, ByteBuffer buffer) throws IOException {
        while (buffer.hasRemaining()) {
            channel.write(buffer);
        }
    }

    private static void forceDirectory(Path directory) {
        try (FileChannel channel = FileChannel.open(directory, StandardOpenOption.READ)) {
            channel.force(true);
        } catch (IOException | UnsupportedOperationException ignored) {
            // Directory fsync is not available on every supported platform.
        }
    }

    @FunctionalInterface
    interface BeforeMove {
        void run(Path temporary, Path target) throws IOException;
    }
}
