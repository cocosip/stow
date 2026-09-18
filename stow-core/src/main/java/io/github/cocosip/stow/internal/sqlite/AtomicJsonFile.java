package io.github.cocosip.stow.internal.sqlite;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.WritableByteChannel;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;
import java.util.regex.Pattern;

public final class AtomicJsonFile<T> {

    private final Path target;
    private final Path parent;
    private final String targetFileName;
    private final ObjectMapper objectMapper;
    private final Class<T> documentType;
    private final BeforeMove beforeMove;
    private final Pattern temporaryFileName;

    public AtomicJsonFile(Path target, ObjectMapper objectMapper, Class<T> documentType) {
        this(target, objectMapper, documentType, (temporary, destination) -> {});
    }

    AtomicJsonFile(Path target, ObjectMapper objectMapper, Class<T> documentType, BeforeMove beforeMove) {
        this.target = Objects.requireNonNull(target, "target").toAbsolutePath().normalize();
        parent = this.target.getParent();
        Path fileName = this.target.getFileName();
        if (parent == null || fileName == null) {
            throw new IllegalArgumentException("Atomic JSON target must name a file in a parent directory");
        }
        targetFileName = fileName.toString();
        this.objectMapper = Objects.requireNonNull(objectMapper, "objectMapper");
        this.documentType = Objects.requireNonNull(documentType, "documentType");
        this.beforeMove = Objects.requireNonNull(beforeMove, "beforeMove");
        temporaryFileName = Pattern.compile("^\\."
                + Pattern.quote(targetFileName)
                + "\\.[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}\\.tmp$");
    }

    public Optional<T> read() throws IOException {
        if (!Files.exists(target)) {
            return Optional.empty();
        }
        return Optional.of(objectMapper.readValue(target.toFile(), documentType));
    }

    public void deleteStaleTemporaryFiles() throws IOException {
        if (!Files.isDirectory(parent, LinkOption.NOFOLLOW_LINKS)) {
            return;
        }
        try (DirectoryStream<Path> files = Files.newDirectoryStream(parent)) {
            for (Path file : files) {
                Path fileName = file.getFileName();
                if (fileName != null
                        && temporaryFileName.matcher(fileName.toString()).matches()
                        && Files.isRegularFile(file, LinkOption.NOFOLLOW_LINKS)) {
                    Files.delete(file);
                }
            }
        }
    }

    public void write(T document) throws IOException {
        Objects.requireNonNull(document, "document");
        Files.createDirectories(parent);
        Path temporary = parent.resolve(
                "." + targetFileName + "." + UUID.randomUUID().toString().toLowerCase() + ".tmp");
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
            if (channel.write(buffer) <= 0) {
                throw new IOException("Unable to write JSON state file because the channel made no progress");
            }
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
