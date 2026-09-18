package io.github.cocosip.stow.internal.filesystem;

import io.github.cocosip.stow.config.VolumeConfiguration;
import io.github.cocosip.stow.exception.StorageVolumeUnavailableException;
import io.github.cocosip.stow.exception.StoredFileNotFoundException;
import io.github.cocosip.stow.spi.StorageVolume;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;

final class LocalFileSystemVolume implements StorageVolume {

    private static final long PROBE_CACHE_NANOS = 250_000_000L;

    private final String id;
    private final Path mountPath;
    private final int shardingDepth;
    private final int bufferSize;
    private final boolean forceFlushAfterWrite;
    private final AtomicReference<Probe> cachedProbe = new AtomicReference<>();

    LocalFileSystemVolume(VolumeConfiguration configuration) {
        Objects.requireNonNull(configuration, "configuration");
        id = PathPolicy.identifier("volume.id", configuration.id());
        mountPath = configuration.mountPath().toAbsolutePath().normalize();
        shardingDepth = configuration.shardingDepth();
        bufferSize = configuration.bufferSize();
        forceFlushAfterWrite = configuration.forceFlushAfterWrite();
        try {
            ensureMountExists();
        } catch (IOException exception) {
            throw unavailable("Unable to initialize storage volume", exception);
        }
    }

    @Override
    public String id() {
        return id;
    }

    @Override
    public Path mountPath() {
        return mountPath;
    }

    @Override
    public boolean healthy() {
        return probe().healthy();
    }

    @Override
    public long totalCapacity() {
        return probe().totalCapacity();
    }

    @Override
    public long availableCapacity() {
        return probe().availableCapacity();
    }

    @Override
    public Path buildPath(String tenantId, String fileKey, String extension) {
        String tenant = PathPolicy.identifier("tenantId", tenantId);
        String key = PathPolicy.fileKey(fileKey);
        String safeExtension = PathPolicy.extension(extension);
        Path result = mountPath.resolve(tenant);
        for (int level = 0; level < shardingDepth; level++) {
            result = result.resolve(key.substring(level * 2, level * 2 + 2));
        }
        result = result.resolve(key + safeExtension).toAbsolutePath().normalize();
        try {
            validateTarget(result, false);
            return result;
        } catch (IOException exception) {
            throw unavailable("Unable to validate storage path", exception);
        }
    }

    @Override
    public long write(Path target, InputStream content) {
        Objects.requireNonNull(content, "content");
        Path temporary = null;
        try {
            Path destination = validateTarget(target, true);
            Path parent = Objects.requireNonNull(destination.getParent(), "storage target parent");
            temporary = parent.resolve("."
                    + destination.getFileName()
                    + "."
                    + UUID.randomUUID().toString().toLowerCase()
                    + ".tmp");
            long length = writeTemporary(temporary, content);
            Files.move(temporary, destination, StandardCopyOption.ATOMIC_MOVE);
            return length;
        } catch (IOException exception) {
            throw unavailable("Unable to write file to storage volume", exception);
        } finally {
            if (temporary != null) {
                try {
                    Files.deleteIfExists(temporary);
                } catch (IOException ignored) {
                    // A startup cleanup pass can safely remove an unpublished same-directory temp file.
                }
            }
        }
    }

    @Override
    public InputStream read(Path path) {
        try {
            Path target = validateTarget(path, false);
            if (!Files.isRegularFile(target, LinkOption.NOFOLLOW_LINKS)) {
                throw new StoredFileNotFoundException("Stored file does not exist");
            }
            return Files.newInputStream(target, StandardOpenOption.READ);
        } catch (StoredFileNotFoundException exception) {
            throw exception;
        } catch (IOException exception) {
            throw unavailable("Unable to read file from storage volume", exception);
        }
    }

    @Override
    public void delete(Path path) {
        try {
            Files.deleteIfExists(validateTarget(path, false));
        } catch (IOException exception) {
            throw unavailable("Unable to delete file from storage volume", exception);
        }
    }

    @Override
    public void move(Path source, Path target) {
        try {
            Path safeSource = validateTarget(source, false);
            Path safeTarget = validateTarget(target, true);
            Files.move(safeSource, safeTarget, StandardCopyOption.ATOMIC_MOVE);
        } catch (IOException exception) {
            throw unavailable("Unable to move file within storage volume", exception);
        }
    }

    @Override
    public void close() {
        // This implementation owns no open resources.
    }

    private long writeTemporary(Path temporary, InputStream content) throws IOException {
        long length = 0;
        ByteBuffer buffer = ByteBuffer.allocate(bufferSize);
        try (FileChannel channel =
                FileChannel.open(temporary, StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE)) {
            while (true) {
                int read = content.read(buffer.array(), 0, buffer.capacity());
                if (read < 0) {
                    break;
                }
                if (read == 0) {
                    continue;
                }
                buffer.limit(read);
                buffer.position(0);
                while (buffer.hasRemaining()) {
                    channel.write(buffer);
                }
                length += read;
                buffer.clear();
            }
            if (forceFlushAfterWrite) {
                channel.force(true);
            }
        }
        return length;
    }

    private Path validateTarget(Path path, boolean createParentDirectories) throws IOException {
        Objects.requireNonNull(path, "path");
        Path target = path.toAbsolutePath().normalize();
        if (!target.startsWith(mountPath) || target.equals(mountPath) || target.getFileName() == null) {
            throw new IllegalArgumentException("Storage path must remain within the volume mount path");
        }
        ensureMountExists();
        ensureSafeDirectories(target.getParent(), createParentDirectories);
        if (Files.exists(target, LinkOption.NOFOLLOW_LINKS) && isLink(target)) {
            throw new IllegalArgumentException("Storage path must not use symbolic links or reparse points");
        }
        return target;
    }

    private void ensureMountExists() throws IOException {
        if (!Files.exists(mountPath, LinkOption.NOFOLLOW_LINKS)) {
            Files.createDirectories(mountPath);
        }
        if (!Files.isDirectory(mountPath, LinkOption.NOFOLLOW_LINKS) || isLink(mountPath)) {
            throw new IOException("Storage volume mount path must be a real directory");
        }
    }

    private void ensureSafeDirectories(Path directory, boolean createMissing) throws IOException {
        if (directory == null || !directory.startsWith(mountPath)) {
            throw new IllegalArgumentException("Storage directory must remain within the volume mount path");
        }
        Path current = mountPath;
        for (Path segment : mountPath.relativize(directory)) {
            current = current.resolve(segment);
            if (!Files.exists(current, LinkOption.NOFOLLOW_LINKS)) {
                if (!createMissing) {
                    continue;
                }
                try {
                    Files.createDirectory(current);
                } catch (FileAlreadyExistsException ignored) {
                    // Another writer created the directory; validate it below.
                }
            }
            if (!Files.isDirectory(current, LinkOption.NOFOLLOW_LINKS) || isLink(current)) {
                throw new IOException("Storage directory must not use symbolic links or reparse points");
            }
        }
    }

    private Probe probe() {
        long now = System.nanoTime();
        Probe existing = cachedProbe.get();
        if (existing != null && now - existing.checkedAtNanos() < PROBE_CACHE_NANOS) {
            return existing;
        }
        Probe fresh;
        try {
            ensureMountExists();
            var store = Files.getFileStore(mountPath);
            fresh = new Probe(true, store.getTotalSpace(), store.getUsableSpace(), now);
        } catch (IOException exception) {
            fresh = new Probe(false, 0, 0, now);
        }
        cachedProbe.set(fresh);
        return fresh;
    }

    private static boolean isLink(Path path) throws IOException {
        BasicFileAttributes attributes =
                Files.readAttributes(path, BasicFileAttributes.class, LinkOption.NOFOLLOW_LINKS);
        return attributes.isSymbolicLink() || attributes.isOther();
    }

    private static StorageVolumeUnavailableException unavailable(String message, IOException cause) {
        return new StorageVolumeUnavailableException(message, cause);
    }

    private record Probe(boolean healthy, long totalCapacity, long availableCapacity, long checkedAtNanos) {}
}
