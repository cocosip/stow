package io.github.cocosip.stow.internal.filesystem;

import io.github.cocosip.stow.config.VolumeConfiguration;
import io.github.cocosip.stow.exception.StorageVolumeUnavailableException;
import io.github.cocosip.stow.exception.StoredFileNotFoundException;
import io.github.cocosip.stow.spi.StorageVolume;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.FileChannel;
import java.nio.channels.SeekableByteChannel;
import java.nio.file.DirectoryStream;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.NoSuchFileException;
import java.nio.file.OpenOption;
import java.nio.file.Path;
import java.nio.file.SecureDirectoryStream;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.nio.file.attribute.BasicFileAttributeView;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;
import java.util.regex.Pattern;

final class LocalFileSystemVolume implements StorageVolume {

    private static final long PROBE_CACHE_NANOS = 250_000_000L;
    private static final Pattern TEMPORARY_FILE_NAME = Pattern.compile("^\\.[0-9a-f]{32}(?:\\.[A-Za-z0-9._-]{1,31})?"
            + "\\.[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}\\.tmp$");

    private final String id;
    private final Path mountPath;
    private final Path realMountPath;
    private final Object mountFileKey;
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
            MountIdentity mountIdentity = initializeMount();
            realMountPath = mountIdentity.realPath();
            mountFileKey = mountIdentity.fileKey();
            deleteStaleTemporaryFiles();
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
        } catch (UnsafeStoragePathException exception) {
            throw new IllegalArgumentException(exception.getMessage(), exception);
        } catch (IOException exception) {
            throw unavailable("Unable to validate storage path", exception);
        }
    }

    @Override
    public long write(Path target, InputStream content) {
        Objects.requireNonNull(content, "content");
        try {
            Path destination = validateTarget(target, true);
            try (SecureParent parent = openSecureParent(destination)) {
                if (parent != null) {
                    return writeSecure(parent, content);
                }
            }
            return writeFallback(validateTarget(destination, true), content);
        } catch (IOException exception) {
            throw unavailable("Unable to write file to storage volume", exception);
        }
    }

    @Override
    public InputStream read(Path path) {
        try {
            Path target = validateTarget(path, false);
            try (SecureParent parent = openSecureParent(target)) {
                if (parent != null) {
                    BasicFileAttributes attributes = parent.readAttributes();
                    if (!attributes.isRegularFile()) {
                        throw new StoredFileNotFoundException("Stored file does not exist");
                    }
                    SeekableByteChannel channel = parent.directory()
                            .newByteChannel(
                                    parent.fileName(),
                                    Set.<OpenOption>of(StandardOpenOption.READ, LinkOption.NOFOLLOW_LINKS));
                    return Channels.newInputStream(channel);
                }
            }
            target = validateTarget(target, false);
            if (!Files.isRegularFile(target, LinkOption.NOFOLLOW_LINKS)) {
                throw new StoredFileNotFoundException("Stored file does not exist");
            }
            InputStream input = Files.newInputStream(target, StandardOpenOption.READ, LinkOption.NOFOLLOW_LINKS);
            try {
                validateTarget(target, false);
                return input;
            } catch (IOException | RuntimeException failure) {
                try {
                    input.close();
                } catch (IOException closeFailure) {
                    failure.addSuppressed(closeFailure);
                }
                throw failure;
            }
        } catch (StoredFileNotFoundException exception) {
            throw exception;
        } catch (NoSuchFileException exception) {
            throw new StoredFileNotFoundException("Stored file does not exist");
        } catch (IOException exception) {
            throw unavailable("Unable to read file from storage volume", exception);
        }
    }

    @Override
    public void delete(Path path) {
        try {
            Path target = validateTarget(path, false);
            try (SecureParent parent = openSecureParent(target)) {
                if (parent != null) {
                    try {
                        parent.directory().deleteFile(parent.fileName());
                    } catch (NoSuchFileException ignored) {
                        // Delete is idempotent.
                    }
                    return;
                }
            }
            Files.deleteIfExists(validateTarget(target, false));
        } catch (NoSuchFileException ignored) {
            // Delete is idempotent when its parent directory is already absent.
        } catch (IOException exception) {
            throw unavailable("Unable to delete file from storage volume", exception);
        }
    }

    @Override
    public void move(Path source, Path target) {
        try {
            Path safeSource = validateTarget(source, false);
            Path safeTarget = validateTarget(target, true);
            try (SecureParent sourceParent = openSecureParent(safeSource);
                    SecureParent targetParent = openSecureParent(safeTarget)) {
                if (sourceParent != null && targetParent != null) {
                    sourceParent
                            .directory()
                            .move(sourceParent.fileName(), targetParent.directory(), targetParent.fileName());
                    return;
                }
            }
            safeSource = validateTarget(safeSource, false);
            safeTarget = validateTarget(safeTarget, true);
            Files.move(safeSource, safeTarget, StandardCopyOption.ATOMIC_MOVE);
        } catch (IOException exception) {
            throw unavailable("Unable to move file within storage volume", exception);
        }
    }

    @Override
    public void close() {
        // This implementation owns no open resources.
    }

    private long writeSecure(SecureParent parent, InputStream content) throws IOException {
        Path temporaryName = temporaryName(parent.fileName());
        try {
            long length;
            try (SeekableByteChannel channel = parent.directory()
                    .newByteChannel(
                            temporaryName,
                            Set.<OpenOption>of(
                                    StandardOpenOption.CREATE_NEW,
                                    StandardOpenOption.WRITE,
                                    LinkOption.NOFOLLOW_LINKS))) {
                length = writeTemporary(channel, content);
            }
            parent.directory().move(temporaryName, parent.directory(), parent.fileName());
            return length;
        } finally {
            try {
                parent.directory().deleteFile(temporaryName);
            } catch (IOException ignored) {
                // The atomic move published the file, or startup cleanup will remove the temporary.
            }
        }
    }

    private long writeFallback(Path destination, InputStream content) throws IOException {
        Path parent = Objects.requireNonNull(destination.getParent(), "storage target parent");
        Path fileName = Objects.requireNonNull(destination.getFileName(), "storage target file name");
        Path temporary = parent.resolve(temporaryName(fileName));
        try {
            long length;
            try (FileChannel channel = FileChannel.open(
                    temporary, StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE, LinkOption.NOFOLLOW_LINKS)) {
                validateTarget(temporary, false);
                length = writeTemporary(channel, content);
            }
            validateTarget(temporary, false);
            validateTarget(destination, false);
            Files.move(temporary, destination, StandardCopyOption.ATOMIC_MOVE);
            return length;
        } finally {
            try {
                Files.deleteIfExists(validateTarget(temporary, false));
            } catch (IOException | IllegalArgumentException ignored) {
                // Startup cleanup handles unpublished files that remain under the validated mount.
            }
        }
    }

    private long writeTemporary(SeekableByteChannel channel, InputStream content) throws IOException {
        long length = 0;
        ByteBuffer buffer = ByteBuffer.allocate(bufferSize);
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
            if (!(channel instanceof FileChannel fileChannel)) {
                throw new IOException("Storage provider does not support forcing file contents");
            }
            fileChannel.force(true);
        }
        return length;
    }

    private Path validateTarget(Path path, boolean createParentDirectories) throws IOException {
        Objects.requireNonNull(path, "path");
        Path target = path.toAbsolutePath().normalize();
        if (!target.startsWith(mountPath) || target.equals(mountPath) || target.getFileName() == null) {
            throw new IllegalArgumentException("Storage path must remain within the volume mount path");
        }
        ensureMountValid();
        ensureSafeDirectories(target.getParent(), createParentDirectories);
        if (Files.exists(target, LinkOption.NOFOLLOW_LINKS) && isLink(target)) {
            throw new IllegalArgumentException("Storage path must not use symbolic links or reparse points");
        }
        return target;
    }

    private MountIdentity initializeMount() throws IOException {
        Path current = mountPath.getRoot();
        if (current == null) {
            throw new IOException("Storage volume mount path must be absolute");
        }
        for (Path component : mountPath) {
            current = current.resolve(component);
            if (!Files.exists(current, LinkOption.NOFOLLOW_LINKS)) {
                try {
                    Files.createDirectory(current);
                } catch (FileAlreadyExistsException ignored) {
                    // Another initializer created the directory; validate it below.
                }
            }
            validateNormalDirectory(current, "Storage volume mount path");
        }
        BasicFileAttributes attributes =
                Files.readAttributes(current, BasicFileAttributes.class, LinkOption.NOFOLLOW_LINKS);
        return new MountIdentity(current, attributes.fileKey());
    }

    private void ensureMountValid() throws IOException {
        Path current = mountPath.getRoot();
        if (current == null) {
            throw new IOException("Storage volume mount path must be absolute");
        }
        for (Path component : mountPath) {
            current = current.resolve(component);
            validateNormalDirectory(current, "Storage volume mount path");
        }
        BasicFileAttributes attributes =
                Files.readAttributes(current, BasicFileAttributes.class, LinkOption.NOFOLLOW_LINKS);
        if (!samePath(realMountPath, current) || (mountFileKey != null && !mountFileKey.equals(attributes.fileKey()))) {
            throw new IOException("Storage volume mount path identity changed");
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
            if (isLink(current)) {
                throw new UnsafeStoragePathException("Storage directory must not use symbolic links or reparse points");
            }
            if (!Files.isDirectory(current, LinkOption.NOFOLLOW_LINKS)) {
                throw new IOException("Storage directory must be a directory");
            }
        }
    }

    private SecureParent openSecureParent(Path target) throws IOException {
        DirectoryStream<Path> rootStream = Files.newDirectoryStream(realMountPath);
        if (!(rootStream instanceof SecureDirectoryStream<?>)) {
            rootStream.close();
            return null;
        }
        @SuppressWarnings("unchecked")
        SecureDirectoryStream<Path> secureRoot = (SecureDirectoryStream<Path>) rootStream;
        SecureDirectoryStream<Path> current = secureRoot;
        try {
            verifyOpenedMount(current);
            Path parent = Objects.requireNonNull(target.getParent(), "storage target parent");
            for (Path segment : mountPath.relativize(parent)) {
                SecureDirectoryStream<Path> next = current.newDirectoryStream(segment, LinkOption.NOFOLLOW_LINKS);
                current.close();
                current = next;
            }
            return new SecureParent(current, target.getFileName());
        } catch (IOException | RuntimeException | Error failure) {
            try {
                current.close();
            } catch (Throwable closeFailure) {
                failure.addSuppressed(closeFailure);
            }
            throw failure;
        }
    }

    private void verifyOpenedMount(SecureDirectoryStream<Path> directory) throws IOException {
        BasicFileAttributeView view = directory.getFileAttributeView(BasicFileAttributeView.class);
        BasicFileAttributes attributes = view.readAttributes();
        if (!attributes.isDirectory()
                || attributes.isSymbolicLink()
                || attributes.isOther()
                || (mountFileKey != null && !mountFileKey.equals(attributes.fileKey()))) {
            throw new IOException("Storage volume mount path identity changed");
        }
    }

    private void deleteStaleTemporaryFiles() throws IOException {
        try (DirectoryStream<Path> rootStream = Files.newDirectoryStream(realMountPath)) {
            if (rootStream instanceof SecureDirectoryStream<?>) {
                @SuppressWarnings("unchecked")
                SecureDirectoryStream<Path> secureRoot = (SecureDirectoryStream<Path>) rootStream;
                verifyOpenedMount(secureRoot);
                deleteStaleTemporaryFiles(secureRoot);
                return;
            }
        }
        Files.walkFileTree(realMountPath, new SimpleFileVisitor<>() {
            @Override
            public FileVisitResult visitFile(Path file, BasicFileAttributes attributes) throws IOException {
                if (attributes.isRegularFile() && isStorageTemporaryFile(file.getFileName())) {
                    Files.delete(file);
                }
                return FileVisitResult.CONTINUE;
            }
        });
    }

    private void deleteStaleTemporaryFiles(SecureDirectoryStream<Path> directory) throws IOException {
        for (Path entry : directory) {
            Path name = entry.getFileName();
            BasicFileAttributeView view =
                    directory.getFileAttributeView(name, BasicFileAttributeView.class, LinkOption.NOFOLLOW_LINKS);
            BasicFileAttributes attributes = view.readAttributes();
            if (attributes.isDirectory() && !attributes.isSymbolicLink() && !attributes.isOther()) {
                try (SecureDirectoryStream<Path> child =
                        directory.newDirectoryStream(name, LinkOption.NOFOLLOW_LINKS)) {
                    deleteStaleTemporaryFiles(child);
                }
            } else if (attributes.isRegularFile() && isStorageTemporaryFile(name)) {
                directory.deleteFile(name);
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
            ensureMountValid();
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

    private static void validateNormalDirectory(Path path, String description) throws IOException {
        BasicFileAttributes attributes =
                Files.readAttributes(path, BasicFileAttributes.class, LinkOption.NOFOLLOW_LINKS);
        if (!attributes.isDirectory() || attributes.isSymbolicLink() || attributes.isOther()) {
            throw new IOException(description + " must be a real directory");
        }
    }

    private static Path temporaryName(Path targetFileName) {
        return targetFileName
                .getFileSystem()
                .getPath("." + targetFileName + "."
                        + UUID.randomUUID().toString().toLowerCase() + ".tmp");
    }

    private static boolean isStorageTemporaryFile(Path fileName) {
        return fileName != null
                && TEMPORARY_FILE_NAME.matcher(fileName.toString()).matches();
    }

    private static boolean samePath(Path left, Path right) throws IOException {
        return Files.isSameFile(left, right);
    }

    private static StorageVolumeUnavailableException unavailable(String message, IOException cause) {
        return new StorageVolumeUnavailableException(message, cause);
    }

    private static final class UnsafeStoragePathException extends IOException {

        private UnsafeStoragePathException(String message) {
            super(message);
        }
    }

    private record MountIdentity(Path realPath, Object fileKey) {}

    private record SecureParent(SecureDirectoryStream<Path> directory, Path fileName) implements AutoCloseable {

        private BasicFileAttributes readAttributes() throws IOException {
            return directory
                    .getFileAttributeView(fileName, BasicFileAttributeView.class, LinkOption.NOFOLLOW_LINKS)
                    .readAttributes();
        }

        @Override
        public void close() throws IOException {
            directory.close();
        }
    }

    private record Probe(boolean healthy, long totalCapacity, long availableCapacity, long checkedAtNanos) {}
}
