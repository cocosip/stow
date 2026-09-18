package io.github.cocosip.stow.internal.runtime;

import io.github.cocosip.stow.config.PathsConfiguration;
import io.github.cocosip.stow.exception.RuntimeDirectoryLockedException;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.channels.OverlappingFileLockException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayDeque;
import java.util.Comparator;
import java.util.Deque;
import java.util.List;

final class RuntimeDirectoryLock implements AutoCloseable {

    private static final String LOCK_FILE_NAME = ".stow.lock";

    private final Deque<LockedDirectory> lockedDirectories;

    private RuntimeDirectoryLock(Deque<LockedDirectory> lockedDirectories) {
        this.lockedDirectories = lockedDirectories;
    }

    static RuntimeDirectoryLock acquire(PathsConfiguration paths) {
        List<Path> roots = List.of(
                        paths.metadataDirectory(),
                        paths.quotaDirectory(),
                        paths.queueDirectory(),
                        paths.watcherDirectory())
                .stream()
                .sorted(Comparator.comparing(Path::toString))
                .toList();
        Deque<LockedDirectory> acquired = new ArrayDeque<>();
        Path current = null;
        try {
            for (Path root : roots) {
                current = root;
                Files.createDirectories(root);
                FileChannel channel = FileChannel.open(
                        root.resolve(LOCK_FILE_NAME), StandardOpenOption.CREATE, StandardOpenOption.WRITE);
                FileLock lock = null;
                try {
                    lock = channel.tryLock();
                    if (lock == null) {
                        throw new OverlappingFileLockException();
                    }
                    acquired.push(new LockedDirectory(channel, lock));
                } catch (IOException | RuntimeException exception) {
                    if (lock != null) {
                        lock.close();
                    }
                    channel.close();
                    throw exception;
                }
            }
            return new RuntimeDirectoryLock(acquired);
        } catch (IOException | OverlappingFileLockException exception) {
            closeAll(acquired);
            throw new RuntimeDirectoryLockedException(
                    "Unable to acquire runtime directory lock for " + current, exception);
        }
    }

    @Override
    public void close() {
        IOException failure = closeAll(lockedDirectories);
        if (failure != null) {
            throw new RuntimeDirectoryLockedException("Unable to release runtime directory locks", failure);
        }
    }

    private static IOException closeAll(Deque<LockedDirectory> directories) {
        IOException failure = null;
        while (!directories.isEmpty()) {
            try {
                directories.pop().close();
            } catch (IOException exception) {
                if (failure == null) {
                    failure = exception;
                } else {
                    failure.addSuppressed(exception);
                }
            }
        }
        return failure;
    }

    private record LockedDirectory(FileChannel channel, FileLock lock) implements AutoCloseable {

        @Override
        public void close() throws IOException {
            try {
                lock.close();
            } finally {
                channel.close();
            }
        }
    }
}
