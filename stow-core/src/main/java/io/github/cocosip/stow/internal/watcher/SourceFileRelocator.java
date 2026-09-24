package io.github.cocosip.stow.internal.watcher;

import java.io.IOException;
import java.io.InputStream;
import java.nio.channels.FileChannel;
import java.nio.file.AtomicMoveNotSupportedException;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.security.DigestInputStream;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import java.util.Objects;

final class SourceFileRelocator {

    enum Status {
        COMPLETED,
        SOURCE_MISSING,
        FINGERPRINT_MISMATCH
    }

    record Result(Status status, Path destination) {}

    private record CollisionResolution(Path destination, boolean reuseExisting) {}

    @FunctionalInterface
    interface AtomicMover {
        void move(Path source, Path target) throws IOException;
    }

    private final AtomicMover atomicMover;

    SourceFileRelocator() {
        this((source, target) -> Files.move(source, target, StandardCopyOption.ATOMIC_MOVE));
    }

    SourceFileRelocator(AtomicMover atomicMover) {
        this.atomicMover = Objects.requireNonNull(atomicMover, "atomicMover");
    }

    Status deleteIfMatching(Path source, SourceFingerprint expected) throws IOException {
        Path normalized = source.toAbsolutePath().normalize();
        Status state = sourceState(normalized, expected);
        if (state != Status.COMPLETED) return state;
        Files.delete(normalized);
        return Status.COMPLETED;
    }

    Result moveIfMatching(Path source, Path requestedTarget, SourceFingerprint expected) throws IOException {
        Path normalizedSource = source.toAbsolutePath().normalize();
        Status state = sourceState(normalizedSource, expected);
        if (state != Status.COMPLETED) return new Result(state, null);

        Path normalizedTarget = requestedTarget.toAbsolutePath().normalize();
        Path targetParent = normalizedTarget.getParent();
        if (targetParent == null || normalizedTarget.getFileName() == null) {
            throw new IOException("Move target must name a file below a parent directory");
        }
        Files.createDirectories(targetParent);
        CollisionResolution resolution = resolveCollision(normalizedSource, normalizedTarget, expected);
        Path destination = resolution.destination();
        if (resolution.reuseExisting()) {
            Files.delete(normalizedSource);
            return new Result(Status.COMPLETED, destination);
        }
        try {
            atomicMover.move(normalizedSource, destination);
            return new Result(Status.COMPLETED, destination);
        } catch (AtomicMoveNotSupportedException exception) {
            return copyVerifyPublishAndDelete(normalizedSource, destination, expected);
        }
    }

    private Result copyVerifyPublishAndDelete(Path source, Path destination, SourceFingerprint expected)
            throws IOException {
        Path parent = destination.getParent();
        Path fileName = destination.getFileName();
        if (parent == null || fileName == null) throw new IOException("Move destination must have a parent and name");
        Path temporary = Files.createTempFile(parent, "." + fileName + ".", ".tmp");
        boolean published = false;
        try {
            Files.copy(source, temporary, StandardCopyOption.REPLACE_EXISTING);
            try (FileChannel channel = FileChannel.open(temporary, StandardOpenOption.WRITE)) {
                channel.force(true);
            }
            if (!Arrays.equals(fullHash(source), fullHash(temporary))) {
                throw new IOException("Source changed while it was copied");
            }
            Files.move(temporary, destination, StandardCopyOption.ATOMIC_MOVE);
            published = true;
            if (!SourceFingerprint.capture(source).equals(expected)) {
                Files.deleteIfExists(destination);
                return new Result(Status.FINGERPRINT_MISMATCH, null);
            }
            Files.delete(source);
            return new Result(Status.COMPLETED, destination);
        } finally {
            if (!published) Files.deleteIfExists(temporary);
        }
    }

    private static CollisionResolution resolveCollision(Path source, Path requested, SourceFingerprint expected)
            throws IOException {
        for (int suffix = 0; ; suffix++) {
            Path candidate = suffix == 0 ? requested : suffixed(requested, suffix);
            if (!Files.exists(candidate, LinkOption.NOFOLLOW_LINKS)) {
                return new CollisionResolution(candidate, false);
            }
            if (Arrays.equals(fullHash(source), fullHash(candidate))) {
                if (SourceFingerprint.capture(source).equals(expected)) {
                    return new CollisionResolution(candidate, true);
                }
                throw new IOException("Source changed while resolving a move collision");
            }
        }
    }

    private static Path suffixed(Path path, int suffix) {
        Path fileName = path.getFileName();
        if (fileName == null) throw new IllegalArgumentException("Move destination must name a file");
        String name = fileName.toString();
        int dot = name.lastIndexOf('.');
        String stem = dot <= 0 ? name : name.substring(0, dot);
        String extension = dot <= 0 ? "" : name.substring(dot);
        return path.resolveSibling(stem + "." + suffix + extension);
    }

    private static Status sourceState(Path source, SourceFingerprint expected) throws IOException {
        Objects.requireNonNull(expected, "expected");
        if (!Files.exists(source, LinkOption.NOFOLLOW_LINKS)) return Status.SOURCE_MISSING;
        return SourceFingerprint.capture(source).equals(expected) ? Status.COMPLETED : Status.FINGERPRINT_MISMATCH;
    }

    private static byte[] fullHash(Path path) throws IOException {
        MessageDigest digest = sha256();
        try (InputStream input = new DigestInputStream(Files.newInputStream(path), digest)) {
            input.transferTo(java.io.OutputStream.nullOutputStream());
        }
        return digest.digest();
    }

    private static MessageDigest sha256() {
        try {
            return MessageDigest.getInstance("SHA-256");
        } catch (NoSuchAlgorithmException exception) {
            throw new IllegalStateException("SHA-256 is unavailable", exception);
        }
    }
}
