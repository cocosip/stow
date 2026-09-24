package io.github.cocosip.stow.internal.watcher;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SeekableByteChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.nio.file.attribute.BasicFileAttributes;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HexFormat;

record SourceFingerprint(
        int version, String path, long size, long lastModifiedMillis, long creationTimeMillis, String sampleSha256) {

    private static final int VERSION = 1;
    private static final int SAMPLE_SIZE = 64 * 1024;

    static SourceFingerprint capture(Path source) throws IOException {
        Path normalized = source.toAbsolutePath().normalize();
        BasicFileAttributes attributes =
                Files.readAttributes(normalized, BasicFileAttributes.class, LinkOption.NOFOLLOW_LINKS);
        if (!attributes.isRegularFile() || attributes.isSymbolicLink() || attributes.isOther()) {
            throw new IOException("Source must be a regular file: " + normalized);
        }
        MessageDigest digest = sha256();
        digest.update(normalized.toString().getBytes(StandardCharsets.UTF_8));
        digest.update((byte) 0);
        digest.update(ByteBuffer.allocate(Long.BYTES * 3)
                .putLong(attributes.size())
                .putLong(attributes.lastModifiedTime().toMillis())
                .putLong(attributes.creationTime().toMillis())
                .array());
        try (SeekableByteChannel channel = Files.newByteChannel(normalized, StandardOpenOption.READ)) {
            update(digest, channel, 0, Math.min(attributes.size(), SAMPLE_SIZE));
            if (attributes.size() > SAMPLE_SIZE) {
                long tailOffset = Math.max(SAMPLE_SIZE, attributes.size() - SAMPLE_SIZE);
                update(digest, channel, tailOffset, attributes.size() - tailOffset);
            }
        }
        return new SourceFingerprint(
                VERSION,
                normalized.toString(),
                attributes.size(),
                attributes.lastModifiedTime().toMillis(),
                attributes.creationTime().toMillis(),
                HexFormat.of().formatHex(digest.digest()));
    }

    private static void update(MessageDigest digest, SeekableByteChannel channel, long offset, long length)
            throws IOException {
        channel.position(offset);
        ByteBuffer buffer = ByteBuffer.allocate((int) Math.min(SAMPLE_SIZE, length));
        long remaining = length;
        while (remaining > 0) {
            buffer.clear();
            buffer.limit((int) Math.min(buffer.capacity(), remaining));
            int read = channel.read(buffer);
            if (read < 0) throw new IOException("Source changed while its fingerprint was captured");
            if (read == 0) continue;
            digest.update(buffer.array(), 0, read);
            remaining -= read;
        }
    }

    private static MessageDigest sha256() {
        try {
            return MessageDigest.getInstance("SHA-256");
        } catch (NoSuchAlgorithmException exception) {
            throw new IllegalStateException("SHA-256 is unavailable", exception);
        }
    }
}
