package io.github.cocosip.stow.internal.filesystem;

import java.security.SecureRandom;
import java.util.concurrent.atomic.AtomicLong;

public final class FileKeyGenerator {

    private static final char[] HEX = "0123456789abcdef".toCharArray();

    /** Consecutive keys sharing a shard prefix, so short write bursts land in one directory. */
    private static final int SHARD_REUSE_WINDOW = 32;

    private static final long SHARD_MIX_CONSTANT = 0x9E3779B97F4A7C15L;

    private final SecureRandom random;
    private final AtomicLong sequence = new AtomicLong();
    private final int shardSeed;

    /** Derives the per-process shard seed like Locus: a hash of a fresh GUID. */
    public FileKeyGenerator() {
        this(java.util.UUID.randomUUID().hashCode());
    }

    public FileKeyGenerator(int shardSeed) {
        this.shardSeed = shardSeed;
        this.random = new SecureRandom();
    }

    public String next() {
        long batchOrdinal = sequence.getAndIncrement() / SHARD_REUSE_WINDOW;
        int shardPrefix = shardPrefix(batchOrdinal);
        byte[] bytes = new byte[16];
        random.nextBytes(bytes);
        bytes[0] = (byte) (shardPrefix >>> 8);
        bytes[1] = (byte) shardPrefix;
        char[] key = new char[32];
        for (int index = 0; index < bytes.length; index++) {
            int value = Byte.toUnsignedInt(bytes[index]);
            key[index * 2] = HEX[value >>> 4];
            key[index * 2 + 1] = HEX[value & 0x0f];
        }
        return new String(key);
    }

    /** Murmur3-style 64-bit finalizer (Locus ComputeFileKeyShardPrefix) over the batch ordinal. */
    private int shardPrefix(long batchOrdinal) {
        long mixed = ((long) shardSeed << 32) ^ batchOrdinal ^ SHARD_MIX_CONSTANT;
        mixed ^= mixed >>> 33;
        mixed *= 0xff51afd7ed558ccdL;
        mixed ^= mixed >>> 33;
        mixed *= 0xc4ceb9fe1a85ec53L;
        mixed ^= mixed >>> 33;
        return (int) (mixed & 0xffff);
    }
}
