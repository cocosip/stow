package io.github.cocosip.stow.internal.filesystem;

import java.security.SecureRandom;

final class FileKeyGenerator {

    private static final char[] HEX = "0123456789abcdef".toCharArray();

    private final SecureRandom random = new SecureRandom();

    String next() {
        byte[] bytes = new byte[16];
        random.nextBytes(bytes);
        char[] key = new char[32];
        for (int index = 0; index < bytes.length; index++) {
            int value = Byte.toUnsignedInt(bytes[index]);
            key[index * 2] = HEX[value >>> 4];
            key[index * 2 + 1] = HEX[value & 0x0f];
        }
        return new String(key);
    }
}
