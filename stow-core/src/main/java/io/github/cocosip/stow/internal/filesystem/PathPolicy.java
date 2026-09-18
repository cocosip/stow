package io.github.cocosip.stow.internal.filesystem;

import java.util.Locale;
import java.util.Set;
import java.util.regex.Pattern;

final class PathPolicy {

    private static final Pattern IDENTIFIER = Pattern.compile("[A-Za-z0-9._-]{1,128}");
    private static final Pattern FILE_KEY = Pattern.compile("[0-9a-f]{32}");
    private static final Pattern EXTENSION = Pattern.compile("\\.[A-Za-z0-9._-]{1,31}");
    private static final Set<String> WINDOWS_RESERVED = Set.of(
            "CON", "PRN", "AUX", "NUL", "COM1", "COM2", "COM3", "COM4", "COM5", "COM6", "COM7", "COM8", "COM9", "LPT1",
            "LPT2", "LPT3", "LPT4", "LPT5", "LPT6", "LPT7", "LPT8", "LPT9");

    private PathPolicy() {}

    static String identifier(String name, String value) {
        String windowsBaseName = value == null ? "" : value.split("\\.", 2)[0].toUpperCase(Locale.ROOT);
        if (value == null
                || !IDENTIFIER.matcher(value).matches()
                || value.equals(".")
                || value.equals("..")
                || WINDOWS_RESERVED.contains(windowsBaseName)) {
            throw new IllegalArgumentException(name + " is not a valid identifier");
        }
        return value;
    }

    static String fileKey(String value) {
        if (value == null || !FILE_KEY.matcher(value).matches()) {
            throw new IllegalArgumentException("fileKey must contain exactly 32 lowercase hexadecimal characters");
        }
        return value;
    }

    static String extensionFrom(String originalFileName) {
        if (originalFileName == null) {
            return "";
        }
        int dot = originalFileName.lastIndexOf('.');
        if (dot <= 0 || dot == originalFileName.length() - 1) {
            return "";
        }
        String extension = originalFileName.substring(dot);
        return EXTENSION.matcher(extension).matches() ? extension : "";
    }

    static String extension(String value) {
        if (value == null || value.isEmpty()) {
            return "";
        }
        return EXTENSION.matcher(value).matches() ? value : "";
    }
}
