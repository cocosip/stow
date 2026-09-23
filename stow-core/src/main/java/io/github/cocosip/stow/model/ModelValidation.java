package io.github.cocosip.stow.model;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.regex.Pattern;

final class ModelValidation {

    static final int MAX_ERROR_LENGTH = 4_096;
    static final int MAX_FILE_NAME_LENGTH = 255;
    static final int MAX_EXTENSION_LENGTH = 32;

    private static final Pattern IDENTIFIER = Pattern.compile("[A-Za-z0-9._-]{1,128}");
    private static final Pattern FILE_KEY = Pattern.compile("[0-9a-f]{32}");
    private static final Pattern EXTENSION = Pattern.compile("[A-Za-z0-9._-]{1,32}");
    private static final Set<String> WINDOWS_RESERVED = Set.of(
            "CON", "PRN", "AUX", "NUL", "COM1", "COM2", "COM3", "COM4", "COM5", "COM6", "COM7", "COM8", "COM9", "LPT1",
            "LPT2", "LPT3", "LPT4", "LPT5", "LPT6", "LPT7", "LPT8", "LPT9");

    private ModelValidation() {}

    static <T> T required(String name, T value) {
        if (value == null) {
            throw invalid(name, "must not be null");
        }
        return value;
    }

    static String requiredText(String name, String value) {
        if (value == null || value.isBlank()) {
            throw invalid(name, "must not be blank");
        }
        return value;
    }

    static String identifier(String name, String value) {
        requiredText(name, value);
        if (!IDENTIFIER.matcher(value).matches()
                || value.equals(".")
                || value.equals("..")
                || WINDOWS_RESERVED.contains(value.toUpperCase(Locale.ROOT))) {
            throw invalid(name, "is not a valid identifier");
        }
        return value;
    }

    static String fileKey(String value) {
        requiredText("fileKey", value);
        if (!FILE_KEY.matcher(value).matches()) {
            throw invalid("fileKey", "must contain exactly 32 lowercase hexadecimal characters");
        }
        return value;
    }

    static String logicalDirectory(String value) {
        if (value == null || value.isBlank()) {
            return "/";
        }
        if (value.indexOf('\\') >= 0 || value.indexOf('\0') >= 0) {
            throw invalid("logicalDirectory", "contains a forbidden character");
        }
        // canonicalize so "docs", "/docs", "docs/", and "docs//." all map to one quota key "/docs"
        List<String> segments = new ArrayList<>();
        for (String segment : value.split("/", -1)) {
            if (segment.isEmpty() || segment.equals(".")) {
                continue;
            }
            if (segment.equals("..") || segment.chars().anyMatch(Character::isISOControl)) {
                throw invalid("logicalDirectory", "contains an unsafe segment");
            }
            segments.add(segment);
        }
        if (segments.isEmpty()) {
            return "/";
        }
        return "/" + String.join("/", segments);
    }

    static String fileName(String value) {
        if (value != null && value.length() > MAX_FILE_NAME_LENGTH) {
            throw invalid("originalFileName", "must not exceed 255 characters");
        }
        return value;
    }

    static String extension(String value) {
        if (value == null || value.isEmpty()) {
            return value;
        }
        if (value.length() > MAX_EXTENSION_LENGTH || !EXTENSION.matcher(value).matches()) {
            throw invalid("fileExtension", "is invalid or longer than 32 characters");
        }
        return value;
    }

    static String errorSummary(String value) {
        if (value == null || value.length() <= MAX_ERROR_LENGTH) {
            return value;
        }
        return value.substring(0, MAX_ERROR_LENGTH);
    }

    static long nonNegative(String name, long value) {
        if (value < 0) {
            throw invalid(name, "must not be negative");
        }
        return value;
    }

    static int nonNegative(String name, int value) {
        if (value < 0) {
            throw invalid(name, "must not be negative");
        }
        return value;
    }

    static int positive(String name, int value) {
        if (value <= 0) {
            throw invalid(name, "must be greater than zero");
        }
        return value;
    }

    static Duration nonNegative(String name, Duration value) {
        required(name, value);
        if (value.isNegative()) {
            throw invalid(name, "must not be negative");
        }
        return value;
    }

    static IllegalArgumentException invalid(String name, String detail) {
        return new IllegalArgumentException(name + " " + detail);
    }
}
