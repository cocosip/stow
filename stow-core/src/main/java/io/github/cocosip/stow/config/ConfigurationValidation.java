package io.github.cocosip.stow.config;

import io.github.cocosip.stow.exception.InvalidConfigurationException;
import java.nio.file.Path;
import java.time.Duration;

final class ConfigurationValidation {

    private ConfigurationValidation() {}

    static int positive(String name, int value) {
        if (value <= 0) {
            throw invalid(name, "must be greater than zero");
        }
        return value;
    }

    static long positive(String name, long value) {
        if (value <= 0) {
            throw invalid(name, "must be greater than zero");
        }
        return value;
    }

    static long nonNegative(String name, long value) {
        if (value < 0) {
            throw invalid(name, "must not be negative");
        }
        return value;
    }

    static Duration positive(String name, Duration value) {
        nonNull(name, value);
        if (value.isZero() || value.isNegative()) {
            throw invalid(name, "must be greater than zero");
        }
        return value;
    }

    static Duration nonNegative(String name, Duration value) {
        nonNull(name, value);
        if (value.isNegative()) {
            throw invalid(name, "must not be negative");
        }
        return value;
    }

    static <T> T nonNull(String name, T value) {
        if (value == null) {
            throw invalid(name, "must not be null");
        }
        return value;
    }

    static Path normalizedPath(String name, Path value) {
        nonNull(name, value);
        return value.toAbsolutePath().normalize();
    }

    static InvalidConfigurationException invalid(String name, String detail) {
        return new InvalidConfigurationException(name + " " + detail);
    }
}
