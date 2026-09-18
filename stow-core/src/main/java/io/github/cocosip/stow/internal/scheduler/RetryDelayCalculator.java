package io.github.cocosip.stow.internal.scheduler;

import io.github.cocosip.stow.config.RetryConfiguration;
import java.time.Duration;
import java.time.Instant;
import java.util.Objects;

/** Calculates retry availability without overflowing duration arithmetic. */
public final class RetryDelayCalculator {

    private final int maxRetryCount;
    private final Duration initialDelay;
    private final boolean exponentialBackoff;
    private final Duration maxDelay;

    public RetryDelayCalculator(RetryConfiguration configuration) {
        this(
                Objects.requireNonNull(configuration, "configuration").maxRetryCount(),
                configuration.initialDelay(),
                configuration.exponentialBackoff(),
                configuration.maxDelay());
    }

    public RetryDelayCalculator(
            int maxRetryCount, Duration initialDelay, boolean exponentialBackoff, Duration maxDelay) {
        if (maxRetryCount < 0) throw new IllegalArgumentException("maxRetryCount must not be negative");
        this.maxRetryCount = maxRetryCount;
        this.initialDelay = requireNonNegative("initialDelay", initialDelay);
        this.exponentialBackoff = exponentialBackoff;
        this.maxDelay = requireNonNegative("maxDelay", maxDelay);
        if (this.maxDelay.compareTo(this.initialDelay) < 0) {
            throw new IllegalArgumentException("maxDelay must not be shorter than initialDelay");
        }
    }

    public Duration delayFor(int retryCount) {
        if (retryCount <= 0) throw new IllegalArgumentException("retryCount must be positive");
        if (!exponentialBackoff || retryCount == 1) return initialDelay;
        long seconds = initialDelay.getSeconds();
        int nanos = initialDelay.getNano();
        for (int index = 1; index < retryCount; index++) {
            if (seconds > Long.MAX_VALUE / 2) return maxDelay;
            seconds *= 2;
            nanos *= 2;
            if (nanos >= 1_000_000_000) {
                seconds++;
                nanos -= 1_000_000_000;
            }
            if (Duration.ofSeconds(seconds, nanos).compareTo(maxDelay) >= 0) return maxDelay;
        }
        Duration result = Duration.ofSeconds(seconds, nanos);
        return result.compareTo(maxDelay) > 0 ? maxDelay : result;
    }

    public Duration calculate(int retryCount) {
        return delayFor(retryCount);
    }

    public Duration calculateDelay(int retryCount) {
        return delayFor(retryCount);
    }

    public Instant availableAt(Instant failedAt, int retryCount) {
        Objects.requireNonNull(failedAt, "failedAt");
        return failedAt.plus(delayFor(retryCount));
    }

    public Instant nextAvailableAt(Instant failedAt, int retryCount) {
        return availableAt(failedAt, retryCount);
    }

    public boolean isPermanent(int retryCount) {
        return retryCount >= maxRetryCount;
    }

    public boolean permanentlyFailed(int retryCount) {
        return isPermanent(retryCount);
    }

    public boolean shouldDeadLetter(int retryCount) {
        return isPermanent(retryCount);
    }

    public int maxRetryCount() {
        return maxRetryCount;
    }

    private static Duration requireNonNegative(String name, Duration value) {
        Objects.requireNonNull(value, name);
        if (value.isNegative()) throw new IllegalArgumentException(name + " must not be negative");
        return value;
    }
}
