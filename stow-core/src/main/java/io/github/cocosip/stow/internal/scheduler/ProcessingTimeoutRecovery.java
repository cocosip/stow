package io.github.cocosip.stow.internal.scheduler;

import java.time.Duration;
import java.util.Objects;

/** Reclaims leases that have exceeded the configured processing timeout. */
public final class ProcessingTimeoutRecovery {

    private final DefaultStoragePool pool;
    private final Duration configuredTimeout;

    public ProcessingTimeoutRecovery(DefaultStoragePool pool) {
        this(pool, null);
    }

    public ProcessingTimeoutRecovery(DefaultStoragePool pool, Duration timeout) {
        this.pool = Objects.requireNonNull(pool, "pool");
        if (timeout != null && timeout.isNegative()) throw new IllegalArgumentException("timeout must not be negative");
        this.configuredTimeout = timeout;
    }

    public int recover(Duration timeout) {
        return pool.recoverTimedOut(timeout);
    }

    public int recoverTimedOut(Duration timeout) {
        return recover(timeout);
    }

    public int run(Duration timeout) {
        return recover(timeout);
    }

    public int recover() {
        if (configuredTimeout == null) throw new IllegalStateException("No processing timeout was configured");
        return recover(configuredTimeout);
    }
}
