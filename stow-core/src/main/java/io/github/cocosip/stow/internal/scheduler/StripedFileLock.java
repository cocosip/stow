package io.github.cocosip.stow.internal.scheduler;

import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Supplier;

/** Small bounded lock set used to serialize transitions for one file key. */
public final class StripedFileLock {

    private final ReentrantLock[] stripes;

    public StripedFileLock() {
        this(256);
    }

    public StripedFileLock(int stripeCount) {
        if (stripeCount <= 0) throw new IllegalArgumentException("stripeCount must be positive");
        stripes = new ReentrantLock[stripeCount];
        for (int index = 0; index < stripeCount; index++) stripes[index] = new ReentrantLock();
    }

    public void withLock(String fileKey, Runnable action) {
        lock(fileKey).lock();
        try {
            action.run();
        } finally {
            lock(fileKey).unlock();
        }
    }

    public <T> T withLock(String fileKey, Supplier<T> action) {
        lock(fileKey).lock();
        try {
            return action.get();
        } finally {
            lock(fileKey).unlock();
        }
    }

    private ReentrantLock lock(String fileKey) {
        if (fileKey == null) throw new IllegalArgumentException("fileKey must not be null");
        return stripes[Math.floorMod(fileKey.hashCode(), stripes.length)];
    }
}
