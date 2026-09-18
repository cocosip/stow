package io.github.cocosip.stow.internal.runtime;

import io.github.cocosip.stow.RuntimeState;
import io.github.cocosip.stow.exception.RuntimeNotReadyException;
import io.github.cocosip.stow.exception.StowInterruptedException;
import io.github.cocosip.stow.internal.quota.QuotaOperationAdmission;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReentrantReadWriteLock;

final class RuntimeQuotaOperationAdmission implements QuotaOperationAdmission {

    private final AtomicReference<RuntimeState> state;
    private final ReentrantReadWriteLock lifecycleLock = new ReentrantReadWriteLock(true);

    RuntimeQuotaOperationAdmission(AtomicReference<RuntimeState> state) {
        this.state = Objects.requireNonNull(state, "state");
    }

    @Override
    public void enter() {
        try {
            lifecycleLock.readLock().lockInterruptibly();
        } catch (InterruptedException exception) {
            throw new StowInterruptedException("Interrupted while waiting to enter a quota operation", exception);
        }
        if (state.get() != RuntimeState.RUNNING) {
            lifecycleLock.readLock().unlock();
            throw new RuntimeNotReadyException("Stow runtime is not running");
        }
    }

    @Override
    public void exit() {
        lifecycleLock.readLock().unlock();
    }

    void closeAdmission(Runnable beginClosing, Runnable cleanup) {
        Objects.requireNonNull(beginClosing, "beginClosing").run();
        lifecycleLock.writeLock().lock();
        try {
            Objects.requireNonNull(cleanup, "cleanup").run();
        } finally {
            lifecycleLock.writeLock().unlock();
        }
    }
}
