package io.github.cocosip.stow.spring;

import io.github.cocosip.stow.RuntimeState;
import io.github.cocosip.stow.StowRuntime;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;
import org.springframework.context.SmartLifecycle;

/** Owns the lifecycle of the single Stow runtime created by the starter. */
public final class StowLifecycle implements SmartLifecycle {

    private final StowRuntime runtime;
    private final AtomicBoolean running = new AtomicBoolean();

    public StowLifecycle(StowRuntime runtime) {
        this.runtime = Objects.requireNonNull(runtime, "runtime");
    }

    @Override
    public void start() {
        if (running.compareAndSet(false, true)) {
            try {
                RuntimeState state = runtime.state();
                if (state == RuntimeState.NEW) {
                    runtime.start();
                } else if (state != RuntimeState.RUNNING) {
                    throw new IllegalStateException("Stow runtime cannot be started from " + state + " state");
                }
            } catch (RuntimeException | Error failure) {
                running.set(false);
                throw failure;
            }
        }
    }

    @Override
    public void stop() {
        stop(() -> {});
    }

    @Override
    public void stop(Runnable callback) {
        Objects.requireNonNull(callback, "callback");
        if (running.compareAndSet(true, false)) {
            try {
                runtime.close();
            } finally {
                callback.run();
            }
        } else {
            callback.run();
        }
    }

    @Override
    public boolean isRunning() {
        return running.get();
    }

    @Override
    public boolean isAutoStartup() {
        return true;
    }

    @Override
    public int getPhase() {
        return Integer.MAX_VALUE;
    }
}
