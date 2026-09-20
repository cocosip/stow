package io.github.cocosip.stow.internal.runtime;

import io.github.cocosip.stow.exception.StowInterruptedException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/** Owns ordered background services and guarantees reverse-order shutdown. */
public final class BackgroundServiceCoordinator implements AutoCloseable {

    private final List<Service> services;
    private final AtomicBoolean started = new AtomicBoolean();
    private final List<Service> startedServices = new ArrayList<>();

    public BackgroundServiceCoordinator(List<? extends Service> services) {
        this.services = List.copyOf(Objects.requireNonNull(services, "services"));
    }

    public synchronized void start() {
        if (!started.compareAndSet(false, true)) return;
        try {
            for (Service service : services) {
                service.start();
                startedServices.add(service);
            }
        } catch (RuntimeException | Error failure) {
            closeStarted(failure);
            started.set(false);
            throw failure;
        }
    }

    @Override
    public synchronized void close() {
        if (!started.compareAndSet(true, false)) return;
        closeStarted(null);
    }

    public static PeriodicService periodic(
            String name,
            ScheduledExecutorService scheduler,
            Runnable action,
            java.time.Duration initialDelay,
            java.time.Duration interval) {
        return new PeriodicService(name, scheduler, action, initialDelay, interval);
    }

    private void closeStarted(Throwable original) {
        RuntimeException failure = null;
        for (int index = startedServices.size() - 1; index >= 0; index--) {
            try {
                startedServices.get(index).close();
            } catch (RuntimeException exception) {
                if (failure == null) failure = exception;
                else failure.addSuppressed(exception);
            }
        }
        startedServices.clear();
        if (original == null && failure != null) throw failure;
        if (original != null && failure != null) original.addSuppressed(failure);
    }

    public interface Service {
        void start();

        void close();
    }

    public static final class PeriodicService implements Service {
        private final String name;
        private final ScheduledExecutorService scheduler;
        private final Runnable action;
        private final java.time.Duration initialDelay;
        private final java.time.Duration interval;
        private ScheduledFuture<?> future;
        private boolean closing;
        private Thread runningThread;

        private PeriodicService(
                String name,
                ScheduledExecutorService scheduler,
                Runnable action,
                java.time.Duration initialDelay,
                java.time.Duration interval) {
            this.name = Objects.requireNonNull(name, "name");
            this.scheduler = Objects.requireNonNull(scheduler, "scheduler");
            this.action = Objects.requireNonNull(action, "action");
            this.initialDelay = requireNonNegative(initialDelay, "initialDelay");
            this.interval = requirePositive(interval, "interval");
        }

        @Override
        public synchronized void start() {
            closing = false;
            future = scheduler.scheduleWithFixedDelay(
                    this::runAction, initialDelay.toMillis(), interval.toMillis(), TimeUnit.MILLISECONDS);
        }

        @Override
        public synchronized void close() {
            closing = true;
            if (future != null) future.cancel(false);
            while (runningThread != null && runningThread != Thread.currentThread()) {
                try {
                    wait();
                } catch (InterruptedException exception) {
                    Thread.currentThread().interrupt();
                    throw new StowInterruptedException(
                            "Interrupted while stopping background service " + name, exception);
                }
            }
        }

        public String name() {
            return name;
        }

        private void runAction() {
            synchronized (this) {
                if (closing) return;
                runningThread = Thread.currentThread();
            }
            try {
                action.run();
            } finally {
                synchronized (this) {
                    runningThread = null;
                    notifyAll();
                }
            }
        }

        private static java.time.Duration requireNonNegative(java.time.Duration value, String name) {
            Objects.requireNonNull(value, name);
            if (value.isNegative()) throw new IllegalArgumentException(name + " must not be negative");
            return value;
        }

        private static java.time.Duration requirePositive(java.time.Duration value, String name) {
            Objects.requireNonNull(value, name);
            if (value.isZero() || value.isNegative()) throw new IllegalArgumentException(name + " must be positive");
            return value;
        }
    }
}
