package io.github.cocosip.stow.internal.runtime;

interface ManagedBackgroundService extends BackgroundServiceCoordinator.Service, AutoCloseable {

    void start();

    @Override
    void close();
}
