package io.github.cocosip.stow.internal.runtime;

interface ManagedBackgroundService extends AutoCloseable {

    void start();

    @Override
    void close();
}
