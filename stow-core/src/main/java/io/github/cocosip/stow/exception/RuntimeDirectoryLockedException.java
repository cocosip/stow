package io.github.cocosip.stow.exception;

public final class RuntimeDirectoryLockedException extends StowException {

    public RuntimeDirectoryLockedException(String message) {
        super("STOW_RUNTIME_DIRECTORY_LOCKED", message);
    }

    public RuntimeDirectoryLockedException(String message, Throwable cause) {
        super("STOW_RUNTIME_DIRECTORY_LOCKED", message, cause);
    }
}
