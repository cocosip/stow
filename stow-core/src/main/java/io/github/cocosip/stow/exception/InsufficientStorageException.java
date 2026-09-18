package io.github.cocosip.stow.exception;

public final class InsufficientStorageException extends StowException {

    public InsufficientStorageException(String message) {
        super("STOW_INSUFFICIENT_STORAGE", message);
    }
}
