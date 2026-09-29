package io.github.cocosip.stow.exception;

public final class InsufficientStorageException extends StowException {

    @java.io.Serial
    private static final long serialVersionUID = 1L;

    public InsufficientStorageException(String message) {
        super("STOW_INSUFFICIENT_STORAGE", message);
    }
}
