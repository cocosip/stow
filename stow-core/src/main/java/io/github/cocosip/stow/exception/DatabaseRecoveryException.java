package io.github.cocosip.stow.exception;

public final class DatabaseRecoveryException extends StowException {

    public DatabaseRecoveryException(String message) {
        super("STOW_DATABASE_RECOVERY", message);
    }

    public DatabaseRecoveryException(String message, Throwable cause) {
        super("STOW_DATABASE_RECOVERY", message, cause);
    }
}
