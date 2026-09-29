package io.github.cocosip.stow.exception;

public final class DatabaseRecoveryException extends StowException {

    @java.io.Serial
    private static final long serialVersionUID = 1L;

    public DatabaseRecoveryException(String message) {
        super("STOW_DATABASE_RECOVERY", message);
    }

    public DatabaseRecoveryException(String message, Throwable cause) {
        super("STOW_DATABASE_RECOVERY", message, cause);
    }
}
