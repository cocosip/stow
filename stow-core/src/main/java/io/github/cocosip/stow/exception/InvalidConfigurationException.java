package io.github.cocosip.stow.exception;

public final class InvalidConfigurationException extends StowException {

    @java.io.Serial
    private static final long serialVersionUID = 1L;

    public InvalidConfigurationException(String message) {
        super("STOW_INVALID_CONFIGURATION", message);
    }

    public InvalidConfigurationException(String message, Throwable cause) {
        super("STOW_INVALID_CONFIGURATION", message, cause);
    }
}
