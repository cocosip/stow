package io.github.cocosip.stow.exception;

public final class InvalidConfigurationException extends StowException {

    public InvalidConfigurationException(String message) {
        super("STOW_INVALID_CONFIGURATION", message);
    }

    public InvalidConfigurationException(String message, Throwable cause) {
        super("STOW_INVALID_CONFIGURATION", message, cause);
    }
}
