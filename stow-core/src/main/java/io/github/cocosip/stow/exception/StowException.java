package io.github.cocosip.stow.exception;

public abstract class StowException extends RuntimeException {

    private final String errorCode;

    protected StowException(String errorCode, String message) {
        super(message);
        this.errorCode = errorCode;
    }

    protected StowException(String errorCode, String message, Throwable cause) {
        super(message, cause);
        this.errorCode = errorCode;
    }

    public final String errorCode() {
        return errorCode;
    }
}
