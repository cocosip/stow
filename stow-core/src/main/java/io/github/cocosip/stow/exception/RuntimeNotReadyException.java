package io.github.cocosip.stow.exception;

public final class RuntimeNotReadyException extends StowException {

    @java.io.Serial
    private static final long serialVersionUID = 1L;

    public RuntimeNotReadyException(String message) {
        super("STOW_RUNTIME_NOT_READY", message);
    }
}
