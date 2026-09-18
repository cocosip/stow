package io.github.cocosip.stow.exception;

public final class RuntimeNotReadyException extends StowException {

    public RuntimeNotReadyException(String message) {
        super("STOW_RUNTIME_NOT_READY", message);
    }
}
