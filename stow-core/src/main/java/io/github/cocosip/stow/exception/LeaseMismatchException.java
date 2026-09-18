package io.github.cocosip.stow.exception;

public final class LeaseMismatchException extends StowException {

    public LeaseMismatchException(String message) {
        super("STOW_LEASE_MISMATCH", message);
    }
}
