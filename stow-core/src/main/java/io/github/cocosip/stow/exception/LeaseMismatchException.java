package io.github.cocosip.stow.exception;

public final class LeaseMismatchException extends StowException {

    @java.io.Serial
    private static final long serialVersionUID = 1L;

    public LeaseMismatchException(String message) {
        super("STOW_LEASE_MISMATCH", message);
    }
}
