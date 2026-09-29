package io.github.cocosip.stow.exception;

public final class ProjectionException extends StowException {

    @java.io.Serial
    private static final long serialVersionUID = 1L;

    public ProjectionException(String message) {
        super("STOW_PROJECTION", message);
    }

    public ProjectionException(String message, Throwable cause) {
        super("STOW_PROJECTION", message, cause);
    }
}
