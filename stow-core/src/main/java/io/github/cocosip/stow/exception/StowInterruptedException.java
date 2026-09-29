package io.github.cocosip.stow.exception;

public final class StowInterruptedException extends StowException {

    @java.io.Serial
    private static final long serialVersionUID = 1L;

    public StowInterruptedException(String message, InterruptedException cause) {
        super("STOW_INTERRUPTED", message, cause);
        Thread.currentThread().interrupt();
    }

    public StowInterruptedException(InterruptedException cause) {
        this("Stow operation was interrupted", cause);
    }
}
