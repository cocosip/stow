package io.github.cocosip.stow.exception;

public final class JournalCorruptionException extends StowException {

    @java.io.Serial
    private static final long serialVersionUID = 1L;

    public JournalCorruptionException(String message) {
        super("STOW_JOURNAL_CORRUPTION", message);
    }

    public JournalCorruptionException(String message, Throwable cause) {
        super("STOW_JOURNAL_CORRUPTION", message, cause);
    }
}
