package io.github.cocosip.stow.exception;

public final class JournalCorruptionException extends StowException {

    public JournalCorruptionException(String message) {
        super("STOW_JOURNAL_CORRUPTION", message);
    }

    public JournalCorruptionException(String message, Throwable cause) {
        super("STOW_JOURNAL_CORRUPTION", message, cause);
    }
}
