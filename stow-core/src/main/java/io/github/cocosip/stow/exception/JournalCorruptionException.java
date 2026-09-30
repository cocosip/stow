package io.github.cocosip.stow.exception;

public final class JournalCorruptionException extends StowException {

    /** Structural class of the corruption; repair decisions must key on this, never on message text. */
    public enum Reason {
        /** Truncated, malformed, or otherwise structurally invalid frame or record. */
        STRUCTURE,
        /** Checksum mismatch over otherwise complete bytes. */
        CRC,
        /** Sequence gap or regression between consecutive records. */
        SEQUENCE,
        /** Unknown schema version or field; the bytes may be valid for a newer writer. */
        SCHEMA
    }

    @java.io.Serial
    private static final long serialVersionUID = 1L;

    private final Reason reason;

    public JournalCorruptionException(String message) {
        this(Reason.STRUCTURE, message);
    }

    public JournalCorruptionException(Reason reason, String message) {
        super("STOW_JOURNAL_CORRUPTION", message);
        this.reason = reason == null ? Reason.STRUCTURE : reason;
    }

    public JournalCorruptionException(String message, Throwable cause) {
        this(Reason.STRUCTURE, message, cause);
    }

    public JournalCorruptionException(Reason reason, String message, Throwable cause) {
        super("STOW_JOURNAL_CORRUPTION", message, cause);
        this.reason = reason == null ? Reason.STRUCTURE : reason;
    }

    public Reason reason() {
        return reason;
    }
}
