package io.github.cocosip.stow.exception;

public final class PhysicalFileMissingException extends StowException {

    @java.io.Serial
    private static final long serialVersionUID = 1L;

    public PhysicalFileMissingException(String message) {
        super("STOW_PHYSICAL_FILE_MISSING", message);
    }
}
