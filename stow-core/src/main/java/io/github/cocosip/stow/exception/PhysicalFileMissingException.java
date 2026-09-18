package io.github.cocosip.stow.exception;

public final class PhysicalFileMissingException extends StowException {

    public PhysicalFileMissingException(String message) {
        super("STOW_PHYSICAL_FILE_MISSING", message);
    }
}
