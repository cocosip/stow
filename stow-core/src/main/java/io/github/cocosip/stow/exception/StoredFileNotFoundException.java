package io.github.cocosip.stow.exception;

public final class StoredFileNotFoundException extends StowException {

    @java.io.Serial
    private static final long serialVersionUID = 1L;

    public StoredFileNotFoundException(String message) {
        super("STOW_STORED_FILE_NOT_FOUND", message);
    }
}
