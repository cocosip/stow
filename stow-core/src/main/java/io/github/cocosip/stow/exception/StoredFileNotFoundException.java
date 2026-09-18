package io.github.cocosip.stow.exception;

public final class StoredFileNotFoundException extends StowException {

    public StoredFileNotFoundException(String message) {
        super("STOW_STORED_FILE_NOT_FOUND", message);
    }
}
