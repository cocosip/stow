package io.github.cocosip.stow.exception;

public final class FileAlreadyProcessingException extends StowException {

    public FileAlreadyProcessingException(String message) {
        super("STOW_FILE_ALREADY_PROCESSING", message);
    }
}
