package io.github.cocosip.stow.exception;

public final class FileAlreadyProcessingException extends StowException {

    @java.io.Serial
    private static final long serialVersionUID = 1L;

    public FileAlreadyProcessingException(String message) {
        super("STOW_FILE_ALREADY_PROCESSING", message);
    }
}
