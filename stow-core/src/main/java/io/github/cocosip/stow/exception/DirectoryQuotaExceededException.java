package io.github.cocosip.stow.exception;

public final class DirectoryQuotaExceededException extends StowException {

    public DirectoryQuotaExceededException(String message) {
        super("STOW_DIRECTORY_QUOTA_EXCEEDED", message);
    }
}
