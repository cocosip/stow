package io.github.cocosip.stow.exception;

public final class StorageVolumeUnavailableException extends StowException {

    @java.io.Serial
    private static final long serialVersionUID = 1L;

    public StorageVolumeUnavailableException(String message) {
        super("STOW_STORAGE_VOLUME_UNAVAILABLE", message);
    }

    public StorageVolumeUnavailableException(String message, Throwable cause) {
        super("STOW_STORAGE_VOLUME_UNAVAILABLE", message, cause);
    }
}
