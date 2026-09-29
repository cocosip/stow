package io.github.cocosip.stow.exception;

public final class DirectoryQuotaExceededException extends StowException {

    @java.io.Serial
    private static final long serialVersionUID = 1L;

    private final String directoryPath;
    private final long currentCount;
    private final long maxCount;

    public DirectoryQuotaExceededException(String message) {
        super("STOW_DIRECTORY_QUOTA_EXCEEDED", message);
        this.directoryPath = null;
        this.currentCount = -1;
        this.maxCount = -1;
    }

    public DirectoryQuotaExceededException(String directoryPath, long currentCount, long maxCount) {
        super("STOW_DIRECTORY_QUOTA_EXCEEDED", "Directory quota exceeded: " + directoryPath);
        this.directoryPath = directoryPath;
        this.currentCount = currentCount;
        this.maxCount = maxCount;
    }

    public String directoryPath() {
        return directoryPath;
    }

    public long currentCount() {
        return currentCount;
    }

    public long maxCount() {
        return maxCount;
    }
}
