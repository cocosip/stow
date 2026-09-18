package io.github.cocosip.stow.model;

public record WriteOptions(String originalFileName, String logicalDirectory) {

    public WriteOptions {
        originalFileName = ModelValidation.fileName(originalFileName);
        if (logicalDirectory != null) {
            logicalDirectory = ModelValidation.logicalDirectory(logicalDirectory);
        }
    }

    public static WriteOptions defaults() {
        return new WriteOptions(null, null);
    }

    public static WriteOptions ofOriginalFileName(String originalFileName) {
        return new WriteOptions(originalFileName, null);
    }
}
