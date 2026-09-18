package io.github.cocosip.stow.model;

import java.time.Instant;

public record StoredFileInfo(
        String fileKey,
        String tenantId,
        long fileSize,
        Instant createdAt,
        FileProcessingStatus status,
        int retryCount,
        String originalFileName,
        String fileExtension) {

    public StoredFileInfo {
        fileKey = ModelValidation.fileKey(fileKey);
        tenantId = ModelValidation.identifier("tenantId", tenantId);
        ModelValidation.nonNegative("fileSize", fileSize);
        ModelValidation.required("createdAt", createdAt);
        ModelValidation.required("status", status);
        ModelValidation.nonNegative("retryCount", retryCount);
        originalFileName = ModelValidation.fileName(originalFileName);
        fileExtension = ModelValidation.extension(fileExtension);
    }
}
