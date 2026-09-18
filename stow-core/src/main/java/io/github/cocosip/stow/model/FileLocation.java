package io.github.cocosip.stow.model;

import java.nio.file.Path;
import java.time.Instant;

public record FileLocation(
        String fileKey,
        String tenantId,
        String volumeId,
        Path physicalPath,
        String logicalDirectory,
        long fileSize,
        Instant createdAt,
        FileProcessingStatus status,
        int retryCount,
        Instant lastFailedAt,
        String lastError,
        Instant availableAt) {

    public FileLocation {
        fileKey = ModelValidation.fileKey(fileKey);
        tenantId = ModelValidation.identifier("tenantId", tenantId);
        volumeId = ModelValidation.identifier("volumeId", volumeId);
        ModelValidation.required("physicalPath", physicalPath);
        physicalPath = physicalPath.normalize();
        logicalDirectory = ModelValidation.logicalDirectory(logicalDirectory);
        ModelValidation.nonNegative("fileSize", fileSize);
        ModelValidation.required("createdAt", createdAt);
        ModelValidation.required("status", status);
        ModelValidation.nonNegative("retryCount", retryCount);
        lastError = ModelValidation.errorSummary(lastError);
    }
}
