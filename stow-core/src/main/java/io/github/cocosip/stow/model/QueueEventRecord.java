package io.github.cocosip.stow.model;

import java.nio.file.Path;
import java.time.Instant;
import java.util.UUID;

public record QueueEventRecord(
        int schemaVersion,
        UUID eventId,
        String tenantId,
        String fileKey,
        QueueEventType eventType,
        Instant occurredAt,
        long sequenceNumber,
        String volumeId,
        Path physicalPath,
        String logicalDirectory,
        long fileSize,
        FileProcessingStatus status,
        UUID leaseId,
        Instant processingStartedAt,
        int retryCount,
        Instant availableAt,
        String errorMessage,
        String originalFileName,
        String fileExtension) {

    public QueueEventRecord {
        if (schemaVersion <= 0) {
            throw ModelValidation.invalid("schemaVersion", "must be greater than zero");
        }
        ModelValidation.required("eventId", eventId);
        tenantId = ModelValidation.identifier("tenantId", tenantId);
        fileKey = ModelValidation.fileKey(fileKey);
        ModelValidation.required("eventType", eventType);
        ModelValidation.required("occurredAt", occurredAt);
        if (sequenceNumber <= 0) {
            throw ModelValidation.invalid("sequenceNumber", "must be greater than zero");
        }
        volumeId = ModelValidation.identifier("volumeId", volumeId);
        ModelValidation.required("physicalPath", physicalPath);
        physicalPath = physicalPath.normalize();
        logicalDirectory = ModelValidation.logicalDirectory(logicalDirectory);
        ModelValidation.nonNegative("fileSize", fileSize);
        ModelValidation.required("status", status);
        ModelValidation.nonNegative("retryCount", retryCount);
        errorMessage = ModelValidation.errorSummary(errorMessage);
        originalFileName = ModelValidation.fileName(originalFileName);
        fileExtension = ModelValidation.extension(fileExtension);
    }
}
