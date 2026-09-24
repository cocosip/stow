package io.github.cocosip.stow.internal.watcher;

import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.Objects;

record SourceCleanupJob(
        long id,
        String watcherId,
        String tenantId,
        Path sourcePath,
        SourceFingerprint fingerprint,
        String importOperationId,
        String fileKey,
        SourceCleanupAction action,
        Path moveTargetPath,
        Path failureDirectory,
        int maxAttempts,
        Duration retryInitialDelay,
        Duration retryMaxDelay,
        int attemptCount,
        SourceCleanupState state,
        Instant nextAttemptAt,
        String lastError,
        Instant createdAt,
        Instant updatedAt,
        Instant leaseUntil) {

    SourceCleanupJob {
        if (id < 0) throw new IllegalArgumentException("id must not be negative");
        if (watcherId == null || watcherId.isBlank()) throw new IllegalArgumentException("watcherId is required");
        if (tenantId == null || tenantId.isBlank()) throw new IllegalArgumentException("tenantId is required");
        sourcePath = Objects.requireNonNull(sourcePath, "sourcePath")
                .toAbsolutePath()
                .normalize();
        Objects.requireNonNull(fingerprint, "fingerprint");
        if (importOperationId == null || importOperationId.isBlank()) {
            throw new IllegalArgumentException("importOperationId is required");
        }
        Objects.requireNonNull(action, "action");
        if (moveTargetPath != null)
            moveTargetPath = moveTargetPath.toAbsolutePath().normalize();
        if (failureDirectory != null)
            failureDirectory = failureDirectory.toAbsolutePath().normalize();
        if (action == SourceCleanupAction.MOVE && moveTargetPath == null) {
            throw new IllegalArgumentException("moveTargetPath is required for MOVE");
        }
        if (maxAttempts <= 0) throw new IllegalArgumentException("maxAttempts must be positive");
        Objects.requireNonNull(retryInitialDelay, "retryInitialDelay");
        Objects.requireNonNull(retryMaxDelay, "retryMaxDelay");
        if (retryInitialDelay.isNegative() || retryMaxDelay.isNegative()) {
            throw new IllegalArgumentException("retry delays must not be negative");
        }
        if (attemptCount < 0) throw new IllegalArgumentException("attemptCount must not be negative");
        Objects.requireNonNull(state, "state");
        Objects.requireNonNull(createdAt, "createdAt");
        Objects.requireNonNull(updatedAt, "updatedAt");
    }

    static SourceCleanupJob reservation(
            String watcherId,
            String tenantId,
            Path sourcePath,
            SourceFingerprint fingerprint,
            String importOperationId,
            SourceCleanupAction action,
            Path moveTargetPath,
            Path failureDirectory,
            int maxAttempts,
            Duration retryInitialDelay,
            Duration retryMaxDelay,
            Instant now) {
        return new SourceCleanupJob(
                0,
                watcherId,
                tenantId,
                sourcePath,
                fingerprint,
                importOperationId,
                null,
                action,
                moveTargetPath,
                failureDirectory,
                maxAttempts,
                retryInitialDelay,
                retryMaxDelay,
                0,
                SourceCleanupState.IMPORTING,
                null,
                null,
                now,
                now,
                null);
    }

    SourceCleanupJob withUpdatedAt(Instant value) {
        return copy(fileKey, attemptCount, state, nextAttemptAt, lastError, value, leaseUntil, moveTargetPath);
    }

    SourceCleanupJob withLease(Instant updated, Instant lease) {
        return copy(fileKey, attemptCount, state, nextAttemptAt, lastError, updated, lease, moveTargetPath);
    }

    SourceCleanupJob transition(
            String newFileKey,
            int newAttemptCount,
            SourceCleanupState newState,
            Instant newNextAttemptAt,
            String newLastError,
            Instant updated,
            Path newMoveTargetPath) {
        return copy(
                newFileKey,
                newAttemptCount,
                newState,
                newNextAttemptAt,
                newLastError,
                updated,
                null,
                newMoveTargetPath);
    }

    private SourceCleanupJob copy(
            String newFileKey,
            int newAttemptCount,
            SourceCleanupState newState,
            Instant newNextAttemptAt,
            String newLastError,
            Instant newUpdatedAt,
            Instant newLeaseUntil,
            Path newMoveTargetPath) {
        return new SourceCleanupJob(
                id,
                watcherId,
                tenantId,
                sourcePath,
                fingerprint,
                importOperationId,
                newFileKey,
                action,
                newMoveTargetPath,
                failureDirectory,
                maxAttempts,
                retryInitialDelay,
                retryMaxDelay,
                newAttemptCount,
                newState,
                newNextAttemptAt,
                newLastError,
                createdAt,
                newUpdatedAt,
                newLeaseUntil);
    }
}
