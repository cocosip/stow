package io.github.cocosip.stow.model;

import java.time.Instant;
import java.util.List;

public record CleanupStatistics(
        Instant startedAt,
        Instant finishedAt,
        long scannedCount,
        long succeededCount,
        long skippedCount,
        long failedCount,
        long releasedBytes,
        long affectedTenantCount,
        List<MaintenanceError> errors) {

    public CleanupStatistics {
        ModelValidation.required("startedAt", startedAt);
        ModelValidation.required("finishedAt", finishedAt);
        if (finishedAt.isBefore(startedAt)) {
            throw ModelValidation.invalid("finishedAt", "must not precede startedAt");
        }
        ModelValidation.nonNegative("scannedCount", scannedCount);
        ModelValidation.nonNegative("succeededCount", succeededCount);
        ModelValidation.nonNegative("skippedCount", skippedCount);
        ModelValidation.nonNegative("failedCount", failedCount);
        ModelValidation.nonNegative("releasedBytes", releasedBytes);
        ModelValidation.nonNegative("affectedTenantCount", affectedTenantCount);
        errors = List.copyOf(ModelValidation.required("errors", errors));
    }
}
