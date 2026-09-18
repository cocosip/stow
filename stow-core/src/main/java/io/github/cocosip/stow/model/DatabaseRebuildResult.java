package io.github.cocosip.stow.model;

import java.time.Instant;
import java.util.List;

public record DatabaseRebuildResult(
        String tenantId,
        Instant startedAt,
        Instant finishedAt,
        long scannedCount,
        long rebuiltCount,
        long skippedCount,
        long failedCount,
        List<MaintenanceError> errors) {

    public DatabaseRebuildResult {
        tenantId = ModelValidation.identifier("tenantId", tenantId);
        ModelValidation.required("startedAt", startedAt);
        ModelValidation.required("finishedAt", finishedAt);
        if (finishedAt.isBefore(startedAt)) {
            throw ModelValidation.invalid("finishedAt", "must not precede startedAt");
        }
        ModelValidation.nonNegative("scannedCount", scannedCount);
        ModelValidation.nonNegative("rebuiltCount", rebuiltCount);
        ModelValidation.nonNegative("skippedCount", skippedCount);
        ModelValidation.nonNegative("failedCount", failedCount);
        errors = List.copyOf(ModelValidation.required("errors", errors));
    }
}
