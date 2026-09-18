package io.github.cocosip.stow.model;

import java.time.Instant;
import java.util.List;

public record WatcherScanResult(
        String watcherId,
        Instant startedAt,
        Instant finishedAt,
        long discoveredCount,
        long importedCount,
        long skippedCount,
        long failedCount,
        long importedBytes,
        List<MaintenanceError> errors) {

    public WatcherScanResult {
        watcherId = ModelValidation.identifier("watcherId", watcherId);
        ModelValidation.required("startedAt", startedAt);
        ModelValidation.required("finishedAt", finishedAt);
        if (finishedAt.isBefore(startedAt)) {
            throw ModelValidation.invalid("finishedAt", "must not precede startedAt");
        }
        ModelValidation.nonNegative("discoveredCount", discoveredCount);
        ModelValidation.nonNegative("importedCount", importedCount);
        ModelValidation.nonNegative("skippedCount", skippedCount);
        ModelValidation.nonNegative("failedCount", failedCount);
        ModelValidation.nonNegative("importedBytes", importedBytes);
        errors = List.copyOf(ModelValidation.required("errors", errors));
    }
}
