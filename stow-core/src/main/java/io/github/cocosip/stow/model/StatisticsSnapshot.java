package io.github.cocosip.stow.model;

import java.time.Instant;
import java.util.Map;

public record StatisticsSnapshot(
        Instant from,
        Instant to,
        long writtenFileCount,
        long writtenBytes,
        double writeMibPerSecond,
        long readCount,
        long claimCount,
        long completedCount,
        long sqlitePersistenceOperationCount,
        long watcherImportedCount,
        long watcherImportedBytes,
        Map<String, Long> series) {

    public StatisticsSnapshot {
        ModelValidation.required("from", from);
        ModelValidation.required("to", to);
        if (to.isBefore(from)) {
            throw ModelValidation.invalid("to", "must not precede from");
        }
        ModelValidation.nonNegative("writtenFileCount", writtenFileCount);
        ModelValidation.nonNegative("writtenBytes", writtenBytes);
        if (!Double.isFinite(writeMibPerSecond) || writeMibPerSecond < 0) {
            throw ModelValidation.invalid("writeMibPerSecond", "must be finite and non-negative");
        }
        ModelValidation.nonNegative("readCount", readCount);
        ModelValidation.nonNegative("claimCount", claimCount);
        ModelValidation.nonNegative("completedCount", completedCount);
        ModelValidation.nonNegative("sqlitePersistenceOperationCount", sqlitePersistenceOperationCount);
        ModelValidation.nonNegative("watcherImportedCount", watcherImportedCount);
        ModelValidation.nonNegative("watcherImportedBytes", watcherImportedBytes);
        series = Map.copyOf(ModelValidation.required("series", series));
    }
}
