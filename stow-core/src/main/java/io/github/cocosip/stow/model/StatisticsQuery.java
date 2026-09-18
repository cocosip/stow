package io.github.cocosip.stow.model;

import java.time.Instant;

public record StatisticsQuery(
        Instant from, Instant to, String tenantId, String volumeId, String watcherId, String operation) {

    public StatisticsQuery {
        ModelValidation.required("from", from);
        ModelValidation.required("to", to);
        if (to.isBefore(from)) {
            throw ModelValidation.invalid("to", "must not precede from");
        }
        if (tenantId != null) {
            tenantId = ModelValidation.identifier("tenantId", tenantId);
        }
        if (volumeId != null) {
            volumeId = ModelValidation.identifier("volumeId", volumeId);
        }
        if (watcherId != null) {
            watcherId = ModelValidation.identifier("watcherId", watcherId);
        }
    }
}
