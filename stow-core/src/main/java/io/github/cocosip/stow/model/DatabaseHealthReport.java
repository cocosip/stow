package io.github.cocosip.stow.model;

import java.time.Instant;
import java.util.Map;

public record DatabaseHealthReport(HealthStatus status, Instant checkedAt, Map<String, ComponentHealth> databases) {

    public DatabaseHealthReport {
        ModelValidation.required("status", status);
        ModelValidation.required("checkedAt", checkedAt);
        databases = Map.copyOf(ModelValidation.required("databases", databases));
    }
}
