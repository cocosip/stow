package io.github.cocosip.stow.model;

import java.time.Instant;

public record ComponentHealth(HealthStatus status, String summary, Instant checkedAt) {

    public ComponentHealth {
        ModelValidation.required("status", status);
        summary = ModelValidation.errorSummary(ModelValidation.requiredText("summary", summary));
        ModelValidation.required("checkedAt", checkedAt);
    }
}
