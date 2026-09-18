package io.github.cocosip.stow.model;

import java.time.Instant;

public record ProjectionTenantState(
        String tenantId,
        long baseOffset,
        long tailOffset,
        long projectedOffset,
        long lastSequence,
        Instant snapshotAt,
        HealthStatus status) {

    public ProjectionTenantState {
        tenantId = ModelValidation.identifier("tenantId", tenantId);
        ModelValidation.nonNegative("baseOffset", baseOffset);
        ModelValidation.nonNegative("tailOffset", tailOffset);
        ModelValidation.nonNegative("projectedOffset", projectedOffset);
        ModelValidation.nonNegative("lastSequence", lastSequence);
        ModelValidation.required("status", status);
        if (tailOffset < baseOffset) {
            throw ModelValidation.invalid("tailOffset", "must not precede baseOffset");
        }
        if (projectedOffset < baseOffset || projectedOffset > tailOffset) {
            throw ModelValidation.invalid("projectedOffset", "must be between baseOffset and tailOffset");
        }
    }
}
