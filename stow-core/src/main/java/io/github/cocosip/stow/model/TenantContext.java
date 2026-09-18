package io.github.cocosip.stow.model;

import java.time.Instant;

public record TenantContext(String tenantId, TenantStatus status, Instant createdAt, Instant updatedAt) {

    public TenantContext {
        tenantId = ModelValidation.identifier("tenantId", tenantId);
        ModelValidation.required("status", status);
        ModelValidation.required("createdAt", createdAt);
        ModelValidation.required("updatedAt", updatedAt);
        if (updatedAt.isBefore(createdAt)) {
            throw ModelValidation.invalid("updatedAt", "must not precede createdAt");
        }
    }
}
