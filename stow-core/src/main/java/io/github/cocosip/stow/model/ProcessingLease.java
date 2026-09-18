package io.github.cocosip.stow.model;

import java.time.Instant;
import java.util.UUID;

public record ProcessingLease(String tenantId, String fileKey, UUID leaseId, Instant startedAt) {

    public ProcessingLease {
        tenantId = ModelValidation.identifier("tenantId", tenantId);
        fileKey = ModelValidation.fileKey(fileKey);
        ModelValidation.required("leaseId", leaseId);
        ModelValidation.required("startedAt", startedAt);
    }
}
