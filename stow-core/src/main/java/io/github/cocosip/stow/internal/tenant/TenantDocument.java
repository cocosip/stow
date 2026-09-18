package io.github.cocosip.stow.internal.tenant;

import io.github.cocosip.stow.model.TenantContext;
import io.github.cocosip.stow.model.TenantStatus;
import java.time.Instant;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public record TenantDocument(int schemaVersion, List<TenantEntry> tenants) {

    static final int CURRENT_SCHEMA_VERSION = 1;

    public TenantDocument {
        if (schemaVersion != CURRENT_SCHEMA_VERSION) {
            throw new IllegalArgumentException("Unsupported tenant document schema version");
        }
        tenants = List.copyOf(tenants);
        Set<String> tenantIds = new HashSet<>();
        for (TenantEntry tenant : tenants) {
            if (!tenantIds.add(tenant.tenantId())) {
                throw new IllegalArgumentException("Duplicate tenant ID " + tenant.tenantId());
            }
        }
    }

    static TenantDocument empty() {
        return new TenantDocument(CURRENT_SCHEMA_VERSION, List.of());
    }

    public record TenantEntry(
            String tenantId, TenantStatus status, Instant createdAt, Instant updatedAt, long maxFiles) {

        public TenantEntry {
            TenantContext validated = new TenantContext(tenantId, status, createdAt, updatedAt);
            tenantId = validated.tenantId();
            if (maxFiles < 0) {
                throw new IllegalArgumentException("maxFiles must not be negative");
            }
        }

        TenantContext toContext() {
            return new TenantContext(tenantId, status, createdAt, updatedAt);
        }

        TenantEntry withStatus(TenantStatus newStatus, Instant changedAt) {
            return new TenantEntry(tenantId, newStatus, createdAt, changedAt, maxFiles);
        }
    }
}
