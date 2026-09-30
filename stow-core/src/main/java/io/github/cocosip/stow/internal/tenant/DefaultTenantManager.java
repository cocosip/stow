package io.github.cocosip.stow.internal.tenant;

import io.github.cocosip.stow.api.TenantManager;
import io.github.cocosip.stow.exception.TenantNotFoundException;
import io.github.cocosip.stow.internal.tenant.TenantDocument.TenantEntry;
import io.github.cocosip.stow.model.TenantContext;
import io.github.cocosip.stow.model.TenantStatus;
import java.nio.file.Path;
import java.time.Clock;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;

public final class DefaultTenantManager implements TenantManager {

    private final JsonTenantRepository repository;
    private final Clock clock;
    private final boolean autoCreateTenants;
    private final long defaultQuota;
    // Tenant directories provisioned under these roots when a tenant is created
    // (metadata, quota, queue, and volume mounts), mirroring Locus storage-path
    // provisioning; empty when the runtime has not attached roots yet.
    private final java.util.Set<Path> provisioningRoots = new java.util.HashSet<>();

    public DefaultTenantManager(
            JsonTenantRepository repository, Clock clock, boolean autoCreateTenants, long defaultQuota) {
        if (defaultQuota < 0) {
            throw new IllegalArgumentException("defaultQuota must not be negative");
        }
        this.repository = repository;
        this.clock = clock;
        this.autoCreateTenants = autoCreateTenants;
        this.defaultQuota = defaultQuota;
    }

    @Override
    public TenantContext get(String tenantId) {
        return find(tenantId).orElseGet(() -> {
            if (autoCreateTenants) {
                return create(tenantId);
            }
            throw new TenantNotFoundException("Tenant not found: " + tenantId);
        });
    }

    @Override
    public Optional<TenantContext> find(String tenantId) {
        validateTenantId(tenantId);
        return repository.read().tenants().stream()
                .filter(tenant -> tenant.tenantId().equals(tenantId))
                .findFirst()
                .map(TenantEntry::toContext);
    }

    @Override
    public List<TenantContext> list() {
        return repository.read().tenants().stream()
                .map(TenantEntry::toContext)
                .sorted(Comparator.comparing(TenantContext::tenantId))
                .toList();
    }

    @Override
    public TenantContext create(String tenantId) {
        validateTenantId(tenantId);
        boolean[] created = {false};
        TenantDocument updated = repository.update(document -> {
            Optional<TenantEntry> existing = document.tenants().stream()
                    .filter(tenant -> tenant.tenantId().equals(tenantId))
                    .findFirst();
            if (existing.isPresent()) {
                return document;
            }
            created[0] = true;
            Instant now = clock.instant();
            List<TenantEntry> tenants = new ArrayList<>(document.tenants());
            tenants.add(new TenantEntry(tenantId, TenantStatus.ENABLED, now, now, defaultQuota));
            tenants.sort(Comparator.comparing(TenantEntry::tenantId));
            return new TenantDocument(TenantDocument.CURRENT_SCHEMA_VERSION, tenants);
        });
        if (created[0]) {
            provisionStoragePaths(tenantId);
        }
        return updated.tenants().stream()
                .filter(tenant -> tenant.tenantId().equals(tenantId))
                .findFirst()
                .orElseThrow()
                .toContext();
    }

    /** Attaches runtime roots (metadata, quota, queue, volume mounts) for provisioning. */
    public void provisionUnder(List<Path> roots) {
        for (Path root : roots) {
            if (root != null) provisioningRoots.add(root.toAbsolutePath().normalize());
        }
        // Tenants that already exist (preconfigured or restored) get their directories too.
        for (TenantContext tenant : list()) {
            provisionStoragePaths(tenant.tenantId());
        }
    }

    private void provisionStoragePaths(String tenantId) {
        for (Path root : provisioningRoots) {
            try {
                java.nio.file.Files.createDirectories(root.resolve(tenantId));
            } catch (java.io.IOException exception) {
                throw new IllegalStateException("Unable to provision storage path for tenant " + tenantId, exception);
            }
        }
    }

    public long quotaLimit(String tenantId) {
        get(tenantId);
        return repository.read().tenants().stream()
                .filter(tenant -> tenant.tenantId().equals(tenantId))
                .findFirst()
                .orElseThrow()
                .maxFiles();
    }

    @Override
    public void enable(String tenantId) {
        updateStatus(tenantId, TenantStatus.ENABLED);
    }

    @Override
    public void disable(String tenantId) {
        updateStatus(tenantId, TenantStatus.DISABLED);
    }

    private void updateStatus(String tenantId, TenantStatus status) {
        validateTenantId(tenantId);
        repository.update(document -> {
            boolean found = false;
            List<TenantEntry> tenants = new ArrayList<>(document.tenants().size());
            for (TenantEntry tenant : document.tenants()) {
                if (tenant.tenantId().equals(tenantId)) {
                    found = true;
                    tenants.add(tenant.status() == status ? tenant : tenant.withStatus(status, clock.instant()));
                } else {
                    tenants.add(tenant);
                }
            }
            if (!found) {
                throw new TenantNotFoundException("Tenant not found: " + tenantId);
            }
            return new TenantDocument(TenantDocument.CURRENT_SCHEMA_VERSION, tenants);
        });
    }

    private static void validateTenantId(String tenantId) {
        Instant instant = Instant.EPOCH;
        new TenantContext(tenantId, TenantStatus.ENABLED, instant, instant);
    }
}
