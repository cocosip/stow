package io.github.cocosip.stow.internal.quota;

import io.github.cocosip.stow.api.TenantQuotaManager;
import java.util.Objects;

public final class DefaultTenantQuotaManager implements TenantQuotaManager {

    private final SqliteQuotaRepository repository;

    public DefaultTenantQuotaManager(SqliteQuotaRepository repository) {
        this.repository = Objects.requireNonNull(repository, "repository");
    }

    @Override
    public long currentCount(String tenantId) {
        return repository.tenantCurrentCount(tenantId);
    }

    @Override
    public long limit(String tenantId) {
        return repository.tenantLimit(tenantId);
    }

    @Override
    public void setLimit(String tenantId, long maxFiles) {
        repository.setTenantLimit(tenantId, maxFiles);
    }
}
