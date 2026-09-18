package io.github.cocosip.stow.internal.quota;

import io.github.cocosip.stow.api.TenantQuotaManager;
import java.util.Objects;
import java.util.function.Supplier;

public final class DefaultTenantQuotaManager implements TenantQuotaManager {

    private final SqliteQuotaRepository repository;
    private final QuotaOperationAdmission admission;

    public DefaultTenantQuotaManager(SqliteQuotaRepository repository) {
        this(repository, QuotaOperationAdmission.UNRESTRICTED);
    }

    public DefaultTenantQuotaManager(SqliteQuotaRepository repository, QuotaOperationAdmission admission) {
        this.repository = Objects.requireNonNull(repository, "repository");
        this.admission = Objects.requireNonNull(admission, "admission");
    }

    @Override
    public long currentCount(String tenantId) {
        return admitted(() -> repository.tenantCurrentCount(tenantId));
    }

    @Override
    public long limit(String tenantId) {
        return admitted(() -> repository.tenantLimit(tenantId));
    }

    @Override
    public void setLimit(String tenantId, long maxFiles) {
        admitted(() -> {
            repository.setTenantLimit(tenantId, maxFiles);
            return null;
        });
    }

    private <T> T admitted(Supplier<T> operation) {
        admission.enter();
        try {
            return operation.get();
        } finally {
            admission.exit();
        }
    }
}
