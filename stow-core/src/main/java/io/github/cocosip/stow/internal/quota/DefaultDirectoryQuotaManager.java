package io.github.cocosip.stow.internal.quota;

import io.github.cocosip.stow.api.DirectoryQuotaManager;
import io.github.cocosip.stow.model.DirectoryQuota;
import java.util.Objects;
import java.util.function.Supplier;

public final class DefaultDirectoryQuotaManager implements DirectoryQuotaManager {

    private final SqliteQuotaRepository repository;
    private final QuotaOperationAdmission admission;

    public DefaultDirectoryQuotaManager(SqliteQuotaRepository repository) {
        this(repository, QuotaOperationAdmission.UNRESTRICTED);
    }

    public DefaultDirectoryQuotaManager(SqliteQuotaRepository repository, QuotaOperationAdmission admission) {
        this.repository = Objects.requireNonNull(repository, "repository");
        this.admission = Objects.requireNonNull(admission, "admission");
    }

    @Override
    public DirectoryQuota get(String tenantId, String logicalDirectory) {
        return admitted(() -> repository.directoryQuota(tenantId, normalize(tenantId, logicalDirectory)));
    }

    @Override
    public void setLimit(String tenantId, String logicalDirectory, long maxFiles) {
        admitted(() -> {
            repository.setDirectoryLimit(tenantId, normalize(tenantId, logicalDirectory), maxFiles);
            return null;
        });
    }

    private static String normalize(String tenantId, String logicalDirectory) {
        return new DirectoryQuota(tenantId, logicalDirectory, 0, 0, true).logicalDirectory();
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
