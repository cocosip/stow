package io.github.cocosip.stow.internal.quota;

import io.github.cocosip.stow.api.DirectoryQuotaManager;
import io.github.cocosip.stow.model.DirectoryQuota;
import java.util.Objects;

public final class DefaultDirectoryQuotaManager implements DirectoryQuotaManager {

    private final SqliteQuotaRepository repository;

    public DefaultDirectoryQuotaManager(SqliteQuotaRepository repository) {
        this.repository = Objects.requireNonNull(repository, "repository");
    }

    @Override
    public DirectoryQuota get(String tenantId, String logicalDirectory) {
        return repository.directoryQuota(tenantId, normalize(tenantId, logicalDirectory));
    }

    @Override
    public void setLimit(String tenantId, String logicalDirectory, long maxFiles) {
        repository.setDirectoryLimit(tenantId, normalize(tenantId, logicalDirectory), maxFiles);
    }

    private static String normalize(String tenantId, String logicalDirectory) {
        return new DirectoryQuota(tenantId, logicalDirectory, 0, 0, true).logicalDirectory();
    }
}
