package io.github.cocosip.stow.api;

import io.github.cocosip.stow.model.DirectoryQuota;

public interface DirectoryQuotaManager {

    DirectoryQuota get(String tenantId, String logicalDirectory);

    void setLimit(String tenantId, String logicalDirectory, long maxFiles);
}
