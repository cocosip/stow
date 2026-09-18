package io.github.cocosip.stow.api;

public interface TenantQuotaManager {

    long currentCount(String tenantId);

    long limit(String tenantId);

    void setLimit(String tenantId, long maxFiles);
}
