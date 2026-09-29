package io.github.cocosip.stow.exception;

public final class TenantQuotaExceededException extends StowException {

    @java.io.Serial
    private static final long serialVersionUID = 1L;

    private final String tenantId;
    private final long currentCount;
    private final long maxCount;

    public TenantQuotaExceededException(String message) {
        super("STOW_TENANT_QUOTA_EXCEEDED", message);
        this.tenantId = null;
        this.currentCount = -1;
        this.maxCount = -1;
    }

    public TenantQuotaExceededException(String tenantId, long currentCount, long maxCount) {
        super("STOW_TENANT_QUOTA_EXCEEDED", "Tenant quota exceeded: " + tenantId);
        this.tenantId = tenantId;
        this.currentCount = currentCount;
        this.maxCount = maxCount;
    }

    public String tenantId() {
        return tenantId;
    }

    public long currentCount() {
        return currentCount;
    }

    public long maxCount() {
        return maxCount;
    }
}
