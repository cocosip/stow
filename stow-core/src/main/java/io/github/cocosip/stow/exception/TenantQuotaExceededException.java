package io.github.cocosip.stow.exception;

public final class TenantQuotaExceededException extends StowException {

    public TenantQuotaExceededException(String message) {
        super("STOW_TENANT_QUOTA_EXCEEDED", message);
    }
}
