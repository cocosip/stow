package io.github.cocosip.stow.exception;

public final class TenantDisabledException extends StowException {

    public TenantDisabledException(String message) {
        super("STOW_TENANT_DISABLED", message);
    }
}
