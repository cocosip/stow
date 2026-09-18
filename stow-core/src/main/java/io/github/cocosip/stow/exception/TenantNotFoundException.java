package io.github.cocosip.stow.exception;

public final class TenantNotFoundException extends StowException {

    public TenantNotFoundException(String message) {
        super("STOW_TENANT_NOT_FOUND", message);
    }
}
