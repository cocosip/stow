package io.github.cocosip.stow.exception;

public final class TenantNotFoundException extends StowException {

    @java.io.Serial
    private static final long serialVersionUID = 1L;

    public TenantNotFoundException(String message) {
        super("STOW_TENANT_NOT_FOUND", message);
    }
}
