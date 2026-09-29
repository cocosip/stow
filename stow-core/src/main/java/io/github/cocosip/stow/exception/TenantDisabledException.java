package io.github.cocosip.stow.exception;

public final class TenantDisabledException extends StowException {

    @java.io.Serial
    private static final long serialVersionUID = 1L;

    public TenantDisabledException(String message) {
        super("STOW_TENANT_DISABLED", message);
    }
}
