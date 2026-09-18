package io.github.cocosip.stow.api;

import io.github.cocosip.stow.model.TenantContext;
import java.util.List;
import java.util.Optional;

public interface TenantManager {

    TenantContext get(String tenantId);

    Optional<TenantContext> find(String tenantId);

    List<TenantContext> list();

    TenantContext create(String tenantId);

    void enable(String tenantId);

    void disable(String tenantId);
}
