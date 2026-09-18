package io.github.cocosip.stow.api;

import io.github.cocosip.stow.model.ProjectionTenantState;

public interface QueueProjectionMaintenance {

    ProjectionTenantState state(String tenantId);

    ProjectionTenantState replay(String tenantId);

    ProjectionTenantState snapshot(String tenantId);

    ProjectionTenantState rebuild(String tenantId);
}
