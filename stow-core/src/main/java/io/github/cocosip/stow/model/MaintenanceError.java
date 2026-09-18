package io.github.cocosip.stow.model;

public record MaintenanceError(String tenantId, String operation, String summary) {

    public MaintenanceError {
        if (tenantId != null) {
            tenantId = ModelValidation.identifier("tenantId", tenantId);
        }
        operation = ModelValidation.requiredText("operation", operation);
        summary = ModelValidation.errorSummary(ModelValidation.requiredText("summary", summary));
    }
}
