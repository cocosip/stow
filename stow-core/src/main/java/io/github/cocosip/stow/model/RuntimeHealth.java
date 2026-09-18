package io.github.cocosip.stow.model;

import java.util.Map;

public record RuntimeHealth(HealthStatus status, Map<String, ComponentHealth> components) {

    public RuntimeHealth {
        ModelValidation.required("status", status);
        components = Map.copyOf(ModelValidation.required("components", components));
    }
}
