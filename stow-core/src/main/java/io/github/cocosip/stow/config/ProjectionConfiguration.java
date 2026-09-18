package io.github.cocosip.stow.config;

import java.time.Duration;

public record ProjectionConfiguration(
        int maxRecordsPerTenantCycle,
        int maxTenantsPerCycle,
        Duration busyCycleDelay,
        Duration idleCycleDelay,
        Duration cycleTimeBudget) {

    public ProjectionConfiguration {
        ConfigurationValidation.positive("projection.maxRecordsPerTenantCycle", maxRecordsPerTenantCycle);
        ConfigurationValidation.positive("projection.maxTenantsPerCycle", maxTenantsPerCycle);
        ConfigurationValidation.nonNegative("projection.busyCycleDelay", busyCycleDelay);
        ConfigurationValidation.nonNegative("projection.idleCycleDelay", idleCycleDelay);
        ConfigurationValidation.positive("projection.cycleTimeBudget", cycleTimeBudget);
    }
}
