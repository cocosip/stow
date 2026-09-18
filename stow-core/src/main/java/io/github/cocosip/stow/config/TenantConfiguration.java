package io.github.cocosip.stow.config;

import java.util.List;

public record TenantConfiguration(boolean autoCreateTenants, long defaultQuota, List<String> preconfiguredTenants) {

    public TenantConfiguration {
        ConfigurationValidation.nonNegative("defaultQuota", defaultQuota);
        ConfigurationValidation.nonNull("preconfiguredTenants", preconfiguredTenants);
        preconfiguredTenants = List.copyOf(preconfiguredTenants);
    }
}
