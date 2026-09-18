package io.github.cocosip.stow.model;

public record DirectoryQuota(
        String tenantId, String logicalDirectory, long currentCount, long maxFiles, boolean enabled) {

    public DirectoryQuota {
        tenantId = ModelValidation.identifier("tenantId", tenantId);
        logicalDirectory = ModelValidation.logicalDirectory(logicalDirectory);
        ModelValidation.nonNegative("currentCount", currentCount);
        ModelValidation.nonNegative("maxFiles", maxFiles);
    }
}
