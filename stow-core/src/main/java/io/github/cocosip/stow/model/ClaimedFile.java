package io.github.cocosip.stow.model;

public record ClaimedFile(FileLocation location, ProcessingLease lease) {

    public ClaimedFile {
        ModelValidation.required("location", location);
        ModelValidation.required("lease", lease);
    }
}
