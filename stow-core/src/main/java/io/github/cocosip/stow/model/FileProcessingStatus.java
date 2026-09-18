package io.github.cocosip.stow.model;

public enum FileProcessingStatus {
    PENDING,
    PROCESSING,
    COMPLETED,
    FAILED,
    PERMANENTLY_FAILED,
    DELETE_REQUESTED,
    DELETE_SUCCEEDED,
    DEAD_LETTERED
}
