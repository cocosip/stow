package io.github.cocosip.stow.internal.quota;

import java.util.Objects;

public record QuotaReservation(String reservationId, String fileKey, String logicalDirectory) {

    public QuotaReservation {
        if (reservationId == null || reservationId.isBlank()) {
            throw new IllegalArgumentException("reservationId must not be blank");
        }
        if (fileKey == null || fileKey.isBlank()) {
            throw new IllegalArgumentException("fileKey must not be blank");
        }
        Objects.requireNonNull(logicalDirectory, "logicalDirectory");
    }
}
