package io.github.cocosip.stow.config;

public enum PermanentlyFailedDisposition {
    KEEP,
    MOVE_TO_DEAD_LETTER,
    DELETE,
    /**
     * Converges the metadata to {@code DEAD_LETTERED} and releases quota without
     * touching the physical file (Locus {@code PurgeMetadataOnly}); the residual file
     * is owned by orphan recovery on the volume where it lives.
     */
    PURGE_METADATA_ONLY
}
