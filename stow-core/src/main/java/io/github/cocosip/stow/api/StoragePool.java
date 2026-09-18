package io.github.cocosip.stow.api;

import io.github.cocosip.stow.model.ClaimedFile;
import io.github.cocosip.stow.model.FileLocation;
import io.github.cocosip.stow.model.FileProcessingStatus;
import io.github.cocosip.stow.model.ProcessingLease;
import io.github.cocosip.stow.model.StoredFileInfo;
import io.github.cocosip.stow.model.TenantContext;
import io.github.cocosip.stow.model.WriteOptions;
import java.io.InputStream;
import java.util.List;
import java.util.Optional;

public interface StoragePool {

    String write(TenantContext tenant, InputStream content, WriteOptions options);

    String write(TenantContext tenant, ContentSource content, WriteOptions options);

    InputStream read(TenantContext tenant, String fileKey);

    Optional<StoredFileInfo> findFileInfo(TenantContext tenant, String fileKey);

    Optional<FileLocation> findFileLocation(TenantContext tenant, String fileKey);

    Optional<ClaimedFile> claimNext(TenantContext tenant);

    List<ClaimedFile> claimBatch(TenantContext tenant, int batchSize);

    void complete(ProcessingLease lease);

    void fail(ProcessingLease lease, String errorMessage);

    FileProcessingStatus status(TenantContext tenant, String fileKey);

    long totalCapacity();

    long availableCapacity();
}
