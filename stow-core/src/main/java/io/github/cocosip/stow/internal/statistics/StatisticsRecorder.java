package io.github.cocosip.stow.internal.statistics;

import io.github.cocosip.stow.api.StatisticsReader;

public interface StatisticsRecorder extends StatisticsReader {

    boolean recordWrite(String tenantId, String volumeId, long bytes);

    boolean recordRead(String tenantId, String volumeId);

    boolean recordClaim(String tenantId);

    boolean recordCompleted(String tenantId);

    boolean recordSqlitePersistence(String tenantId);

    boolean recordWatcherImport(String watcherId, String tenantId, long bytes);
}
