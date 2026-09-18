package io.github.cocosip.stow.api;

import io.github.cocosip.stow.model.StatisticsQuery;
import io.github.cocosip.stow.model.StatisticsSnapshot;

public interface StatisticsReader {

    StatisticsSnapshot snapshot(StatisticsQuery query);
}
