package org.apache.flink.runtime.spector.controller;

import java.util.List;
import java.util.Map;

public interface ReconfigExecutor {

	void remap(Map<String, List<String>> executorMapping);

	void scale(int parallelism, Map<String, List<String>> executorMapping);

	boolean checkReplicationProgress();

	void updateBackupKeyGroups(int filter);
}
