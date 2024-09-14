/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.spector.controller.impl;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.checkpoint.CheckpointCoordinator;
import org.apache.flink.runtime.executiongraph.ExecutionGraph;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.spector.controller.ReconfigExecutor;
import org.apache.flink.runtime.spector.controller.StateMigrationPlanner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.flink.runtime.spector.SpectorOptions.*;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Set the plan for the state migration according to the Configurations
 */
public class StateMigrationPlannerImpl implements StateMigrationPlanner {
	private static final Logger LOG = LoggerFactory.getLogger(StateMigrationPlanner.class);


	private final Configuration configuration;
	private final JobVertexID jobVertexID;

	private int numOpenedSubtask;

	private Map<String, List<String>> oldExecutorMapping;

	private final ReconfigExecutor reconfigExecutor;

	private volatile boolean waitForMigrationDeployed;

	private int syncKeys;

	private String orderFunction;

	private int replicationFilter;

	private final ExecutionGraph executionGraph;



	public StateMigrationPlannerImpl(Configuration configuration,
									 JobVertexID jobVertexID,
									 int parallelism,
									 Map<String, List<String>> executorMapping,
									 ReconfigExecutor reconfigExecutor,
									 ExecutionGraph executionGraph) {
		this.configuration = configuration;

//		int numAffectedKeys = configuration.getInteger(NUM_AFFECTED_KEYS);
		this.syncKeys = configuration.getInteger(SYNC_KEYS) == 0 ?
			Integer.MAX_VALUE / 2 : configuration.getInteger(SYNC_KEYS);

		this.orderFunction = configuration.getString(ORDER_FUNCTION);

		this.replicationFilter = configuration.getInteger(REPLICATE_KEYS_FILTER);

		this.jobVertexID = jobVertexID;
		this.numOpenedSubtask = parallelism;

		// Deep copy
		this.oldExecutorMapping = new HashMap<>();
		for (String taskId : executorMapping.keySet()) {
			oldExecutorMapping.put(taskId, new ArrayList<>(executorMapping.get(taskId)));
		}

		this.reconfigExecutor = reconfigExecutor;

		this.executionGraph = executionGraph;
	}

	public void changePlan(int syncKeys, int replicationFilter, String orderFunction) {
		// Wait until the reconfiguration is completed
		while (reconfigExecutor.checkReplicationProgress());
		this.syncKeys = syncKeys;
		this.orderFunction = orderFunction;
		if (this.replicationFilter != replicationFilter) {
			this.replicationFilter = replicationFilter;

			// Stop checkpoint coordinator then start the reconfiguration
			LOG.info("++++++ Stop Checkpoint Coordinator and start to make finalPlan");
			CheckpointCoordinator checkpointCoordinator = executionGraph.getCheckpointCoordinator();
			checkNotNull(checkpointCoordinator);
			checkpointCoordinator.stopCheckpointScheduler();

			reconfigExecutor.updateBackupKeyGroups(replicationFilter);

			LOG.info("++++++ Resume Checkpoint Coordinator.");
			if (checkpointCoordinator.isPeriodicCheckpointingConfigured()) {
				checkpointCoordinator.startCheckpointScheduler();
			}
		}
	}

	@Override
	public void remap(Map<String, List<String>> executorMapping) {
		makePlan(executorMapping);
	}

	@Override
	public void scale(int parallelism, Map<String, List<String>> executorMapping) {
		makePlan(executorMapping);
	}

	public void makePlan(Map<String, List<String>> executorMapping) {
		// Stop checkpoint coordinator then start the reconfiguration
		LOG.info("++++++ Stop Checkpoint Coordinator and start to make finalPlan");
		CheckpointCoordinator checkpointCoordinator = executionGraph.getCheckpointCoordinator();
		checkNotNull(checkpointCoordinator);
		checkpointCoordinator.stopCheckpointScheduler();

		while (reconfigExecutor.checkReplicationProgress());

		int newParallelism = executorMapping.keySet().size();
		// find out the affected keys.
		// order the migrating keys
		// return the state migration finalPlan to reconfig executor
		Map<String, Map<String, Tuple2<String, String>>> affectedKeys = getAffectedKeys(executorMapping);

		Map<String, Map<String, Tuple2<String, String>>> prioritizedKeySequences = prioritizeKeys(affectedKeys);
		Map<String, List<Map<String, Tuple2<String, String>>>> taskPlans = batching(prioritizedKeySequences);
		List<Map<String, Tuple2<String, String>>> finalPlan = merging(taskPlans);

		if (numOpenedSubtask >= newParallelism) {
			// repartition
			for (Map<String, Tuple2<String, String>> batchedAffectedKeys : finalPlan) {
				Map<String, List<String>> fluidExecutorMapping = deepCopy(oldExecutorMapping);
				for (String affectedKey : batchedAffectedKeys.keySet()) {
					Tuple2<String, String> srcToDst = batchedAffectedKeys.get(affectedKey);

					fluidExecutorMapping.get(srcToDst.f0).remove(affectedKey);
					fluidExecutorMapping.get(srcToDst.f1).add(affectedKey);
				}
				// trigger reconfig Executor rescale
				triggerAction(
					"trigger 1 fluid repartition",
					() -> reconfigExecutor.remap(fluidExecutorMapping),
					fluidExecutorMapping);
			}
		} else {
			// scale out
			throw new UnsupportedOperationException();
		}

		LOG.info("++++++ Resume Checkpoint Coordinator.");
		if (checkpointCoordinator.isPeriodicCheckpointingConfigured()) {
			checkpointCoordinator.startCheckpointScheduler();
		}
	}

	private List<Map<String, Tuple2<String, String>>> merging(Map<String, List<Map<String, Tuple2<String, String>>>> taskPlans) {
		List<Map<String, Tuple2<String, String>>> plan = new ArrayList<>();

		AtomicInteger maxLength = new AtomicInteger();
		taskPlans.values().forEach(taskPlan -> {
			if (taskPlan.size() > maxLength.get()) {
				maxLength.set(taskPlan.size());
			}
		});

		for (int i = 0; i < maxLength.get(); i++) {
			Map<String, Tuple2<String, String>> curBatch = new HashMap<>();
			int finalI = i;
			taskPlans.values().forEach(taskPlan -> {
				if (finalI < taskPlan.size()) {
					Map<String, Tuple2<String, String>> batch = taskPlan.get(finalI);
					curBatch.putAll(batch);
				}
			});
			plan.add(curBatch);
		}

		return plan;
	}

	private Map<String, List<String>> deepCopy(Map<String, List<String>> executorMapping) {
		Map<String, List<String>> fluidExecutorMapping = new HashMap<>();
		for (String taskId : executorMapping.keySet()) {
			fluidExecutorMapping.put(taskId, new ArrayList<>(executorMapping.get(taskId)));
		}
		return fluidExecutorMapping;
	}

	private Map<String, Map<String, Tuple2<String, String>>> getAffectedKeys(Map<String, List<String>> executorMapping) {
		// Key -> <SrcTaskId, DstTaskId>, can be used to set new ExecutorMapping
//		Map<String, Tuple2<String, String>> affectedKeys = new HashMap<>();
		Map<String, Map<String, Tuple2<String, String>>> affectedKeys = new HashMap<>();

		// Key -> Task
		Map<String, String> oldKeysToExecutorMapping = new HashMap<>();
		oldExecutorMapping.keySet().forEach(oldTaskId -> {
			for (String key : oldExecutorMapping.get(oldTaskId)) {
				oldKeysToExecutorMapping.put(key, oldTaskId);
			}
		});

		executorMapping.keySet().forEach(newTaskId -> {
			for (String key : executorMapping.get(newTaskId)) {
				affectedKeys.putIfAbsent(oldKeysToExecutorMapping.get(key), new HashMap<>());
				// check whether the keys is migrated from old task to new task
				if (!oldKeysToExecutorMapping.get(key).equals(newTaskId)) {
					affectedKeys.get(oldKeysToExecutorMapping.get(key)).put(key, Tuple2.of(oldKeysToExecutorMapping.get(key), newTaskId));
				}
			}
		});
		return affectedKeys;
	}

	private Map<String, List<Map<String, Tuple2<String, String>>>> batching(Map<String, Map<String, Tuple2<String, String>>> prioritizedKeySequences) {
		Map<String, List<Map<String, Tuple2<String, String>>>> plans = new HashMap<>();
		prioritizedKeySequences.keySet().forEach(taskId -> plans.putIfAbsent(taskId, new ArrayList<>()));

		prioritizedKeySequences.keySet().forEach(taskId -> {
			int count = 0;
			Map<String, Tuple2<String, String>> batchedAffectedKeys = new HashMap<>();

			for (String affectedKey : prioritizedKeySequences.get(taskId).keySet()) {
				batchedAffectedKeys.put(affectedKey, prioritizedKeySequences.get(taskId).get(affectedKey));
				count++;
				if (count % syncKeys == 0) {
					plans.get(taskId).add(batchedAffectedKeys);
					batchedAffectedKeys= new HashMap<>();
				}
			}

			if (!batchedAffectedKeys.isEmpty()) {
				plans.get(taskId).add(batchedAffectedKeys);
			}
		});


		return plans;
	}

	public Map<String, Map<String, Tuple2<String, String>>> prioritizeKeys(Map<String, Map<String, Tuple2<String, String>>> affectedKeys) {
		Map<String, Map<String, Tuple2<String, String>>> prioritizedKeySequences = new HashMap<>();
		affectedKeys.keySet().forEach(taskId -> {
			prioritizedKeySequences.putIfAbsent(taskId, new TreeMap<>(Comparator.comparing(Integer::valueOf)));
			prioritizedKeySequences.get(taskId).putAll(affectedKeys.get(taskId));
		});

		switch (orderFunction) {
			case "default":
				// Option 1:
				return prioritizedKeySequences;
			case "reverse":
				// option 2:
				prioritizedKeySequences.keySet().forEach(taskId -> {
					NavigableMap batch = ((TreeMap<?, ?>) prioritizedKeySequences.get(taskId)).descendingMap();
					prioritizedKeySequences.put(taskId, batch);
				});
				return prioritizedKeySequences;
			case "random":
				Random random = new Random(12345678);
				prioritizedKeySequences.keySet().forEach(taskId -> {
					List<String> list = new ArrayList<>(prioritizedKeySequences.get(taskId).keySet());
					Collections.shuffle(list, random);
					Map<String, Tuple2<String, String>> shuffleMap = new LinkedHashMap<>();
					list.forEach(k -> shuffleMap.put(k, prioritizedKeySequences.get(taskId).get(k)));
					prioritizedKeySequences.put(taskId, shuffleMap);
				});
				return prioritizedKeySequences;
			default:
				throw new UnsupportedOperationException();
		}
	}

	private void triggerAction(String logStr, Runnable runnable, Map<String, List<String>> partitionAssignment) {
		LOG.info("------ " + logStr + "   partitionAssignment: " + partitionAssignment);
		long start = System.currentTimeMillis();
		waitForMigrationDeployed = true;

		runnable.run();

		while (waitForMigrationDeployed);
		oldExecutorMapping = partitionAssignment;
		LOG.info("++++++ reconfig completion time: " + (System.currentTimeMillis() - start));
	}

	public void onMigrationCompleted() {
		waitForMigrationDeployed = false;
	}
}
