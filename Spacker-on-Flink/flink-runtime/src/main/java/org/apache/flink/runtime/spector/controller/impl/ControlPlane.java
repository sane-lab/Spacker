package org.apache.flink.runtime.spector.controller.impl;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.executiongraph.ExecutionGraph;
import org.apache.flink.runtime.executiongraph.ExecutionJobVertex;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.spector.migration.JobExecutionPlan;
import org.apache.flink.runtime.spector.migration.JobReconfigActor;
import org.apache.flink.runtime.spector.controller.OperatorController;
import org.apache.flink.runtime.spector.controller.ReconfigExecutor;
import org.apache.flink.runtime.spector.controller.StateMigrationPlanner;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.KeyGroupRangeAssignment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

import static org.apache.flink.runtime.spector.SpectorOptions.RECONFIG_START_TIME;
import static org.apache.flink.runtime.spector.SpectorOptions.TARGET_OPERATORS;

public class ControlPlane {

	private static final Logger LOG = LoggerFactory.getLogger(ControlPlane.class);

	private final JobReconfigActor jobReconfigActor;

	private final Map<JobVertexID, OperatorController> controllers;

	public ControlPlane(JobReconfigActor jobReconfigActor, ExecutionGraph executionGraph) {

		this.jobReconfigActor = jobReconfigActor;

		this.controllers = new HashMap<>(executionGraph.getAllVertices().size());



		Configuration configuration = executionGraph.getJobConfiguration();


		String targetOperatorsStr = configuration.getString(TARGET_OPERATORS);
		String reconfigStartStr = configuration.getString(RECONFIG_START_TIME);

		List<String> targetOperatorsList = Arrays.asList(targetOperatorsStr.split(","));
		List<String> reconfigStartList = Arrays.asList(reconfigStartStr.split(","));
		Map<String, Integer> targetOperators = new HashMap<>(targetOperatorsList.size());
		for (int i = 0; i < targetOperatorsList.size(); i++) {
			targetOperators.put(targetOperatorsList.get(i), Integer.valueOf(reconfigStartList.get(i)));
		}

		for (Map.Entry<JobVertexID, ExecutionJobVertex> entry : executionGraph.getAllVertices().entrySet()) {
			JobVertexID vertexID = entry.getKey();
			int parallelism = entry.getValue().getParallelism();
			int maxParallelism = entry.getValue().getMaxParallelism();

			String operatorName = entry.getValue().getName();
			if (targetOperators.containsKey(operatorName)) {
				Map<String, List<String>> executorMapping = generateExecutorMapping(parallelism, maxParallelism);

				ReconfigExecutor reconfigExecutor = new ReconfigExecutorImpl(
					vertexID,
					parallelism,
					executorMapping);

				StateMigrationPlanner stateMigrationPlanner = new StateMigrationPlannerImpl(
					configuration,
					vertexID,
					parallelism,
					executorMapping,
					reconfigExecutor,
					jobReconfigActor.getExecutionGraph());

				OperatorController controller = new DummyController(
					configuration,
					operatorName,
					targetOperators.get(operatorName),
					stateMigrationPlanner,
					executorMapping,
					maxParallelism);

				controller.initMetrics(jobReconfigActor.getJobGraph(), vertexID, configuration, parallelism);
				this.controllers.put(vertexID, controller);
			}
		}
	}

	public static Map<String, List<String>> generateExecutorMapping(int parallelism, int maxParallelism) {
		Map<String, List<String>> executorMapping = new HashMap<>();

		int numExecutors = generateExecutorDelegates(parallelism).size();
		int numPartitions = generateFinestPartitionDelegates(maxParallelism).size();
		for (int executorId = 0; executorId < numExecutors; executorId++) {
			List<String> executorPartitions = new ArrayList<>();
			executorMapping.put(String.valueOf(executorId), executorPartitions);

			KeyGroupRange keyGroupRange = KeyGroupRangeAssignment.computeKeyGroupRangeForOperatorIndex(
				numPartitions, numExecutors, executorId);
			for (int i = keyGroupRange.getStartKeyGroup(); i <= keyGroupRange.getEndKeyGroup(); i++) {
				executorPartitions.add(String.valueOf(i));
			}
		}
		return executorMapping;
	}

	public void startControllers() {
		for (OperatorController controller : controllers.values()) {
			controller.start();
		}
	}

	public void stopControllers() {
		for (OperatorController controller : controllers.values()) {
			controller.stopGracefully();
		}
	}

	public void onChangeImplemented(JobVertexID jobVertexID) {
		LOG.info("++++++ onChangeImplemented triggered for jobVertex " + jobVertexID);
		this.controllers.get(jobVertexID).getStateMigrationPlanner().onMigrationCompleted();
	}

	public void onForceRetrieveMetrics(JobVertexID jobVertexID) {
		LOG.info("++++++ onForceRetrieveMetrics triggered for jobVertex " + jobVertexID);
		this.controllers.get(jobVertexID).onForceRetrieveMetrics();
	}

	public void onMigrationExecutorsStopped(JobVertexID jobVertexID) {
		LOG.info("++++++ onMigrationExecutorsStopped triggered for jobVertex " + jobVertexID);
		this.controllers.get(jobVertexID).onMigrationExecutorsStopped();
	}

	private static List<String> generateExecutorDelegates(int parallelism) {
		List<String> executors = new ArrayList<>();
		for (int i = 0; i < parallelism; i++) {
			executors.add(String.valueOf(i));
		}
		return executors;
	}

	private static List<String> generateFinestPartitionDelegates(int maxParallelism) {
		List<String> finestPartitions = new ArrayList<>();
		for (int i = 0; i < maxParallelism; i++) {
			finestPartitions.add(String.valueOf(i));
		}
		return finestPartitions;
	}

	public class ReconfigExecutorImpl implements ReconfigExecutor {

		private final JobVertexID jobVertexID;

		private int numOpenedSubtask;

		private JobExecutionPlan oldExecutionPlan;

		private Map<String, List<String>> oldExecutorMapping;


		public ReconfigExecutorImpl(JobVertexID jobVertexID, int parallelism, Map<String, List<String>> executorMapping) {
			this.jobVertexID = jobVertexID;
			this.numOpenedSubtask = parallelism;

			this.oldExecutionPlan = new JobExecutionPlan(jobVertexID, executorMapping, numOpenedSubtask);
			// Deep copy
			this.oldExecutorMapping = deepCopy(executorMapping);
			jobReconfigActor.setInitialJobExecutionPlan(jobVertexID, oldExecutionPlan);
		}

		@Override
		public void remap(Map<String, List<String>> executorMapping) {
			handleTreatment(executorMapping);
		}

		@Override
		public void scale(int newParallelism, Map<String, List<String>> executorMapping) {
			handleTreatment(executorMapping);
		}

		@Override
		public boolean checkReplicationProgress() {
			return jobReconfigActor.checkReplicationProgress();
		}

		@Override
		public void updateBackupKeyGroups(int filter) {
			jobReconfigActor.updateBackupKeyGroups(filter);
		}

		private void handleTreatment(Map<String, List<String>> executorMapping) {
			int newParallelism = executorMapping.keySet().size();

			JobExecutionPlan jobExecutionPlan;

			if (numOpenedSubtask >= newParallelism) {
				// repartition
				jobExecutionPlan = new JobExecutionPlan(
					jobVertexID, executorMapping, oldExecutorMapping, oldExecutionPlan, numOpenedSubtask);

				// if there are multiple reconfig triggers, only one of them should be happened first.
				synchronized (jobReconfigActor) {
					try {
						jobReconfigActor.repartition(jobVertexID, jobExecutionPlan);
					} catch (InterruptedException e) {
						throw new RuntimeException(e);
					}
				}
			} else {
				// scale out
				jobExecutionPlan = new JobExecutionPlan(
					jobVertexID, executorMapping, oldExecutorMapping, oldExecutionPlan, newParallelism);
//				synchronized (rescaleAction) {
//					rescaleAction.scaleOut(jobVertexID, newParallelism, jobExecutionPlan);
//				}
				numOpenedSubtask = newParallelism;
			}

			this.oldExecutionPlan = jobExecutionPlan;
			this.oldExecutorMapping = deepCopy(executorMapping);
		}

		private Map<String, List<String>> deepCopy(Map<String, List<String>> executorMapping) {
			Map<String, List<String>> copiedExecutorMapping = new HashMap<>();
			for (String taskId : executorMapping.keySet()) {
				copiedExecutorMapping.put(taskId, new ArrayList<>(executorMapping.get(taskId)));
			}
			return copiedExecutorMapping;
		}
	}
}
