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

package org.apache.flink.runtime.spector;

import org.apache.flink.api.common.JobID;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.checkpoint.*;
import org.apache.flink.runtime.checkpoint.StateAssignmentOperation.Operation;
import org.apache.flink.runtime.clusterframework.types.SlotID;
import org.apache.flink.runtime.clusterframework.types.TaskManagerSlot;
import org.apache.flink.runtime.concurrent.ComponentMainThreadExecutor;
import org.apache.flink.runtime.concurrent.FutureUtils;
import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.executiongraph.*;
import org.apache.flink.runtime.instance.InstanceID;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobStatus;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.jobgraph.jsonplan.JsonPlanGenerator;
import org.apache.flink.runtime.jobmaster.slotpool.SchedulerImpl;
import org.apache.flink.runtime.spector.controller.impl.ControlPlane;
import org.apache.flink.runtime.spector.migration.*;
import org.apache.flink.runtime.util.profiling.ReconfigurationProfiler;
import org.apache.flink.util.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;

import static org.apache.flink.runtime.checkpoint.StateAssignmentOperation.Operation.DISPATCH_STATE_TO_STANDBY_TASK;
import static org.apache.flink.runtime.clusterframework.types.TaskManagerSlot.State.FREE;
import static org.apache.flink.runtime.spector.JobStateCoordinator.AckStatus.DONE;
import static org.apache.flink.runtime.spector.JobStateCoordinator.AckStatus.FAILED;
import static org.apache.flink.runtime.spector.SpectorOptions.REPLICATE_KEYS_FILTER;
import static org.apache.flink.runtime.spector.SpectorOptions.TARGET_OPERATORS;
import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;


/**
 * General State Migration Steps:
 * 1. on trigger state migration, update jobgraph + execution graph.
 * 2. trigger reconfig point to get the latest state snapshot to redistribute state from src to dst.
 * 2. dispatch snapshoted state to standby tasks. => On all standby tasks ack to coordinator
 * 3.
 */
public class JobStateCoordinator implements JobReconfigActor, CheckpointProgressListener {

	private static final Logger LOG = LoggerFactory.getLogger(JobStateCoordinator.class);
	private final JobGraph jobGraph;
	private ExecutionGraph executionGraph;
	private ComponentMainThreadExecutor mainThreadExecutor;
	private ControlPlane controlPlane;
	private final JobGraphUpdater jobGraphUpdater;

	private final List<ExecutionAttemptID> notYetAcknowledgedTasks;

	private JobStatusListener jobStatusListener;

	private final Object lock = new Object();

	// mutable fields
	private volatile boolean reconfigInProgress;

	private volatile boolean replicationInProgress;

	private volatile ActionType actionType;

	private volatile ReconfigID reconfigId;

	private volatile JobExecutionPlan jobExecutionPlan;

	private volatile ExecutionJobVertex targetVertex;

	private volatile long checkpointId;

	/**
	 * standby executions for backup state maintenance of each operator
	 * Operator -> [BackupTask1, BackTask2, ...]
	 */
	private final HashMap<JobVertexID, List<ExecutionVertex>> standbyExecutionVertexes;

	/**
	 * keys to replicate
	 */
	private final Set<Integer> backupKeyGroups;

	private final Map<InstanceID, List<TaskManagerSlot>> slotsMap;

	// for replicating states confirmation
	private final Map<ExecutionAttemptID, ExecutionVertex> pendingAckTasks;

	private boolean scheduleReconfigCompleted = false;

	private final ReconfigurationProfiler reconfigurationProfiler;

	private final Configuration configuration;


	public enum AckStatus {
		DONE,
		FAILED
	}


	public JobStateCoordinator(
		JobGraph jobGraph,
		ExecutionGraph executionGraph,
		ClassLoader userCodeLoader) {

		this.jobGraph = jobGraph;
		this.executionGraph = executionGraph;

		this.notYetAcknowledgedTasks = new ArrayList<>();

		this.controlPlane = new ControlPlane(this, executionGraph);
		this.jobGraphUpdater = JobGraphUpdater.instantiate(jobGraph, userCodeLoader);

		this.configuration = executionGraph.getJobConfiguration();

		this.standbyExecutionVertexes = new HashMap<>();
		this.backupKeyGroups = new HashSet<>();
		initBackupKeyGroups();
		this.slotsMap = new HashMap<>();
		this.pendingAckTasks = new HashMap<>();

		this.reconfigurationProfiler = new ReconfigurationProfiler(executionGraph.getJobConfiguration());
	}

	public void initBackupKeyGroups() {
		int filer = configuration.getInteger(REPLICATE_KEYS_FILTER);
		String targetOperatorsStr = configuration.getString(TARGET_OPERATORS);
		String[] targetOperatorsList = targetOperatorsStr.split(",");

		for (String targetOperator : targetOperatorsList) {
			if (filer == 0) return;
			executionGraph.getAllVertices().forEach((key, value) -> {
				if (value.getName().contains(targetOperator)) {
					int maxParallelism = value.getMaxParallelism();
					for (int i = 0; i < maxParallelism; i++) {
						if (i % filer == 0) {
							backupKeyGroups.add(i);
						}
					}
				}
			});
		}
	}

	@Override
	public void updateBackupKeyGroups(int filer) {
		String targetOperatorsStr = configuration.getString(TARGET_OPERATORS);
		String[] targetOperatorsList = targetOperatorsStr.split(",");

		for (String targetOperator : targetOperatorsList) {
			if (filer == 0) return;
			executionGraph.getAllVertices().forEach((key, value) -> {
				if (value.getName().contains(targetOperator)) {
					int maxParallelism = value.getMaxParallelism();
					for (int i = 0; i < maxParallelism; i++) {
						if (i % filer == 0) {
							backupKeyGroups.add(i);
						}
					}

					for (ExecutionVertex vertex : value.getTaskVertices()) {
						Execution execution = vertex.getCurrentExecutionAttempt();
						execution.setBackupKeyGroups(backupKeyGroups);
					}
				}
			});
		}


	}

	public void setSlotsMap(CompletableFuture<Collection<TaskManagerSlot>> allSlots) {
		try {
			allSlots.thenAccept(taskManagerSlots -> {
				for (TaskManagerSlot taskManagerSlot : taskManagerSlots) {
					InstanceID taskManagerId = taskManagerSlot.getInstanceId();
					List<TaskManagerSlot> slots = slotsMap.computeIfAbsent(taskManagerId, k -> new ArrayList<>());
					slots.add(taskManagerSlot);
				}
			}).get();
		} catch (InterruptedException | ExecutionException e) {
			throw new RuntimeException(e);
		}
	}

	//****************************Standby Task Creation****************************

	/**
	 * handle failure recovery based on upstream backup + checkpoint mechanism
	 * @param taskExecution
	 * @param cause
	 */
	public void onTaskFailure(Execution taskExecution, Throwable cause) {
		composePrepareNewStandby(); // TODO: this is not required by far.
	}

	/**
	 * reinitialize after failure request new slot from different task managers and deploy backup task on the machine
	 */
	public void composePrepareNewStandby() {
		// TODO
	}

	/**
	 * create backup tasks on task manager, maintain backup state for more efficient state management.
	 * @param newExecutionJobVerticesTopological
	 */
	public void notifyNewVertices(List<ExecutionJobVertex> newExecutionJobVerticesTopological) {
		final ArrayList<CompletableFuture<Void>> schedulingFutures = new ArrayList<>();

		LOG.info("++++++ Waiting for standby tasks for: " + newExecutionJobVerticesTopological);

		final Collection<CompletableFuture<Void>> currentExecutionFutures = new ArrayList<>();

		for (ExecutionJobVertex executionJobVertex : newExecutionJobVerticesTopological) {
			for (ExecutionVertex executionVertex : executionJobVertex.getTaskVertices()) {
				currentExecutionFutures.add(
					// TODO: Anti-affinity constraint
					CompletableFuture.runAsync(() -> waitForExecutionToReachRunningState(executionVertex)));
			}
		}

		// once all execution of current executionJobVertex are in running state, deploy backup executions
		FutureUtils.combineAll(currentExecutionFutures).whenComplete((ignored, t) -> {
			checkState(executionGraph.getSlotProvider() instanceof SchedulerImpl,
				"++++++ slotProvider must be SchedulerImpl");
			setSlotsMap(((SchedulerImpl) executionGraph.getSlotProvider()).getAllSlots());
			checkState(slotsMap.size() > 0, "++++++ Empty slotsMap");

			Object[] slotsPerTMArray = slotsMap.values().toArray();

			Set<SlotID> allocatedSlots = new HashSet<>();

			for (ExecutionJobVertex executionJobVertex : newExecutionJobVerticesTopological) {

				if (t == null) {
					// create a set of executionVertex
					List<ExecutionVertex> createCandidates = executionJobVertex.addStandbyExecutionVertex(
						executionGraph.getRpcTimeout(),
						executionGraph.getGlobalModVersion(),
						System.currentTimeMillis(),
						slotsMap.size());
					// Create number of tasks according to number of nodes in the cluster.
					standbyExecutionVertexes.put(executionJobVertex.getJobVertexId(), createCandidates);

					LOG.info("++++++ add standby task for: " + executionJobVertex.getJobVertexId()
						+ " number of backup tasks: " + createCandidates.size());

					checkState(createCandidates.size() == slotsMap.size(),
						"++++++ Inconsistent number of execution vertices are created");

					// schedule for execution
					for (int i = 0; i < createCandidates.size(); i++) {
						SlotID allocatedSlot = null;
						Execution executionAttempt = createCandidates.get(i).getCurrentExecutionAttempt();
						for (TaskManagerSlot taskManagerSlot : (List<TaskManagerSlot>) slotsPerTMArray[i]) {
							if (taskManagerSlot.getState() == FREE && !allocatedSlots.contains(taskManagerSlot.getSlotId())) {
								allocatedSlot = taskManagerSlot.getSlotId();
								allocatedSlots.add(allocatedSlot);
								break;
							}
						}

						LOG.info("++++++ Allocating slot: " + allocatedSlot + " for subtask: " + executionAttempt);

						checkNotNull(allocatedSlot,
							"++++++ Failed to allocate slot for subtask: " + executionAttempt);
						schedulingFutures.add(executionAttempt.scheduleForExecution(allocatedSlot));
					}
				} else {
					LOG.info("++++++ Wrong scheduling results");
					schedulingFutures.add(
						new CompletableFuture<>());
					schedulingFutures.get(schedulingFutures.size() - 1)
						.completeExceptionally(t);
				}
			}
		}).whenCompleteAsync((ignore, t) -> {
			if (t != null) {
				LOG.error("++++++ Failed to deploy standby tasks: " + t);
				throw new CompletionException(t);
			} else {
				final CompletableFuture<Void> allSchedulingFutures = FutureUtils.waitForAll(schedulingFutures);
				allSchedulingFutures.whenComplete((Void ignored2, Throwable t2) -> {
					LOG.info("++++++ Standby tasks allocation completed");

					Preconditions.checkState(standbyExecutionVertexes.size() == newExecutionJobVerticesTopological.size(),
						"++++++ Inconsistent standby tasks number");

					controlPlane.startControllers();
					CheckpointCoordinator checkpointCoordinator = executionGraph.getCheckpointCoordinator();
					checkNotNull(checkpointCoordinator);
					checkpointCoordinator.setReconfigpointAcknowledgeListener(this);

					checkpointCoordinator.stopCheckpointScheduler();
					// dispatch standby gateways to all running tasks
					for (ExecutionJobVertex executionJobVertex : newExecutionJobVerticesTopological) {
						List<String> standbyTaskGateways = new ArrayList<>();
						for (ExecutionVertex standbyVertex : executionJobVertex.getStandbyExecutionVertexs()) {
							Execution execution = standbyVertex.getCurrentExecutionAttempt();
							standbyTaskGateways.add(execution.getTaskManagerGateway());
						}
						for (ExecutionVertex runningVertex : executionJobVertex.getTaskVertices()) {
							Execution execution = runningVertex.getCurrentExecutionAttempt();
							execution.dispatchStandbyTaskGateways(standbyTaskGateways);
						}
					}

					checkpointCoordinator.startCheckpointScheduler();

					if (t2 != null) {
						LOG.warn("Scheduling of standby tasks failed. Cancelling the scheduling of standby tasks.");
						for (ExecutionJobVertex executionJobVertex : newExecutionJobVerticesTopological) {
							cancelStandbyExecution(executionJobVertex);
						}
					}
				});
			}
		}, mainThreadExecutor);
	}

	// TODO: see if other place need to add this
	public CompletableFuture<?> cancelStandbyExecution(ExecutionJobVertex executionJobVertex) {
		if (!standbyExecutionVertexes.get(executionJobVertex.getJobVertexId()).isEmpty()) {
			LOG.debug(String.format("Cancelling standby execution %s", this));
			final ExecutionVertex standbyExecution =
				standbyExecutionVertexes.get(executionJobVertex.getJobVertexId()).remove(0);
			return standbyExecution.cancel();
		}
		return null;
	}

	private void waitForExecutionToReachRunningState(ExecutionVertex executionVertex) {
		ExecutionState executionState;
		do {
			executionState = executionVertex.getExecutionState();
		} while (executionState == ExecutionState.CREATED ||
			executionState == ExecutionState.SCHEDULED ||
			executionState == ExecutionState.DEPLOYING);
	}

	@Override
	public void onCompleteCheckpoint(CompletedCheckpoint checkpoint, PendingCheckpointStats statsCallback) throws Exception {
		checkNotNull(checkpoint);
		checkNotNull(statsCallback,
			"++++++ statsCallback cannot be null for tracking migration/replication metrics report");



//		System.out.println("++++++Current state size: " + checkpoint.getStateSize());
		if (checkpoint.getProperties().getCheckpointType() == CheckpointType.RECONFIGPOINT) {
			TaskStateStats taskStateStats = statsCallback.getTaskStateStats(targetVertex.getJobVertexId());
			long transferDuration = taskStateStats.getSummaryStats().getAsyncCheckpointDurationStats().getMaximum();
			long syncDuration = checkpoint.getDuration() - transferDuration;
			reconfigurationProfiler.onSyncEnd(syncDuration);
			LOG.info("++++++ redistribute operator states");
			migrateStateToDestinationTasks(checkpoint, transferDuration);
		} else {
//			LOG.info("++++++ checkpoint complete, start to dispatch state to replica");
			LOG.info("++++++ checkpoint and replication complete");
			Preconditions.checkState(!reconfigInProgress,
				"++++++ A reconfig is in progress, cannot do replication at the moment");
//			replicationInProgress = true;
//			reconfigurationProfiler.onReplicationStart();
//			dispatchLatestCheckpointedStateToStandbyTasks(checkpoint);
			reconfigurationProfiler.onReplicationEnd(checkpoint.getDuration());
//			replicationInProgress = false;
//			LOG.info("++++++ Skip replication in this version");
		}
	}

	/**
	 * dispatch checkpointed state to backup task based on the fine-grained state management policies
	 * by default, it replicate state to all other backup task
	 */
//	public void dispatchLatestCheckpointedStateToStandbyTasks(CompletedCheckpoint checkpoint) {
//		Preconditions.checkState(checkpoint.getProperties().getCheckpointType() == CheckpointType.CHECKPOINT, "++++++  Need to be a CHECKPOINT");
//
//		prepareAssignStates(checkpoint, DISPATCH_STATE_TO_STANDBY_TASK);
//		scheduleReconfigCompleted = true;
//		// check replication progress once, in case no keys are replicated.
//		checkStateOperationProgress();
//	}

	/**
	 * dispatch checkpointed state to backup task based on the fine-grained state management policies
	 * by default, it replicate state to all other backup task
	 */
	public void migrateStateToDestinationTasks(CompletedCheckpoint checkpoint, long transferDuration) throws ExecutionGraphException {
		Preconditions.checkState(checkpoint.getProperties().getCheckpointType() == CheckpointType.RECONFIGPOINT, "++++++ Need to be a RECONFIGPOINT");

//		prepareAssignStates(checkpoint, REPARTITION_STATE);
		// start to transfer the state to the destination tasks
		assignNewStates(transferDuration);
	}

	private void prepareAssignStates(CompletedCheckpoint checkpoint, Operation operation) {
		Map<JobVertexID, ExecutionJobVertex> tasks = executionGraph.getAllVertices();
		// re-assign the task states
		final Map<OperatorID, OperatorState> operatorStates = checkpoint.getOperatorStates();

		StateAssignmentOperation stateAssignmentOperation =
			new StateAssignmentOperation(checkpoint.getCheckpointID(), tasks, operatorStates,
				true, operation, backupKeyGroups);
		checkNotNull(jobExecutionPlan, "jobExecutionPlan should not be null.");
		stateAssignmentOperation.setRedistributeStrategy(jobExecutionPlan);
		stateAssignmentOperation.setPendingTasks(pendingAckTasks);

		stateAssignmentOperation.assignStates();
	}

	public void onAckStateTransmission(ExecutionAttemptID executionAttemptID, AckStatus ackStatus) {
		throw new RuntimeException("Deprecated, no need to send ack for replication");
//		if (ackStatus == DONE) {
//			pendingAckTasks.remove(executionAttemptID);
//			LOG.info("++++++ Receive Ack from execution: " + executionAttemptID);
//			checkStateOperationProgress();
//		} else if (ackStatus == FAILED) {
//			throw new RuntimeException("++++++ Replication/Migration failed for some reason.");
//		}
	}

	// Deprecated
//	private void checkStateOperationProgress() {
//		if (pendingAckTasks.isEmpty() && scheduleReconfigCompleted) {
//			String stateOperationType = reconfigInProgress ? "MIGRATION" : "REPLICATION";
//			LOG.info("++++++ State Operation: " + stateOperationType + " completed.");
//			if (stateOperationType.equals("MIGRATION")) {
//				completeReconfiguration(0);
//			} else {
//				reconfigurationProfiler.onReplicationEnd();
//				replicationInProgress = false;
//			}
//		}
//	}

	//****************************Scaling Actions****************************


	public void init(ComponentMainThreadExecutor mainThreadExecutor) {
		this.mainThreadExecutor = mainThreadExecutor;
	}

	public void start() {
		notifyNewVertices(executionGraph.getExecutionJobVertices());
//			.whenCompleteAsync((ignore, t) -> {
//				if (t != null) {
//					LOG.info("++++++ Failed to deploy standby tasks.");
//				} else {
//					controlPlane.startControllers();
//					CheckpointCoordinator checkpointCoordinator = executionGraph.getCheckpointCoordinator();
//					checkNotNull(checkpointCoordinator);
//					checkpointCoordinator.setReconfigpointAcknowledgeListener(this);
//				}
//			}, mainThreadExecutor);
	}

	public void stop() {
		controlPlane.stopControllers();
	}

	public void assignExecutionGraph(ExecutionGraph executionGraph) {
		checkState(!reconfigInProgress, "ExecutionGraph changed after rescaling starts");
		this.executionGraph = executionGraph;

		controlPlane.stopControllers();
		this.controlPlane = new ControlPlane(this, executionGraph);

		controlPlane.startControllers();
	}

	@Override
	public JobGraph getJobGraph() {
		return this.jobGraph;
	}

	@Override
	public ExecutionGraph getExecutionGraph() {
		return this.executionGraph;
	}

	@Override
	public void setInitialJobExecutionPlan(JobVertexID vertexID, JobExecutionPlan jobExecutionPlan) {
		// TODO: by far, we only need to a single operator, but we need to have multiple operator remapping
		this.jobExecutionPlan = jobExecutionPlan;
	}

	@Override
	public boolean checkReplicationProgress() {
		return replicationInProgress;
	}

	@Override
	public void repartition(JobVertexID vertexID, JobExecutionPlan jobExecutionPlan) throws InterruptedException {
		checkState(!reconfigInProgress && !replicationInProgress, "Current rescaling hasn't finished.");
		reconfigInProgress = true;
		reconfigurationProfiler.onReconfigurationStart();
		actionType = ActionType.REPARTITION;

		reconfigId = ReconfigID.generateNextID();
		this.jobExecutionPlan = jobExecutionPlan;

		LOG.info("++++++ repartition job with ReconfigID: " + reconfigId +
			", partitionAssignment: " + jobExecutionPlan);

		List<JobVertexID> involvedUpstream = new ArrayList<>();
		List<JobVertexID> involvedDownstream = new ArrayList<>();
		try {
			jobGraphUpdater.repartition(vertexID,
				jobExecutionPlan.getPartitionAssignment(),
				involvedUpstream, involvedDownstream);
			executionGraph.setJsonPlan(JsonPlanGenerator.generatePlan(jobGraph));

			repartitionVertex(vertexID, involvedUpstream, involvedDownstream);
		} catch (Exception e) {
			failExecution(e);
		}
	}

	private void repartitionVertex(
			JobVertexID vertexID,
			List<JobVertexID> updatedUpstream,
			List<JobVertexID> updatedDownstream) throws ExecutionGraphException {

		Map<JobVertexID, ExecutionJobVertex> tasks = executionGraph.getAllVertices();

		CheckpointCoordinator checkpointCoordinator = executionGraph.getCheckpointCoordinator();

		checkNotNull(checkpointCoordinator);
		checkpointCoordinator.setReconfigpointAcknowledgeListener(this);

		this.targetVertex = tasks.get(vertexID);

		for (ExecutionVertex vertex : this.targetVertex.getTaskVertices()) {
			notYetAcknowledgedTasks.add(vertex.getCurrentExecutionAttempt().getAttemptId());
		}

		// state check
		checkState(targetVertex.getParallelism() == jobExecutionPlan.getNumOpenedSubtask(),
			String.format("parallelism in targetVertex %d is not equal to number of executors %d",
				targetVertex.getParallelism(), jobExecutionPlan.getNumOpenedSubtask()));

		// rescale upstream and downstream
		final Collection<CompletableFuture<Void>> rescaleCandidatesFutures = new ArrayList<>();

		for (JobVertexID jobId : updatedUpstream) {
			tasks.get(jobId).cleanBeforeRescale();

			for (ExecutionVertex vertex : tasks.get(jobId).getTaskVertices()) {
				Execution execution = vertex.getCurrentExecutionAttempt();
				rescaleCandidatesFutures.add(execution.scheduleReconfig(
					reconfigId, ReconfigOptions.UPDATE_WRITERS_ONLY, null, null, null, null));
			}
		}


		for (int subtaskIndex = 0; subtaskIndex < targetVertex.getTaskVertices().length; subtaskIndex++) {
			ExecutionVertex vertex = targetVertex.getTaskVertices()[subtaskIndex];
			Execution execution = vertex.getCurrentExecutionAttempt();
			if (jobExecutionPlan.isSourceSubtask(subtaskIndex) || jobExecutionPlan.isDestinationSubtask(subtaskIndex)) {
				List<Integer> srcKeygroups = jobExecutionPlan.isSourceSubtask(subtaskIndex) ?
					jobExecutionPlan.getAffectedKeygroupsForSource(subtaskIndex) : null;
				List<Integer> dstKeygroups = jobExecutionPlan.isDestinationSubtask(subtaskIndex) ?
					jobExecutionPlan.getAffectedKeygroupsForDestination(subtaskIndex) : null;
				// if is source, find out dst tasks for all migrating keys, keep it in Map<Key, TaskManagerAddress>
				Map<Integer, String> srcKeyGroupsWithDstAddr = getSrcKeyGroupsWithDstAddr(subtaskIndex, srcKeygroups);
				// if is source, set keygroups to be checkpointed, if is dst, set keygroups to be received.
				LOG.info("++++++ Task " + subtaskIndex + " set affected keys: migrate out => "
					+ srcKeygroups + " : in => "
					+ dstKeygroups);
				rescaleCandidatesFutures.add(execution.scheduleReconfig(
						reconfigId,
						ReconfigOptions.PREPARE_AFFECTED_KEYGROUPS,
						null,
						srcKeygroups,
						dstKeygroups,
						srcKeyGroupsWithDstAddr));
			}
		}

		FutureUtils
			.combineAll(rescaleCandidatesFutures)
			.whenComplete((ignored, failure) -> {
				if (failure != null) {
					failExecution(failure);
					throw new CompletionException(failure);
				}
				LOG.info("++++++ Rescale vertex Completed");
			})
			.thenRunAsync(() -> {
				try {
//					checkpointCoordinator.stopCheckpointScheduler();

					checkpointId = checkpointCoordinator
						.triggerReconfigPoint(System.currentTimeMillis()).getCheckpointId();
					reconfigurationProfiler.onSyncStart();
					LOG.info("++++++ Make rescalepoint with checkpointId=" + checkpointId);
				} catch (Exception e) {
					failExecution(e);
					throw new CompletionException(e);
				}
			}, mainThreadExecutor);
	}

	public Map<Integer, String> getSrcKeyGroupsWithDstAddr(int subtaskIndex, List<Integer> srcKeygroups) {
		Map<Integer, String> srcKeyGroupsWithDstAddr = new HashMap<>();
		if (jobExecutionPlan.isSourceSubtask(subtaskIndex)) {
			Preconditions.checkNotNull(srcKeygroups, "++++++ src task should not contain no keygroup to migrate out");
			for (Map.Entry<Integer, List<Integer>> subTaskEntry : jobExecutionPlan.getPartitionAssignment().entrySet()) {
				int curSubTaskIdx = subTaskEntry.getKey();
				for (int keyGroup : subTaskEntry.getValue()) {
					if (srcKeygroups.contains(keyGroup)) {
						Execution currentExecutionAttempt = this.targetVertex.getTaskVertices()[curSubTaskIdx]
							.getCurrentExecutionAttempt();
						srcKeyGroupsWithDstAddr.put(keyGroup, currentExecutionAttempt.getTaskManagerGateway());
					}
				}
			}
		}
		return srcKeyGroupsWithDstAddr;
	}


	private void assignNewStates(long transferDuration) throws ExecutionGraphException {

		reconfigurationProfiler.onUpdateStart();
		scheduleReconfigCompleted = false; // set this to false to wait until all schedule reconfig being processed.

		Collection<CompletableFuture<Void>> rescaledFuture = new ArrayList<>(targetVertex.getTaskVertices().length);

		for (int i = 0; i < targetVertex.getTaskVertices().length; i++) {
			ExecutionVertex vertex  = targetVertex.getTaskVertices()[i];
			Execution executionAttempt = vertex.getCurrentExecutionAttempt();

			CompletableFuture<Void> scheduledRescale;

			if (jobExecutionPlan.isAffectedTask(i)) {
//			if (jobExecutionPlan.isDestinationSubtask(i)) {
				LOG.info("++++++ Schedule State Update for task: " + i);
				scheduledRescale = executionAttempt.scheduleReconfig(reconfigId,
					ReconfigOptions.REDISTRIBUTE_STATE,
					jobExecutionPlan.getAlignedKeyGroupRange(i),
					jobExecutionPlan.getIdInModel(i));
				rescaledFuture.add(scheduledRescale);
			} else {
				scheduledRescale = executionAttempt.scheduleReconfig(reconfigId,
					ReconfigOptions.UPDATE_KEYGROUP_RANGE_ONLY,
					jobExecutionPlan.getAlignedKeyGroupRange(i), null, null, null);
			}
			rescaledFuture.add(scheduledRescale);
		}
		LOG.info("++++++ Assign new state futures created");

		FutureUtils
			.combineAll(rescaledFuture)
			.thenRunAsync(() -> {
				LOG.info("++++++ State migration completed");
				scheduleReconfigCompleted = true;
//				checkStateOperationProgress();
				completeReconfiguration(transferDuration);
			}, mainThreadExecutor);
	}

	private void completeReconfiguration(long transferDuration) {
		LOG.info("++++++ Assign new state for repartition Completed");

		clean();

		// notify streamSwitch that change is finished
		reconfigurationProfiler.onUpdateEnd(transferDuration);
		reconfigurationProfiler.onReconfigurationEnd();
		controlPlane.onMigrationExecutorsStopped(targetVertex.getJobVertexId());
		controlPlane.onChangeImplemented(targetVertex.getJobVertexId());
	}

	private void failExecution(Throwable throwable) {
		LOG.info("++++++ Rescale failed with err: ", throwable);
		clean();
	}

	private void clean() {
		reconfigInProgress = false;
		notYetAcknowledgedTasks.clear();
		pendingAckTasks.clear();
		scheduleReconfigCompleted = false;
	}


	public JobStatusListener createActivatorDeactivator() {
		if (jobStatusListener == null) {
			jobStatusListener = new JobRescaleCoordinatorDeActivator(this);
		}

		return jobStatusListener;
	}

	private static class JobRescaleCoordinatorDeActivator implements JobStatusListener {

		private final JobStateCoordinator coordinator;

		public JobRescaleCoordinatorDeActivator(JobStateCoordinator coordinator) {
			this.coordinator = checkNotNull(coordinator);
		}

		@Override
		public void jobStatusChanges(JobID jobId, JobStatus newJobStatus, long timestamp, Throwable error) {
			if (newJobStatus == JobStatus.RUNNING) {
				coordinator.start();
			} else {
				coordinator.stop();
			}
		}
	}
}
