/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.checkpoint;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.core.fs.FSDataInputStream;
import org.apache.flink.core.memory.DataInputViewStreamWrapper;
import org.apache.flink.runtime.executiongraph.Execution;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.executiongraph.ExecutionJobVertex;
import org.apache.flink.runtime.executiongraph.ExecutionVertex;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.jobgraph.OperatorInstanceID;
import org.apache.flink.runtime.spector.migration.JobExecutionPlan;
import org.apache.flink.runtime.state.*;
import org.apache.flink.runtime.state.memory.ByteStreamStateHandle;
import org.apache.flink.util.Preconditions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;

import static org.apache.flink.runtime.spector.replication.GlobalStateManager.globalManagedStateHandles;
import static org.apache.flink.runtime.spector.replication.GlobalStateManager.globalRawStateHandles;
import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

/**
 * This class encapsulates the operation of assigning restored state when restoring from a checkpoint.
 */
public class StateAssignmentOperation {

	public static enum Operation {
		RESTORE_STATE,
		REPARTITION_STATE,
		DISPATCH_STATE_TO_STANDBY_TASK,
	}

	private static final Logger LOG = LoggerFactory.getLogger(StateAssignmentOperation.class);

	private final Map<JobVertexID, ExecutionJobVertex> tasks;
	private final Map<OperatorID, OperatorState> operatorStates;

	private final long restoreCheckpointId;
	private final boolean allowNonRestoredState;
	public final Operation operation;

	private JobExecutionPlan jobExecutionPlan;

	private Map<ExecutionAttemptID, ExecutionVertex> pendingAckTasks;

	private final Set<Integer> backupKeyGroups;

	public StateAssignmentOperation(
		long restoreCheckpointId,
		Map<JobVertexID, ExecutionJobVertex> tasks,
		Map<OperatorID, OperatorState> operatorStates,
		boolean allowNonRestoredState,
		Operation operation) {

		this(restoreCheckpointId, tasks, operatorStates, allowNonRestoredState, operation, null);
	}

	public StateAssignmentOperation(
		long restoreCheckpointId,
		Map<JobVertexID, ExecutionJobVertex> tasks,
		Map<OperatorID, OperatorState> operatorStates,
		boolean allowNonRestoredState,
		Operation operation,
		@Nullable Set<Integer> backupKeyGroups) {

		this.restoreCheckpointId = restoreCheckpointId;
		this.tasks = Preconditions.checkNotNull(tasks);
		this.operatorStates = Preconditions.checkNotNull(operatorStates);
		this.allowNonRestoredState = allowNonRestoredState;
		this.operation = operation;
		this.backupKeyGroups = backupKeyGroups;
	}

	public void assignStates() {
		Map<OperatorID, OperatorState> localOperators = new HashMap<>(operatorStates);

		checkStateMappingCompleteness(allowNonRestoredState, operatorStates, tasks);

//		if (pendingAckTasks != null) {
//			for (Map.Entry<JobVertexID, ExecutionJobVertex> task : this.tasks.entrySet()) {
//				final ExecutionJobVertex executionJobVertex = task.getValue();
//				// find the states of all operators belonging to this task
//				List<OperatorID> operatorIDs = executionJobVertex.getOperatorIDs();
//				List<OperatorID> altOperatorIDs = executionJobVertex.getUserDefinedOperatorIDs();
//				boolean statelessTask = true;
//				for (int x = 0; x < operatorIDs.size(); x++) {
//					OperatorID operatorID = altOperatorIDs.get(x) == null
//						? operatorIDs.get(x)
//						: altOperatorIDs.get(x);
//
//					OperatorState operatorState = localOperators.get(operatorID);
//					if (operatorState != null) {
//						statelessTask = false;
//					}
//				}
//
//				if (!statelessTask) {
////					if (operation == Operation.REPARTITION_STATE
////						&& !executionJobVertex.getJobVertexId().equals(jobExecutionPlan.getJobVertexID())) {
////							continue;
////					}
//					// only normal checkpoint will trigger state replication, otherwise do nothing.
//					if (operation != Operation.DISPATCH_STATE_TO_STANDBY_TASK) {
//						continue;
//					}
//					for (ExecutionVertex standbyExecutionVertex : executionJobVertex.getStandbyExecutionVertexs()) {
//						pendingAckTasks.put(
//							standbyExecutionVertex.getCurrentExecutionAttempt().getAttemptId(),
//							standbyExecutionVertex);
//					}
//				}
//			}
//			LOG.info("++++++ Pending standby tasks: " + pendingAckTasks);
//		}


		for (Map.Entry<JobVertexID, ExecutionJobVertex> task : this.tasks.entrySet()) {
			final ExecutionJobVertex executionJobVertex = task.getValue();

			// find the states of all operators belonging to this task
			List<OperatorID> operatorIDs = executionJobVertex.getOperatorIDs();
			List<OperatorID> altOperatorIDs = executionJobVertex.getUserDefinedOperatorIDs();
			List<OperatorState> operatorStates = new ArrayList<>(operatorIDs.size());
			boolean statelessTask = true;
			for (int x = 0; x < operatorIDs.size(); x++) {
				OperatorID operatorID = altOperatorIDs.get(x) == null
					? operatorIDs.get(x)
					: altOperatorIDs.get(x);

				OperatorState operatorState = localOperators.remove(operatorID);
				if (operatorState == null) {
					operatorState = new OperatorState(
						operatorID,
						executionJobVertex.getParallelism(),
						executionJobVertex.getMaxParallelism());
				} else {
					statelessTask = false;
				}
				operatorStates.add(operatorState);
			}
			if (statelessTask && operation == Operation.RESTORE_STATE) { // skip tasks where no operator has any state
				// skip tasks where no operator has any state and we want to restore state.
				//If dispatching to standbytask we use this as an epochID notification
				continue;
			}

			if (operation == Operation.REPARTITION_STATE
				&& !executionJobVertex.getJobVertexId().equals(jobExecutionPlan.getJobVertexID())) {
				continue;
			}

			assignAttemptState(task.getValue(), operatorStates);
		}

	}

	public void setRedistributeStrategy(JobExecutionPlan jobExecutionPlan) {
		this.jobExecutionPlan = jobExecutionPlan;
	}

	public void setPendingTasks(Map<ExecutionAttemptID, ExecutionVertex> pendingAckTasks) {
		this.pendingAckTasks = pendingAckTasks;
	}

	private void assignAttemptState(ExecutionJobVertex executionJobVertex, List<OperatorState> operatorStates) {

		List<OperatorID> operatorIDs = executionJobVertex.getOperatorIDs();

		//1. first compute the new parallelism
		checkParallelismPreconditions(operatorStates, executionJobVertex);

		int newParallelism = executionJobVertex.getParallelism();

		List<KeyGroupRange> keyGroupPartitions = createKeyGroupPartitions(
			executionJobVertex.getMaxParallelism(),
			newParallelism);

		final int expectedNumberOfSubTasks = newParallelism * operatorIDs.size();

		// Update global operator state based on new snapshots

		/*
		 * Redistribute ManagedOperatorStates and RawOperatorStates from old parallelism to new parallelism.
		 *
		 * The old ManagedOperatorStates with old parallelism 3:
		 *
		 * 		parallelism0 parallelism1 parallelism2
		 * op0   states0,0    state0,1	   state0,2
		 * op1
		 * op2   states2,0    state2,1	   state1,2
		 * op3   states3,0    state3,1     state3,2
		 *
		 * The new ManagedOperatorStates with new parallelism 4:
		 *
		 * 		parallelism0 parallelism1 parallelism2 parallelism3
		 * op0   state0,0	  state0,1 	   state0,2		state0,3
		 * op1
		 * op2   state2,0	  state2,1 	   state2,2		state2,3
		 * op3   state3,0	  state3,1 	   state3,2		state3,3
		 */
		Map<OperatorInstanceID, List<OperatorStateHandle>> newManagedOperatorStates =
			new HashMap<>(expectedNumberOfSubTasks);
		Map<OperatorInstanceID, List<OperatorStateHandle>> newRawOperatorStates =
			new HashMap<>(expectedNumberOfSubTasks);

		Map<OperatorInstanceID, List<KeyedStateHandle>> newManagedKeyedState =
			new HashMap<>(expectedNumberOfSubTasks);
		Map<OperatorInstanceID, List<KeyedStateHandle>> newRawKeyedState =
			new HashMap<>(expectedNumberOfSubTasks);

		if (operation == Operation.RESTORE_STATE) {
			reDistributePartitionableStates(
				operatorStates,
				newParallelism,
				operatorIDs,
				newManagedOperatorStates,
				newRawOperatorStates);

			reDistributeKeyedStates(
				operatorStates,
				newParallelism,
				operatorIDs,
				keyGroupPartitions,
				newManagedKeyedState,
				newRawKeyedState);
			/*
			 *  An executionJobVertex's all state handles needed to restore are something like a matrix
			 *
			 * 		parallelism0 parallelism1 parallelism2 parallelism3
			 * op0   sh(0,0)     sh(0,1)       sh(0,2)	    sh(0,3)
			 * op1   sh(1,0)	 sh(1,1)	   sh(1,2)	    sh(1,3)
			 * op2   sh(2,0)	 sh(2,1)	   sh(2,2)		sh(2,3)
			 * op3   sh(3,0)	 sh(3,1)	   sh(3,2)		sh(3,3)
			 *
			 */
			assignTaskStateToExecutionJobVertices(
				executionJobVertex,
				newManagedOperatorStates,
				newRawOperatorStates,
				newManagedKeyedState,
				newRawKeyedState,
				newParallelism);
		} else if (operation == Operation.REPARTITION_STATE) {
			checkNotNull(jobExecutionPlan, "++++++ JobExecutionPlan Cannot be null for repartition");

			LOG.info("++++++ Transfer snapshoted + non-replicated states to destination tasks");
//			reDistributePartitionableStates(
//				operatorStates,
//				newParallelism,
//				operatorIDs,
//				newManagedOperatorStates,
//				newRawOperatorStates);

			// TODO: The state assignment operation is applied on all operators, rather than one operator.
//			reDistributeKeyedStatesWithExecutionPlan(
//				operatorStates,
//				newParallelism,
//				operatorIDs,
//				newManagedKeyedState,
//				newRawKeyedState);
			/*
			 *  An executionJobVertex's all state handles needed to restore are something like a matrix
			 *
			 * 		parallelism0 parallelism1 parallelism2 parallelism3
			 * op0   sh(0,0)     sh(0,1)       sh(0,2)	    sh(0,3)
			 * op1   sh(1,0)	 sh(1,1)	   sh(1,2)	    sh(1,3)
			 * op2   sh(2,0)	 sh(2,1)	   sh(2,2)		sh(2,3)
			 * op3   sh(3,0)	 sh(3,1)	   sh(3,2)		sh(3,3)
			 *
			 */
			assignTaskStateToExecutionJobVertices(
				executionJobVertex,
				newManagedOperatorStates,
				newRawOperatorStates,
				newManagedKeyedState,
				newRawKeyedState,
				newParallelism);

		} else if (operation == Operation.DISPATCH_STATE_TO_STANDBY_TASK) {
//			reDistributePartitionableStates(
//				operatorStates,
//				newParallelism,
//				operatorIDs,
//				newManagedOperatorStates,
//				newRawOperatorStates);
//
//			reDistributeKeyedStatesToStandbyTasks(
//				operatorStates,
//				operatorIDs,
//				newManagedKeyedState,
//				newRawKeyedState);

			backupTaskStateToExecutionJobVertices(
				executionJobVertex,
				newManagedOperatorStates,
				newRawOperatorStates,
				newManagedKeyedState,
				newRawKeyedState
			);
		} else {
			throw new UnsupportedOperationException("++++++ Unrecognized operation type");
		}
	}

	private void reDistributeKeyedStatesToStandbyTasks(
		List<OperatorState> oldOperatorStates,
		List<OperatorID> newOperatorIDs,
		Map<OperatorInstanceID, List<KeyedStateHandle>> newManagedKeyedState,
		Map<OperatorInstanceID, List<KeyedStateHandle>> newRawKeyedState) {

		checkState(newOperatorIDs.size() == oldOperatorStates.size(),
			"This method still depends on the order of the new and old operators");

		for (int operatorIndex = 0; operatorIndex < newOperatorIDs.size(); operatorIndex++) {
			OperatorState operatorState = oldOperatorStates.get(operatorIndex);

			// For managed state, hashedKeyGroup -> (offset, streamStateHandle)
			Map<Integer, Tuple3<Long, StreamStateHandle, Boolean>> hashedKeyGroupToManagedStateHandle =
				getHashedKeyGroupToHandleFromOperatorState(operatorState, OperatorSubtaskState::getManagedKeyedState);

			Map<Integer, Tuple3<Long, StreamStateHandle, Boolean>> hashedKeyGroupToRawStateHandle =
				getHashedKeyGroupToHandleFromOperatorState(operatorState, OperatorSubtaskState::getRawKeyedState);

			// Put snapshotted state to global state store for future usage
			globalManagedStateHandles
				.computeIfAbsent(operatorState.getOperatorID(), t -> new HashMap<>())
				.putAll(hashedKeyGroupToManagedStateHandle);
			globalRawStateHandles
				.computeIfAbsent(operatorState.getOperatorID(), t -> new HashMap<>())
				.putAll(hashedKeyGroupToRawStateHandle);


			// TODO: job execution plan is incorrect because it only targeting on a operator but is used in multiple operator.
			Map<Integer, List<Integer>> partitionAssignment = jobExecutionPlan.getPartitionAssignment();

			int backupSubTaskIndex = 0; // default subtaskindex for standby task
			OperatorInstanceID backupInstanceID = OperatorInstanceID.of(backupSubTaskIndex, newOperatorIDs.get(operatorIndex));

			List<KeyedStateHandle> subManagedKeyedStates = new ArrayList<>();
			List<KeyedStateHandle> subRawKeyedStates = new ArrayList<>();

			for (int subTaskIndex = 0; subTaskIndex < operatorState.getParallelism(); subTaskIndex++) {
				OperatorInstanceID instanceID = OperatorInstanceID.of(subTaskIndex, newOperatorIDs.get(operatorIndex));

				for (int i = 0; i < partitionAssignment.get(subTaskIndex).size(); i++) {
					// the keyGroup we get from partitionAssignment is the hashed one (most origin without remapping)
					int assignedKeyGroup = partitionAssignment.get(subTaskIndex).get(i);

					// check whether it is configured to be replicated
					if (!backupKeyGroups.contains(assignedKeyGroup)) {
						continue;
					}

					KeyGroupRange alignedKeyGroupRange = jobExecutionPlan.getAlignedKeyGroupRange(subTaskIndex);
					// keyGroupRange which length is 1
					KeyGroupRange rangeOfOneKeyGroupRange = KeyGroupRange.of(alignedKeyGroupRange.getKeyGroupId(i), alignedKeyGroupRange.getKeyGroupId(i));

					Tuple3<Long, StreamStateHandle, Boolean> managedStateTuple = hashedKeyGroupToManagedStateHandle.get(assignedKeyGroup);
					if (managedStateTuple != null && managedStateTuple.f2) { // not null and the keygroup state is modified
						subManagedKeyedStates.add(new KeyGroupsStateHandle(
							new KeyGroupRangeOffsets(rangeOfOneKeyGroupRange, new long[]{managedStateTuple.f0}),
							managedStateTuple.f1));
						Tuple3<Long, StreamStateHandle, Boolean> rawStateTuple = hashedKeyGroupToRawStateHandle.get(assignedKeyGroup);
						if (rawStateTuple != null) { // TODO: what about changelog for raw keyed state?
							subRawKeyedStates.add(new KeyGroupsStateHandle(
								new KeyGroupRangeOffsets(rangeOfOneKeyGroupRange, new long[]{rawStateTuple.f0}),
								rawStateTuple.f1));
						}
					}
				}
			}
			newManagedKeyedState.put(backupInstanceID, subManagedKeyedStates);
			newRawKeyedState.put(backupInstanceID, subRawKeyedStates);
		}

//		for (int operatorIndex = 0; operatorIndex < newOperatorIDs.size(); operatorIndex++) {
//			OperatorState operatorState = oldOperatorStates.get(operatorIndex);
//
//			int subTaskIndex = 0; // default subtaskindex for standby task
//			OperatorInstanceID instanceID = OperatorInstanceID.of(subTaskIndex, newOperatorIDs.get(operatorIndex));
//
//			List<KeyedStateHandle> subManagedKeyedStates = new ArrayList<>();
//			List<KeyedStateHandle> subRawKeyedStates = new ArrayList<>();
//
//			// TODO: we hard code that all standby task to backup all state of the operator
//			// TODO: we will find a policy to make the replication more flexible
//			for (int i = 0; i < operatorState.getParallelism(); i++) {
//				if (operatorState.getState(i) != null) {
//					subManagedKeyedStates.addAll(operatorState.getState(i).getManagedKeyedState());
//					subRawKeyedStates.addAll(operatorState.getState(i).getRawKeyedState());
//				}
//			}
//
//			newManagedKeyedState.put(instanceID, subManagedKeyedStates);
//			newRawKeyedState.put(instanceID, subRawKeyedStates);
//		}
	}


	private void assignTaskStateToExecutionJobVertices(
		ExecutionJobVertex executionJobVertex,
		Map<OperatorInstanceID, List<OperatorStateHandle>> subManagedOperatorState,
		Map<OperatorInstanceID, List<OperatorStateHandle>> subRawOperatorState,
		Map<OperatorInstanceID, List<KeyedStateHandle>> subManagedKeyedState,
		Map<OperatorInstanceID, List<KeyedStateHandle>> subRawKeyedState,
		int newParallelism) {

		List<OperatorID> operatorIDs = executionJobVertex.getOperatorIDs();

		for (int subTaskIndex = 0; subTaskIndex < newParallelism; subTaskIndex++) {

			Execution currentExecutionAttempt = executionJobVertex.getTaskVertices()[subTaskIndex]
				.getCurrentExecutionAttempt();

			TaskStateSnapshot taskState = new TaskStateSnapshot(operatorIDs.size());
			boolean statelessTask = true;

			for (OperatorID operatorID : operatorIDs) {
				OperatorInstanceID instanceID = OperatorInstanceID.of(subTaskIndex, operatorID);

				OperatorSubtaskState operatorSubtaskState = operatorSubtaskStateFrom(
					instanceID,
					subManagedOperatorState,
					subRawOperatorState,
					subManagedKeyedState,
					subRawKeyedState);

				if (operatorSubtaskState.hasState()) {
					statelessTask = false;
				}
				taskState.putSubtaskStateByOperatorID(operatorID, operatorSubtaskState);
			}

			if (!statelessTask) {
				JobManagerTaskRestore taskRestore = new JobManagerTaskRestore(restoreCheckpointId, taskState);
				pendingAckTasks.put(currentExecutionAttempt.getAttemptId(), executionJobVertex.getTaskVertices()[subTaskIndex]);
				currentExecutionAttempt.setInitialState(taskRestore);
			}
		}
	}

	private void backupTaskStateToExecutionJobVertices(
		ExecutionJobVertex executionJobVertex,
		Map<OperatorInstanceID, List<OperatorStateHandle>> subManagedOperatorState,
		Map<OperatorInstanceID, List<OperatorStateHandle>> subRawOperatorState,
		Map<OperatorInstanceID, List<KeyedStateHandle>> subManagedKeyedState,
		Map<OperatorInstanceID, List<KeyedStateHandle>> subRawKeyedState) {

		List<ExecutionVertex> standbyExecutionVertexs = executionJobVertex.getStandbyExecutionVertexs();
		List<OperatorID> operatorIDs = executionJobVertex.getOperatorIDs();

		for (ExecutionVertex standbyExecutionVertex : standbyExecutionVertexs) {

			Execution currentExecutionAttempt = standbyExecutionVertex.getCurrentExecutionAttempt();

			TaskStateSnapshot taskState = new TaskStateSnapshot(operatorIDs.size());
			boolean statelessTask = true;

			for (OperatorID operatorID : operatorIDs) {
				int subTaskIndex = 0; // default subtaskindex for standby task
				OperatorInstanceID instanceID = OperatorInstanceID.of(subTaskIndex, operatorID);

				OperatorSubtaskState operatorSubtaskState = operatorSubtaskStateFrom(
					instanceID,
					subManagedOperatorState,
					subRawOperatorState,
					subManagedKeyedState,
					subRawKeyedState);

				if (operatorSubtaskState.hasState()) {
					statelessTask = false;
				}
//				else { // need to remove the stateless pending standby tasks if the managed/raw state is null.
//					pendingAckTasks.remove(currentExecutionAttempt.getAttemptId());
//				}
				taskState.putSubtaskStateByOperatorID(operatorID, operatorSubtaskState);
			}

			if (!statelessTask) {
				pendingAckTasks.put(currentExecutionAttempt.getAttemptId(), standbyExecutionVertex);
				JobManagerTaskRestore taskRestore = new JobManagerTaskRestore(restoreCheckpointId, taskState);
				currentExecutionAttempt.setInitialState(taskRestore);
			}
		}
	}

	public static OperatorSubtaskState operatorSubtaskStateFrom(
		OperatorInstanceID instanceID,
		Map<OperatorInstanceID, List<OperatorStateHandle>> subManagedOperatorState,
		Map<OperatorInstanceID, List<OperatorStateHandle>> subRawOperatorState,
		Map<OperatorInstanceID, List<KeyedStateHandle>> subManagedKeyedState,
		Map<OperatorInstanceID, List<KeyedStateHandle>> subRawKeyedState) {

		if (!subManagedOperatorState.containsKey(instanceID) &&
			!subRawOperatorState.containsKey(instanceID) &&
			!subManagedKeyedState.containsKey(instanceID) &&
			!subRawKeyedState.containsKey(instanceID)) {

			return new OperatorSubtaskState();
		}
		if (!subManagedKeyedState.containsKey(instanceID)) {
			checkState(!subRawKeyedState.containsKey(instanceID));
		}
		return new OperatorSubtaskState(
			new StateObjectCollection<>(subManagedOperatorState.getOrDefault(instanceID, Collections.emptyList())),
			new StateObjectCollection<>(subRawOperatorState.getOrDefault(instanceID, Collections.emptyList())),
			new StateObjectCollection<>(subManagedKeyedState.getOrDefault(instanceID, Collections.emptyList())),
			new StateObjectCollection<>(subRawKeyedState.getOrDefault(instanceID, Collections.emptyList())));
	}

	public void checkParallelismPreconditions(List<OperatorState> operatorStates, ExecutionJobVertex executionJobVertex) {
		for (OperatorState operatorState : operatorStates) {
			checkParallelismPreconditions(operatorState, executionJobVertex);
		}
	}

	private void reDistributeKeyedStates(
		List<OperatorState> oldOperatorStates,
		int newParallelism,
		List<OperatorID> newOperatorIDs,
		List<KeyGroupRange> newKeyGroupPartitions,
		Map<OperatorInstanceID, List<KeyedStateHandle>> newManagedKeyedState,
		Map<OperatorInstanceID, List<KeyedStateHandle>> newRawKeyedState) {
		//TODO: rewrite this method to only use OperatorID
		checkState(newOperatorIDs.size() == oldOperatorStates.size(),
			"This method still depends on the order of the new and old operators");

		for (int operatorIndex = 0; operatorIndex < newOperatorIDs.size(); operatorIndex++) {
			OperatorState operatorState = oldOperatorStates.get(operatorIndex);
			int oldParallelism = operatorState.getParallelism();
			for (int subTaskIndex = 0; subTaskIndex < newParallelism; subTaskIndex++) {
				OperatorInstanceID instanceID = OperatorInstanceID.of(subTaskIndex, newOperatorIDs.get(operatorIndex));
				Tuple2<List<KeyedStateHandle>, List<KeyedStateHandle>> subKeyedStates = reAssignSubKeyedStates(
					operatorState,
					newKeyGroupPartitions,
					subTaskIndex,
					newParallelism,
					oldParallelism);
				newManagedKeyedState.put(instanceID, subKeyedStates.f0);
				newRawKeyedState.put(instanceID, subKeyedStates.f1);
			}
		}
	}

	// TODO rewrite based on operator id
	private Tuple2<List<KeyedStateHandle>, List<KeyedStateHandle>> reAssignSubKeyedStates(
		OperatorState operatorState,
		List<KeyGroupRange> keyGroupPartitions,
		int subTaskIndex,
		int newParallelism,
		int oldParallelism) {

		List<KeyedStateHandle> subManagedKeyedState;
		List<KeyedStateHandle> subRawKeyedState;

		if (newParallelism == oldParallelism) {
			if (operatorState.getState(subTaskIndex) != null) {
				subManagedKeyedState = operatorState.getState(subTaskIndex).getManagedKeyedState().asList();
				subRawKeyedState = operatorState.getState(subTaskIndex).getRawKeyedState().asList();
			} else {
				subManagedKeyedState = Collections.emptyList();
				subRawKeyedState = Collections.emptyList();
			}
		} else {
			subManagedKeyedState = getManagedKeyedStateHandles(operatorState, keyGroupPartitions.get(subTaskIndex));
			subRawKeyedState = getRawKeyedStateHandles(operatorState, keyGroupPartitions.get(subTaskIndex));
		}

		if (subManagedKeyedState.isEmpty() && subRawKeyedState.isEmpty()) {
			return new Tuple2<>(Collections.emptyList(), Collections.emptyList());
		} else {
			return new Tuple2<>(subManagedKeyedState, subRawKeyedState);
		}
	}

	@VisibleForTesting
	static void reDistributePartitionableStates(
		List<OperatorState> oldOperatorStates,
		int newParallelism,
		List<OperatorID> newOperatorIDs,
		Map<OperatorInstanceID, List<OperatorStateHandle>> newManagedOperatorStates,
		Map<OperatorInstanceID, List<OperatorStateHandle>> newRawOperatorStates) {

		//TODO: rewrite this method to only use OperatorID
		checkState(newOperatorIDs.size() == oldOperatorStates.size(),
			"This method still depends on the order of the new and old operators");

		// The nested list wraps as the level of operator -> subtask -> state object collection
		List<List<List<OperatorStateHandle>>> oldManagedOperatorStates = new ArrayList<>(oldOperatorStates.size());
		List<List<List<OperatorStateHandle>>> oldRawOperatorStates = new ArrayList<>(oldOperatorStates.size());

		splitManagedAndRawOperatorStates(oldOperatorStates, oldManagedOperatorStates, oldRawOperatorStates);
		OperatorStateRepartitioner opStateRepartitioner = RoundRobinOperatorStateRepartitioner.INSTANCE;

		for (int operatorIndex = 0; operatorIndex < newOperatorIDs.size(); operatorIndex++) {
			OperatorState operatorState = oldOperatorStates.get(operatorIndex);
			int oldParallelism = operatorState.getParallelism();

			OperatorID operatorID = newOperatorIDs.get(operatorIndex);

			newManagedOperatorStates.putAll(applyRepartitioner(
				operatorID,
				opStateRepartitioner,
				oldManagedOperatorStates.get(operatorIndex),
				oldParallelism,
				newParallelism));

			newRawOperatorStates.putAll(applyRepartitioner(
				operatorID,
				opStateRepartitioner,
				oldRawOperatorStates.get(operatorIndex),
				oldParallelism,
				newParallelism));
		}
	}

	@VisibleForTesting
	static void reDistributePartitionableStatesToStandbyTasks(
		List<OperatorState> oldOperatorStates,
		List<OperatorID> newOperatorIDs,
		Map<OperatorInstanceID, List<OperatorStateHandle>> newManagedOperatorStates,
		Map<OperatorInstanceID, List<OperatorStateHandle>> newRawOperatorStates) {

		//TODO: rewrite this method to only use OperatorID
		checkState(newOperatorIDs.size() == oldOperatorStates.size(),
			"This method still depends on the order of the new and old operators");

		for (int operatorIndex = 0; operatorIndex < newOperatorIDs.size(); operatorIndex++) {
			OperatorState operatorState = oldOperatorStates.get(operatorIndex);
			int oldParallelism = operatorState.getParallelism();

			int subTaskIndex = 0; // default subtaskindex for standby task
			OperatorInstanceID instanceID = OperatorInstanceID.of(subTaskIndex, newOperatorIDs.get(operatorIndex));

			List<OperatorStateHandle> managedOpState = new ArrayList<>();
			List<OperatorStateHandle> rawOpState = new ArrayList<>();

			for (int i = 0; i < oldParallelism; i++) {
				OperatorSubtaskState operatorSubtaskState = operatorState.getState(i);

				StateObjectCollection<OperatorStateHandle> managed = operatorSubtaskState.getManagedOperatorState();
				StateObjectCollection<OperatorStateHandle> raw = operatorSubtaskState.getRawOperatorState();

				managedOpState.addAll(managed.asList());
				rawOpState.addAll(raw.asList());
			}

			newManagedOperatorStates.put(instanceID, managedOpState);
			newRawOperatorStates.put(instanceID, rawOpState);
		}
	}


	private void reDistributeKeyedStatesWithExecutionPlan(
		List<OperatorState> oldOperatorStates,
		int newParallelism,
		List<OperatorID> newOperatorIDs,
		Map<OperatorInstanceID, List<KeyedStateHandle>> newManagedKeyedState,
		Map<OperatorInstanceID, List<KeyedStateHandle>> newRawKeyedState) {

		checkState(newOperatorIDs.size() == oldOperatorStates.size(),
			"This method still depends on the order of the new and old operators");

		for (int operatorIndex = 0; operatorIndex < newOperatorIDs.size(); operatorIndex++) {
			OperatorState operatorState = oldOperatorStates.get(operatorIndex);

			// get snapshoted keygroups state handle
			// For managed state, hashedKeyGroup -> (offset, streamStateHandle)
			Map<Integer, Tuple3<Long, StreamStateHandle, Boolean>> hashedKeyGroupToManagedStateHandle =
				getHashedKeyGroupToHandleFromOperatorState(operatorState, OperatorSubtaskState::getManagedKeyedState);

			Map<Integer, Tuple3<Long, StreamStateHandle, Boolean>> hashedKeyGroupToRawStateHandle =
				getHashedKeyGroupToHandleFromOperatorState(operatorState, OperatorSubtaskState::getRawKeyedState);

			// TODO: job execution plan is incorrect because it only targeting on a operator but is used in multiple operator.
			Map<Integer, List<Integer>> partitionAssignment = jobExecutionPlan.getPartitionAssignment();

			for (int subTaskIndex = 0; subTaskIndex < newParallelism; subTaskIndex++) {

				// only need to pass state handle to destination subtasks.
				if (!jobExecutionPlan.isDestinationSubtask(subTaskIndex)) {
					continue;
				}

				Set<Integer> migrateInKeygroups =
					new HashSet<>(jobExecutionPlan.getAffectedKeygroupsForDestination(subTaskIndex));

				// stats for debug
				int migrateInKeyGroupSize = migrateInKeygroups.size();
				int actualMigratingSize = 0;

				Set<Integer> snapshotedKeygroups = new HashSet<>(hashedKeyGroupToManagedStateHandle.keySet());

				OperatorInstanceID instanceID = OperatorInstanceID.of(subTaskIndex, newOperatorIDs.get(operatorIndex));

				List<KeyedStateHandle> subManagedKeyedStates = new ArrayList<>();
				List<KeyedStateHandle> subRawKeyedStates = new ArrayList<>();

				List<Integer> workloads = partitionAssignment.get(subTaskIndex);
				for (int i = 0; i < workloads.size(); i++) {
					// the keyGroup we get from partitionAssignment is the hashed one (most origin without remapping)
					int assignedKeyGroup = workloads.get(i);

					// do not consider the state migration here, the backuped state has been filtered out at checkpoint phase. Here only exists those changed keys to migrate.
					if (!migrateInKeygroups.contains(assignedKeyGroup)) {
						continue;
					}

					Tuple3<Long, StreamStateHandle, Boolean> managedStateTuple;
					Tuple3<Long, StreamStateHandle, Boolean> rawStateTuple;

					if (snapshotedKeygroups.contains(assignedKeyGroup)) {
						// get state handle from hashedKeyGroupToManagedStateHandle/hashedKeyGroupToRawStateHandle
						managedStateTuple = hashedKeyGroupToManagedStateHandle
							.get(assignedKeyGroup);
						rawStateTuple = hashedKeyGroupToRawStateHandle
							.get(assignedKeyGroup);
					} else if (!backupKeyGroups.contains(assignedKeyGroup)) {
						if (!globalManagedStateHandles.containsKey(operatorState.getOperatorID())) {
							continue;
						}
						if (!globalManagedStateHandles.get(operatorState.getOperatorID()).containsKey(assignedKeyGroup)) {
							continue; // skip if the state has not been snapshotted yet
						}
						// for non-snapshoted state, if it is still not in replicated state
						// get state handle from globalManagedStateHandles/globalRawStateHandles
						managedStateTuple = globalManagedStateHandles
							.get(operatorState.getOperatorID())
							.get(assignedKeyGroup);
						rawStateTuple = globalRawStateHandles
							.get(operatorState.getOperatorID())
							.get(assignedKeyGroup);
					} else {
						// otherwise just recover state from standby tasks.
						continue;
					}

					actualMigratingSize++;

					KeyGroupRange alignedKeyGroupRange = jobExecutionPlan.getAlignedKeyGroupRange(subTaskIndex);
					// keyGroupRange which length is 1
					KeyGroupRange rangeOfOneKeyGroupRange = KeyGroupRange.of(alignedKeyGroupRange.getKeyGroupId(i), alignedKeyGroupRange.getKeyGroupId(i));

					if (managedStateTuple != null) {
						subManagedKeyedStates.add(new KeyGroupsStateHandle(
							new KeyGroupRangeOffsets(rangeOfOneKeyGroupRange, new long[]{managedStateTuple.f0}),
							managedStateTuple.f1));
					}

					if (rawStateTuple != null) {
						subRawKeyedStates.add(new KeyGroupsStateHandle(
							new KeyGroupRangeOffsets(rangeOfOneKeyGroupRange, new long[]{rawStateTuple.f0}),
							rawStateTuple.f1));
					}
				}

				LOG.info("++++++ Total KeyGroups to migrate: " + actualMigratingSize + "/" + migrateInKeyGroupSize);

				newManagedKeyedState.put(instanceID, subManagedKeyedStates);
				newRawKeyedState.put(instanceID, subRawKeyedStates);
			}
		}
	}

	private static Map<Integer, Tuple3<Long, StreamStateHandle, Boolean>> getHashedKeyGroupToHandleFromOperatorState(
		OperatorState operatorState,
		Function<OperatorSubtaskState, StateObjectCollection<KeyedStateHandle>> applier) {

		Map<Integer, Tuple3<Long, StreamStateHandle, Boolean>> hashedKeyGroupToHandle = new HashMap<>();

		for (int i = 0; i < operatorState.getParallelism(); i++) {
			if (operatorState.getState(i) != null) {
				for (KeyedStateHandle keyedStateHandle : applier.apply(operatorState.getState(i))) {
					if (keyedStateHandle != null) {
						if (keyedStateHandle instanceof KeyGroupsStateHandle) {

							KeyGroupsStateHandle keyGroupsStateHandle = (KeyGroupsStateHandle) keyedStateHandle;
							KeyGroupRange keyGroupRangeFromOldState = keyGroupsStateHandle.getKeyGroupRange();

							if (keyGroupRangeFromOldState.equals(KeyGroupRange.EMPTY_KEY_GROUP_RANGE)) {
								continue;
							}

							int start = keyGroupRangeFromOldState.getStartKeyGroup();
							int end = keyGroupRangeFromOldState.getEndKeyGroup();

							// for byte stream state handle usage, to reduce the state size to be migrated over Akka rpc
							long startOffset = keyGroupsStateHandle.getOffsetForKeyGroup(start);

							try (FSDataInputStream fsDataInputStream = keyGroupsStateHandle.openInputStream()) {
								DataInputViewStreamWrapper inView = new DataInputViewStreamWrapper(fsDataInputStream);


								for (int alignedOldKeyGroup = start; alignedOldKeyGroup <= end; alignedOldKeyGroup++) {
									long offset = keyGroupsStateHandle.getOffsetForKeyGroup(alignedOldKeyGroup);
									boolean isModified = keyGroupsStateHandle.getIsModified(alignedOldKeyGroup);

									int nextAlignedOldKeyGroup = alignedOldKeyGroup + 1;

									long nextOffset = nextAlignedOldKeyGroup <= end ?
										keyGroupsStateHandle.getOffsetForKeyGroup(nextAlignedOldKeyGroup)
										: keyGroupsStateHandle.getStateSize();

									// skip if the keygroup does not have any state snapshot
									if (nextOffset == offset) {
										continue;
									}

									fsDataInputStream.seek(offset);
									int hashedKeyGroup = inView.readInt();

									hashedKeyGroupToHandle.put(
										hashedKeyGroup,
										Tuple3.of(offset, keyGroupsStateHandle.getDelegateStateHandle(), isModified));

//									StreamStateHandle curStateHandle = keyGroupsStateHandle.getDelegateStateHandle();
////									// TODO: byte array can at most store 2gb state...
//									Preconditions.checkState(curStateHandle instanceof ByteStreamStateHandle,
//										"++++++ State handle operations are only supported for byted state handle");
//									StreamStateHandle newStateHandlePerKeyGroup =
//										((ByteStreamStateHandle) curStateHandle).copyOfRange(startOffset, offset, nextOffset);
//
//									hashedKeyGroupToHandle.put(
//										hashedKeyGroup,
//										Tuple3.of(startOffset, newStateHandlePerKeyGroup, isModified));
								}
							} catch (IOException err) {
								throw new RuntimeException(err);
							}
						} else {
							throw new RuntimeException("++++++ unsupported KeyedStateHandle for state migration");
						}
					}
				}
			}
		}
		return hashedKeyGroupToHandle;
	}

	private static void splitManagedAndRawOperatorStates(
		List<OperatorState> operatorStates,
		List<List<List<OperatorStateHandle>>> managedOperatorStates,
		List<List<List<OperatorStateHandle>>> rawOperatorStates) {

		for (OperatorState operatorState : operatorStates) {

			final int parallelism = operatorState.getParallelism();
			List<List<OperatorStateHandle>> managedOpStatePerSubtasks = new ArrayList<>(parallelism);
			List<List<OperatorStateHandle>> rawOpStatePerSubtasks = new ArrayList<>(parallelism);

			for (int subTaskIndex = 0; subTaskIndex < parallelism; subTaskIndex++) {
				OperatorSubtaskState operatorSubtaskState = operatorState.getState(subTaskIndex);
				if (operatorSubtaskState == null) {
					managedOpStatePerSubtasks.add(Collections.emptyList());
					rawOpStatePerSubtasks.add(Collections.emptyList());
				} else {
					StateObjectCollection<OperatorStateHandle> managed = operatorSubtaskState.getManagedOperatorState();
					StateObjectCollection<OperatorStateHandle> raw = operatorSubtaskState.getRawOperatorState();

					managedOpStatePerSubtasks.add(managed.asList());
					rawOpStatePerSubtasks.add(raw.asList());
				}
			}
			managedOperatorStates.add(managedOpStatePerSubtasks);
			rawOperatorStates.add(rawOpStatePerSubtasks);
		}
	}

	/**
	 * Collect {@link KeyGroupsStateHandle  managedKeyedStateHandles} which have intersection with given
	 * {@link KeyGroupRange} from {@link TaskState operatorState}
	 *
	 * @param operatorState        all state handles of a operator
	 * @param subtaskKeyGroupRange the KeyGroupRange of a subtask
	 * @return all managedKeyedStateHandles which have intersection with given KeyGroupRange
	 */
	public static List<KeyedStateHandle> getManagedKeyedStateHandles(
		OperatorState operatorState,
		KeyGroupRange subtaskKeyGroupRange) {

		final int parallelism = operatorState.getParallelism();

		List<KeyedStateHandle> subtaskKeyedStateHandles = null;

		for (int i = 0; i < parallelism; i++) {
			if (operatorState.getState(i) != null) {

				Collection<KeyedStateHandle> keyedStateHandles = operatorState.getState(i).getManagedKeyedState();

				if (subtaskKeyedStateHandles == null) {
					subtaskKeyedStateHandles = new ArrayList<>(parallelism * keyedStateHandles.size());
				}

				extractIntersectingState(
					keyedStateHandles,
					subtaskKeyGroupRange,
					subtaskKeyedStateHandles);
			}
		}

		return subtaskKeyedStateHandles;
	}

	/**
	 * Collect {@link KeyGroupsStateHandle  rawKeyedStateHandles} which have intersection with given
	 * {@link KeyGroupRange} from {@link TaskState operatorState}
	 *
	 * @param operatorState        all state handles of a operator
	 * @param subtaskKeyGroupRange the KeyGroupRange of a subtask
	 * @return all rawKeyedStateHandles which have intersection with given KeyGroupRange
	 */
	public static List<KeyedStateHandle> getRawKeyedStateHandles(
		OperatorState operatorState,
		KeyGroupRange subtaskKeyGroupRange) {

		final int parallelism = operatorState.getParallelism();

		List<KeyedStateHandle> extractedKeyedStateHandles = null;

		for (int i = 0; i < parallelism; i++) {
			if (operatorState.getState(i) != null) {

				Collection<KeyedStateHandle> rawKeyedState = operatorState.getState(i).getRawKeyedState();

				if (extractedKeyedStateHandles == null) {
					extractedKeyedStateHandles = new ArrayList<>(parallelism * rawKeyedState.size());
				}

				extractIntersectingState(
					rawKeyedState,
					subtaskKeyGroupRange,
					extractedKeyedStateHandles);
			}
		}

		return extractedKeyedStateHandles;
	}

	/**
	 * Extracts certain key group ranges from the given state handles and adds them to the collector.
	 */
	private static void extractIntersectingState(
		Collection<KeyedStateHandle> originalSubtaskStateHandles,
		KeyGroupRange rangeToExtract,
		List<KeyedStateHandle> extractedStateCollector) {

		for (KeyedStateHandle keyedStateHandle : originalSubtaskStateHandles) {

			if (keyedStateHandle != null) {

				KeyedStateHandle intersectedKeyedStateHandle = keyedStateHandle.getIntersection(rangeToExtract);

				if (intersectedKeyedStateHandle != null) {
					extractedStateCollector.add(intersectedKeyedStateHandle);
				}
			}
		}
	}

	/**
	 * Groups the available set of key groups into key group partitions. A key group partition is
	 * the set of key groups which is assigned to the same task. Each set of the returned list
	 * constitutes a key group partition.
	 * <p>
	 * <b>IMPORTANT</b>: The assignment of key groups to partitions has to be in sync with the
	 * KeyGroupStreamPartitioner.
	 *
	 * @param numberKeyGroups Number of available key groups (indexed from 0 to numberKeyGroups - 1)
	 * @param parallelism     Parallelism to generate the key group partitioning for
	 * @return List of key group partitions
	 */
	public static List<KeyGroupRange> createKeyGroupPartitions(int numberKeyGroups, int parallelism) {
		Preconditions.checkArgument(numberKeyGroups >= parallelism);
		List<KeyGroupRange> result = new ArrayList<>(parallelism);

		for (int i = 0; i < parallelism; ++i) {
			result.add(KeyGroupRangeAssignment.computeKeyGroupRangeForOperatorIndex(numberKeyGroups, parallelism, i));
		}
		return result;
	}

	/**
	 * Verifies conditions in regards to parallelism and maxParallelism that must be met when restoring state.
	 *
	 * @param operatorState      state to restore
	 * @param executionJobVertex task for which the state should be restored
	 */
	private static void checkParallelismPreconditions(OperatorState operatorState, ExecutionJobVertex executionJobVertex) {
		//----------------------------------------max parallelism preconditions-------------------------------------

		if (operatorState.getMaxParallelism() < executionJobVertex.getParallelism()) {
			throw new IllegalStateException("The state for task " + executionJobVertex.getJobVertexId() +
				" can not be restored. The maximum parallelism (" + operatorState.getMaxParallelism() +
				") of the restored state is lower than the configured parallelism (" + executionJobVertex.getParallelism() +
				"). Please reduce the parallelism of the task to be lower or equal to the maximum parallelism."
			);
		}

		// check that the number of key groups have not changed or if we need to override it to satisfy the restored state
		if (operatorState.getMaxParallelism() != executionJobVertex.getMaxParallelism()) {

			if (!executionJobVertex.isMaxParallelismConfigured()) {
				// if the max parallelism was not explicitly specified by the user, we derive it from the state

				LOG.debug("Overriding maximum parallelism for JobVertex {} from {} to {}",
					executionJobVertex.getJobVertexId(), executionJobVertex.getMaxParallelism(), operatorState.getMaxParallelism());

				executionJobVertex.setMaxParallelism(operatorState.getMaxParallelism());
			} else {
				// if the max parallelism was explicitly specified, we complain on mismatch
				throw new IllegalStateException("The maximum parallelism (" +
					operatorState.getMaxParallelism() + ") with which the latest " +
					"checkpoint of the execution job vertex " + executionJobVertex +
					" has been taken and the current maximum parallelism (" +
					executionJobVertex.getMaxParallelism() + ") changed. This " +
					"is currently not supported.");
			}
		}
	}

	/**
	 * Verifies that all operator states can be mapped to an execution job vertex.
	 *
	 * @param allowNonRestoredState if false an exception will be thrown if a state could not be mapped
	 * @param operatorStates operator states to map
	 * @param tasks task to map to
	 */
	private static void checkStateMappingCompleteness(
		boolean allowNonRestoredState,
		Map<OperatorID, OperatorState> operatorStates,
		Map<JobVertexID, ExecutionJobVertex> tasks) {

		Set<OperatorID> allOperatorIDs = new HashSet<>();
		for (ExecutionJobVertex executionJobVertex : tasks.values()) {
			allOperatorIDs.addAll(executionJobVertex.getOperatorIDs());
		}
		for (Map.Entry<OperatorID, OperatorState> operatorGroupStateEntry : operatorStates.entrySet()) {
			OperatorState operatorState = operatorGroupStateEntry.getValue();
			//----------------------------------------find operator for state---------------------------------------------

			if (!allOperatorIDs.contains(operatorGroupStateEntry.getKey())) {
				if (allowNonRestoredState) {
					LOG.info("Skipped checkpoint state for operator {}.", operatorState.getOperatorID());
				} else {
					throw new IllegalStateException("There is no operator for the state " + operatorState.getOperatorID());
				}
			}
		}
	}

	public static Map<OperatorInstanceID, List<OperatorStateHandle>> applyRepartitioner(
		OperatorID operatorID,
		OperatorStateRepartitioner opStateRepartitioner,
		List<List<OperatorStateHandle>> chainOpParallelStates,
		int oldParallelism,
		int newParallelism) {

		List<List<OperatorStateHandle>> states = applyRepartitioner(
			opStateRepartitioner,
			chainOpParallelStates,
			oldParallelism,
			newParallelism);

		Map<OperatorInstanceID, List<OperatorStateHandle>> result = new HashMap<>(states.size());

		for (int subtaskIndex = 0; subtaskIndex < states.size(); subtaskIndex++) {
			checkNotNull(states.get(subtaskIndex) != null, "states.get(subtaskIndex) is null");
			result.put(OperatorInstanceID.of(subtaskIndex, operatorID), states.get(subtaskIndex));
		}

		return result;
	}

	/**
	 * Repartitions the given operator state using the given {@link OperatorStateRepartitioner} with respect to the new
	 * parallelism.
	 *
	 * @param opStateRepartitioner  partitioner to use
	 * @param chainOpParallelStates state to repartition
	 * @param oldParallelism        parallelism with which the state is currently partitioned
	 * @param newParallelism        parallelism with which the state should be partitioned
	 * @return repartitioned state
	 */
	// TODO rewrite based on operator id
	public static List<List<OperatorStateHandle>> applyRepartitioner(
		OperatorStateRepartitioner opStateRepartitioner,
		List<List<OperatorStateHandle>> chainOpParallelStates,
		int oldParallelism,
		int newParallelism) {

		if (chainOpParallelStates == null) {
			return Collections.emptyList();
		}

		return opStateRepartitioner.repartitionState(
			chainOpParallelStates,
			oldParallelism,
			newParallelism);
	}

	/**
	 * Determine the subset of {@link KeyGroupsStateHandle KeyGroupsStateHandles} with correct
	 * key group index for the given subtask {@link KeyGroupRange}.
	 *
	 * <p>This is publicly visible to be used in tests.
	 */
	public static List<KeyedStateHandle> getKeyedStateHandles(
		Collection<? extends KeyedStateHandle> keyedStateHandles,
		KeyGroupRange subtaskKeyGroupRange) {

		List<KeyedStateHandle> subtaskKeyedStateHandles = new ArrayList<>(keyedStateHandles.size());

		for (KeyedStateHandle keyedStateHandle : keyedStateHandles) {
			KeyedStateHandle intersectedKeyedStateHandle = keyedStateHandle.getIntersection(subtaskKeyGroupRange);

			if (intersectedKeyedStateHandle != null) {
				subtaskKeyedStateHandles.add(intersectedKeyedStateHandle);
			}
		}

		return subtaskKeyedStateHandles;
	}
}
