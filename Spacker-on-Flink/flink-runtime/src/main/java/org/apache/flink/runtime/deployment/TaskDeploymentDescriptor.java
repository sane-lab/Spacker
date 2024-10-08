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

package org.apache.flink.runtime.deployment;

import org.apache.flink.api.common.JobID;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.runtime.blob.PermanentBlobKey;
import org.apache.flink.runtime.blob.PermanentBlobService;
import org.apache.flink.runtime.checkpoint.JobManagerTaskRestore;
import org.apache.flink.runtime.clusterframework.types.AllocationID;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.executiongraph.JobInformation;
import org.apache.flink.runtime.executiongraph.TaskInformation;
import org.apache.flink.runtime.spector.migration.ReconfigID;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.util.FileUtils;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.SerializedValue;

import javax.annotation.Nullable;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.util.Collection;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * A task deployment descriptor contains all the information necessary to deploy a task on a task manager.
 */
public final class TaskDeploymentDescriptor implements Serializable {

	private static final long serialVersionUID = -3233562176034358530L;

	/**
	 * Wrapper class for serialized values which may be offloaded to the {@link
	 * org.apache.flink.runtime.blob.BlobServer} or not.
	 *
	 * @param <T>
	 * 		type of the serialized value
	 */
	@SuppressWarnings("unused")
	public static class MaybeOffloaded<T> implements Serializable {
		private static final long serialVersionUID = 5977104446396536907L;
	}

	/**
	 * A serialized value that is not offloaded to the {@link org.apache.flink.runtime.blob.BlobServer}.
	 *
	 * @param <T>
	 * 		type of the serialized value
	 */
	public static class NonOffloaded<T> extends MaybeOffloaded<T> {
		private static final long serialVersionUID = 4246628617754862463L;

		/**
		 * The serialized value.
		 */
		public SerializedValue<T> serializedValue;

		@SuppressWarnings("unused")
		public NonOffloaded() {
		}

		public NonOffloaded(SerializedValue<T> serializedValue) {
			this.serializedValue = Preconditions.checkNotNull(serializedValue);
		}
	}

	/**
	 * Reference to a serialized value that was offloaded to the {@link
	 * org.apache.flink.runtime.blob.BlobServer}.
	 *
	 * @param <T>
	 * 		type of the serialized value
	 */
	public static class Offloaded<T> extends MaybeOffloaded<T> {
		private static final long serialVersionUID = 4544135485379071679L;

		/**
		 * The key of the offloaded value BLOB.
		 */
		public PermanentBlobKey serializedValueKey;

		@SuppressWarnings("unused")
		public Offloaded() {
		}

		public Offloaded(PermanentBlobKey serializedValueKey) {
			this.serializedValueKey = Preconditions.checkNotNull(serializedValueKey);
		}
	}

	/**
	 * Serialized job information or <tt>null</tt> if offloaded.
	 */
	private MaybeOffloaded<JobInformation> serializedJobInformation;

	/**
	 * Serialized task information or <tt>null</tt> if offloaded.
	 */
	private MaybeOffloaded<TaskInformation> serializedTaskInformation;

	/**
	 * The ID referencing the job this task belongs to.
	 *
	 * <p>NOTE: this is redundant to the information stored in {@link #serializedJobInformation} but
	 * needed in order to restore offloaded data.</p>
	 */
	private final JobID jobId;

	/** The ID referencing the attempt to execute the task. */
	private final ExecutionAttemptID executionId;

	/** The allocation ID of the slot in which the task shall be run. */
	private final AllocationID allocationId;

	/** The ID referencing the rescale id of the task*/
	private ReconfigID reconfigId;

	private final Set<Integer> backupKeyGroups;

	/** The task's index in the subtask group. */
	private final int subtaskIndex;

	/** Attempt number the task. */
	private final int attemptNumber;

	/** Assigned keyGroupRange. */
	private final KeyGroupRange keyGroupRange;

	/** The id in Streamswitch. */
	private final int idInModel;

	/** The list of produced intermediate result partition deployment descriptors. */
	private final Collection<ResultPartitionDeploymentDescriptor> producedPartitions;

	/** The list of consumed intermediate result partitions. */
	private final Collection<InputGateDeploymentDescriptor> inputGates;

	/** Slot number to run the sub task in on the target machine. */
	private final int targetSlotNumber;

	/** Information to restore the task. This can be null if there is no state to restore. */
	@Nullable
	private final JobManagerTaskRestore taskRestore;

	/** Whether the deployment concerns a standby task. */
	private final boolean isStandby;
	private final Collection<Integer> srcAffectedKeygroups;
	private final Collection<Integer> dstAffectedKeygroups;

	private final Map<Integer, String> srcKeyGroupsWithDstAddr;

	public TaskDeploymentDescriptor(
		JobID jobId,
		MaybeOffloaded<JobInformation> serializedJobInformation,
		MaybeOffloaded<TaskInformation> serializedTaskInformation,
		ExecutionAttemptID executionAttemptId,
		AllocationID allocationId,
		ReconfigID reconfigId,
		int subtaskIndex,
		int attemptNumber,
		int targetSlotNumber,
		@Nullable JobManagerTaskRestore taskRestore,
		@Nullable KeyGroupRange keyGroupRange,
		int idInModel,
		Collection<ResultPartitionDeploymentDescriptor> resultPartitionDeploymentDescriptors,
		Collection<InputGateDeploymentDescriptor> inputGateDeploymentDescriptors) {

		this(jobId, serializedJobInformation, serializedTaskInformation,
			executionAttemptId, allocationId, reconfigId, new HashSet<>(), subtaskIndex, attemptNumber,
			targetSlotNumber, taskRestore, keyGroupRange, idInModel, resultPartitionDeploymentDescriptors,
			inputGateDeploymentDescriptors, null, null, null, false);
	}

	public TaskDeploymentDescriptor(
		JobID jobId,
		MaybeOffloaded<JobInformation> serializedJobInformation,
		MaybeOffloaded<TaskInformation> serializedTaskInformation,
		ExecutionAttemptID executionAttemptId,
		AllocationID allocationId,
		ReconfigID reconfigId,
		Set<Integer> backupKeyGroups,
		int subtaskIndex,
		int attemptNumber,
		int targetSlotNumber,
		@Nullable JobManagerTaskRestore taskRestore,
		@Nullable KeyGroupRange keyGroupRange,
		int idInModel,
		Collection<ResultPartitionDeploymentDescriptor> resultPartitionDeploymentDescriptors,
		Collection<InputGateDeploymentDescriptor> inputGateDeploymentDescriptors,
		@Nullable Collection<Integer> srcAffectedKeygroups,
		@Nullable Collection<Integer> dstAffectedKeygroups,
		Map<Integer, String> srcKeyGroupsWithDstAddr,
		boolean isStandby) {

		this.jobId = Preconditions.checkNotNull(jobId);

		this.serializedJobInformation = Preconditions.checkNotNull(serializedJobInformation);
		this.serializedTaskInformation = Preconditions.checkNotNull(serializedTaskInformation);

		this.executionId = Preconditions.checkNotNull(executionAttemptId);
		this.allocationId = Preconditions.checkNotNull(allocationId);
		this.reconfigId = Preconditions.checkNotNull(reconfigId);
		this.backupKeyGroups = Preconditions.checkNotNull(backupKeyGroups);

		Preconditions.checkArgument(0 <= subtaskIndex, "The subtask index must be positive.");
		this.subtaskIndex = subtaskIndex;

		Preconditions.checkArgument(0 <= attemptNumber, "The attempt number must be positive.");
		this.attemptNumber = attemptNumber;

		Preconditions.checkArgument(0 <= targetSlotNumber, "The target slot number must be positive.");
		this.targetSlotNumber = targetSlotNumber;

		this.taskRestore = taskRestore;
		this.keyGroupRange = keyGroupRange;

		this.idInModel = idInModel;

		this.producedPartitions = Preconditions.checkNotNull(resultPartitionDeploymentDescriptors);
		this.inputGates = Preconditions.checkNotNull(inputGateDeploymentDescriptors);

		this.isStandby = isStandby;

		this.srcAffectedKeygroups = srcAffectedKeygroups;
		this.dstAffectedKeygroups = dstAffectedKeygroups;

		this.srcKeyGroupsWithDstAddr = srcKeyGroupsWithDstAddr;
	}

	/**
	 * Return the sub task's serialized job information.
	 *
	 * @return serialized job information (may be <tt>null</tt> before a call to {@link
	 * #loadBigData(PermanentBlobService)}).
	 */
	@Nullable
	public SerializedValue<JobInformation> getSerializedJobInformation() {
		if (serializedJobInformation instanceof NonOffloaded) {
			NonOffloaded<JobInformation> jobInformation =
				(NonOffloaded<JobInformation>) serializedJobInformation;
			return jobInformation.serializedValue;
		} else {
			throw new IllegalStateException(
				"Trying to work with offloaded serialized job information.");
		}
	}

	/**
	 * Return the sub task's serialized task information.
	 *
	 * @return serialized task information (may be <tt>null</tt> before a call to {@link
	 * #loadBigData(PermanentBlobService)}).
	 */
	@Nullable
	public SerializedValue<TaskInformation> getSerializedTaskInformation() {
		if (serializedTaskInformation instanceof NonOffloaded) {
			NonOffloaded<TaskInformation> taskInformation =
				(NonOffloaded<TaskInformation>) serializedTaskInformation;
			return taskInformation.serializedValue;
		} else {
			throw new IllegalStateException(
				"Trying to work with offloaded serialized job information.");
		}
	}

	/**
	 * Returns the task's job ID.
	 *
	 * @return the job ID this task belongs to
	 */
	public JobID getJobId() {
		return jobId;
	}

	public ExecutionAttemptID getExecutionAttemptId() {
		return executionId;
	}

	/**
	 * Returns the task's index in the subtask group.
	 *
	 * @return the task's index in the subtask group
	 */
	public int getSubtaskIndex() {
		return subtaskIndex;
	}

	/**
	 * Returns the attempt number of the subtask.
	 */
	public int getAttemptNumber() {
		return attemptNumber;
	}

	/**
	 * Gets the number of the slot into which the task is to be deployed.
	 *
	 * @return The number of the target slot.
	 */
	public int getTargetSlotNumber() {
		return targetSlotNumber;
	}

	public Collection<ResultPartitionDeploymentDescriptor> getProducedPartitions() {
		return producedPartitions;
	}

	public Collection<InputGateDeploymentDescriptor> getInputGates() {
		return inputGates;
	}

	@Nullable
	public JobManagerTaskRestore getTaskRestore() {
		return taskRestore;
	}

	public AllocationID getAllocationId() {
		return allocationId;
	}

	public ReconfigID getReconfigId() {
		return reconfigId;
	}

	public KeyGroupRange getKeyGroupRange() {
		return keyGroupRange;
	}

	public int getIdInModel() {
		return idInModel;
	}

	/**
	 * Loads externalized data from the BLOB store back to the object.
	 *
	 * @param blobService
	 * 		the blob store to use (may be <tt>null</tt> if {@link #serializedJobInformation} and {@link
	 * 		#serializedTaskInformation} are non-<tt>null</tt>)
	 *
	 * @throws IOException
	 * 		during errors retrieving or reading the BLOBs
	 * @throws ClassNotFoundException
	 * 		Class of a serialized object cannot be found.
	 */
	public void loadBigData(@Nullable PermanentBlobService blobService)
			throws IOException, ClassNotFoundException {

		// re-integrate offloaded job info from blob
		// here, if this fails, we need to throw the exception as there is no backup path anymore
		if (serializedJobInformation instanceof Offloaded) {
			PermanentBlobKey jobInfoKey = ((Offloaded<JobInformation>) serializedJobInformation).serializedValueKey;

			Preconditions.checkNotNull(blobService);

			final File dataFile = blobService.getFile(jobId, jobInfoKey);
			// NOTE: Do not delete the job info BLOB since it may be needed again during recovery.
			//       (it is deleted automatically on the BLOB server and cache when the job
			//       enters a terminal state)
			SerializedValue<JobInformation> serializedValue =
				SerializedValue.fromBytes(FileUtils.readAllBytes(dataFile.toPath()));
			serializedJobInformation = new NonOffloaded<>(serializedValue);
		}

		// re-integrate offloaded task info from blob
		if (serializedTaskInformation instanceof Offloaded) {
			PermanentBlobKey taskInfoKey = ((Offloaded<TaskInformation>) serializedTaskInformation).serializedValueKey;

			Preconditions.checkNotNull(blobService);

			final File dataFile = blobService.getFile(jobId, taskInfoKey);
			// NOTE: Do not delete the task info BLOB since it may be needed again during recovery.
			//       (it is deleted automatically on the BLOB server and cache when the job
			//       enters a terminal state)
			SerializedValue<TaskInformation> serializedValue =
				SerializedValue.fromBytes(FileUtils.readAllBytes(dataFile.toPath()));
			serializedTaskInformation = new NonOffloaded<>(serializedValue);
		}

		// make sure that the serialized job and task information fields are filled
		Preconditions.checkNotNull(serializedJobInformation);
		Preconditions.checkNotNull(serializedTaskInformation);
	}

	@Override
	public String toString() {
		return String.format("TaskDeploymentDescriptor [execution id: %s, attempt: %d, " +
				"produced partitions: %s, input gates: %s]",
			executionId,
			attemptNumber,
			collectionToString(producedPartitions),
			collectionToString(inputGates));
	}

	private static String collectionToString(Iterable<?> collection) {
		final StringBuilder strBuilder = new StringBuilder();

		strBuilder.append("[");

		for (Object elem : collection) {
			strBuilder.append(elem);
		}

		strBuilder.append("]");

		return strBuilder.toString();
	}

	public boolean getIsStandby() {
		return isStandby;
	}

	public Collection<Integer> getSrcAffectedKeygroups() {
		return srcAffectedKeygroups;
	}

	public Collection<Integer> getDstAffectedKeygroups() {
		return dstAffectedKeygroups;
	}

	public Set<Integer> getBackupKeygroups() {
		return backupKeyGroups;
	}

	public Map<Integer, String> getSrcKeyGroupsWithDstAddr() {
		return srcKeyGroupsWithDstAddr;
	}
}
