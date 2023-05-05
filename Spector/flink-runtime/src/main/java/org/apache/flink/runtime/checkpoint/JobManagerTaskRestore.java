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

import org.apache.flink.core.fs.FSDataInputStream;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataInputViewStreamWrapper;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.KeyGroupsStateHandle;
import org.apache.flink.runtime.state.KeyedStateHandle;
import org.apache.flink.runtime.state.TaskStateManagerImpl;
import org.apache.flink.util.Preconditions;

import javax.annotation.Nonnegative;
import javax.annotation.Nonnull;

import java.io.IOException;
import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;

/**
 * This class encapsulates the data from the job manager to restore a task.
 */
public class JobManagerTaskRestore implements Serializable {

	private static final long serialVersionUID = 1L;

	/** The id of the checkpoint from which we restore. */
	private long restoreCheckpointId;

	/** The state for this task to restore. */
	private TaskStateSnapshot taskStateSnapshot;

	public JobManagerTaskRestore() {}

	public JobManagerTaskRestore(@Nonnegative long restoreCheckpointId, @Nonnull TaskStateSnapshot taskStateSnapshot) {
		this.restoreCheckpointId = restoreCheckpointId;
		this.taskStateSnapshot = taskStateSnapshot;
	}

	public long getRestoreCheckpointId() {
		return restoreCheckpointId;
	}

	@Nonnull
	public TaskStateSnapshot getTaskStateSnapshot() {
		return taskStateSnapshot;
	}

	@Override
	public String toString() {
		return "JobManagerTaskRestore{" +
			"restoreCheckpointId=" + restoreCheckpointId +
			", taskStateSnapshot=" + taskStateSnapshot +
			'}';
	}

	public void write(DataOutputView out) throws Exception {
		out.writeLong(restoreCheckpointId);
		taskStateSnapshot.write(out);
	}

	public void read(DataInputView in) throws Exception {
		restoreCheckpointId = in.readLong();
		taskStateSnapshot = new TaskStateSnapshot();
		taskStateSnapshot.read(in);
	}

	public void merge(JobManagerTaskRestore stateRestore2) {
		if (stateRestore2 == null) {
			return;
		}

		Map<Integer, KeyedStateHandle> hashedKeyGroupToManagedStateHandle =
			getHashedKeyGroupToHandleFromStateRestore(this, OperatorSubtaskState::getManagedKeyedState);
		Map<Integer, KeyedStateHandle> hashedKeyGroupToRawStateHandle =
			getHashedKeyGroupToHandleFromStateRestore(this, OperatorSubtaskState::getRawKeyedState);

		Map<Integer, KeyedStateHandle> hashedKeyGroupToManagedStateHandle2 =
			getHashedKeyGroupToHandleFromStateRestore(stateRestore2, OperatorSubtaskState::getManagedKeyedState);
		Map<Integer, KeyedStateHandle> hashedKeyGroupToRawStateHandle2 =
			getHashedKeyGroupToHandleFromStateRestore(stateRestore2, OperatorSubtaskState::getRawKeyedState);


		// The state restore 2 always be the old version, merge the old version to the new version when the new version does not contain the key
		hashedKeyGroupToManagedStateHandle2.forEach(hashedKeyGroupToManagedStateHandle::putIfAbsent);
		hashedKeyGroupToRawStateHandle2.forEach(hashedKeyGroupToRawStateHandle::putIfAbsent);

//		hashedKeyGroupToManagedStateHandle.putAll(hashedKeyGroupToManagedStateHandle2);
//		hashedKeyGroupToRawStateHandle.putAll(hashedKeyGroupToRawStateHandle2);

		Map.Entry<OperatorID, OperatorSubtaskState> operatorSubtaskStateEntry = getOperatorSubtaskState(this);

		Preconditions.checkState(operatorSubtaskStateEntry != null, "++++++ operatorSubtaskState cannot be null");

		OperatorSubtaskState newOperatorSubtaskState = new OperatorSubtaskState(
			operatorSubtaskStateEntry.getValue().getManagedOperatorState(),
			operatorSubtaskStateEntry.getValue().getRawOperatorState(),
			new StateObjectCollection<>(hashedKeyGroupToManagedStateHandle.values()),
			new StateObjectCollection<>(hashedKeyGroupToRawStateHandle.values()));

		taskStateSnapshot.putSubtaskStateByOperatorID(operatorSubtaskStateEntry.getKey(), newOperatorSubtaskState);
	}

	private Map<Integer, KeyedStateHandle> getHashedKeyGroupToHandleFromStateRestore(
		JobManagerTaskRestore stateRestore,
		Function<OperatorSubtaskState, StateObjectCollection<KeyedStateHandle>> applier) {

		Map<Integer, KeyedStateHandle> hashedKeyGroupToHandle = new HashMap<>();

		if (stateRestore == null) {
			return hashedKeyGroupToHandle;
		}

		Set<Map.Entry<OperatorID, OperatorSubtaskState>> subtaskStateMappings
			= stateRestore.getTaskStateSnapshot().getSubtaskStateMappings();

		// By default, there will only be one Operator inside the OperatorChain.
		for (Map.Entry<OperatorID, OperatorSubtaskState> subtaskStateEntry : subtaskStateMappings) {
			for (KeyedStateHandle keyedStateHandle : applier.apply(subtaskStateEntry.getValue())) {
				if (keyedStateHandle != null) {
					if (keyedStateHandle instanceof KeyGroupsStateHandle) {

						KeyGroupsStateHandle keyGroupsStateHandle = (KeyGroupsStateHandle) keyedStateHandle;
						KeyGroupRange keyGroupRangeFromOldState = keyGroupsStateHandle.getKeyGroupRange();

						if (keyGroupRangeFromOldState.equals(KeyGroupRange.EMPTY_KEY_GROUP_RANGE)) {
							continue;
						}

						int start = keyGroupRangeFromOldState.getStartKeyGroup();
						int end = keyGroupRangeFromOldState.getEndKeyGroup();

						try (FSDataInputStream fsDataInputStream = keyGroupsStateHandle.openInputStream()) {
							DataInputViewStreamWrapper inView = new DataInputViewStreamWrapper(fsDataInputStream);

							for (int alignedOldKeyGroup = start; alignedOldKeyGroup <= end; alignedOldKeyGroup++) {
								long offset = keyGroupsStateHandle.getOffsetForKeyGroup(alignedOldKeyGroup);

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
									hashedKeyGroup, keyGroupsStateHandle);
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
		return hashedKeyGroupToHandle;
	}

	private Map.Entry<OperatorID, OperatorSubtaskState> getOperatorSubtaskState(JobManagerTaskRestore stateRestore) {

		Preconditions.checkState(stateRestore != null, "++++++ new state restore should not be null");

		Set<Map.Entry<OperatorID, OperatorSubtaskState>> subtaskStateMappings
			= stateRestore.getTaskStateSnapshot().getSubtaskStateMappings();

		// By default, there will only be one Operator inside the OperatorChain.
		for (Map.Entry<OperatorID, OperatorSubtaskState> subtaskStateEntry : subtaskStateMappings) {
			return subtaskStateEntry;
		}

		return null;
	}
}
