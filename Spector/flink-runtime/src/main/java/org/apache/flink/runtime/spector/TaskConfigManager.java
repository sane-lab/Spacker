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
import org.apache.flink.runtime.deployment.InputChannelDeploymentDescriptor;
import org.apache.flink.runtime.deployment.InputGateDeploymentDescriptor;
import org.apache.flink.runtime.deployment.ResultPartitionDeploymentDescriptor;
import org.apache.flink.runtime.deployment.ResultPartitionLocation;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.io.disk.iomanager.IOManager;
import org.apache.flink.runtime.io.network.NetworkEnvironment;
import org.apache.flink.runtime.io.network.api.writer.ResultPartitionWriter;
import org.apache.flink.runtime.io.network.partition.ResultPartition;
import org.apache.flink.runtime.io.network.partition.ResultPartitionConsumableNotifier;
import org.apache.flink.runtime.io.network.partition.ResultPartitionID;
import org.apache.flink.runtime.io.network.partition.consumer.InputChannel;
import org.apache.flink.runtime.io.network.partition.consumer.LocalInputChannel;
import org.apache.flink.runtime.io.network.partition.consumer.RemoteInputChannel;
import org.apache.flink.runtime.io.network.partition.consumer.SingleInputGate;
import org.apache.flink.runtime.metrics.groups.TaskMetricGroup;
import org.apache.flink.runtime.spector.migration.ReconfigID;
import org.apache.flink.runtime.spector.migration.ReconfigOptions;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.taskmanager.TaskActions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;

import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

public class TaskConfigManager {

	private static final Logger LOG = LoggerFactory.getLogger(TaskConfigManager.class);

	private final JobID jobId;

	private final ExecutionAttemptID executionId;

	private final String taskNameWithSubtaskAndId;

	private final TaskActions taskActions;

	private final NetworkEnvironment network;

	private final IOManager ioManager;

	private final TaskMetricGroup metrics;

	private final ResultPartitionConsumableNotifier resultPartitionConsumableNotifier;

	private volatile TaskReconfigMeta reconfigMeta;

	private volatile ResultPartition[] storedOldWriterCopies;

	public TaskConfigManager(
			JobID jobId,
			ExecutionAttemptID executionId,
			String taskNameWithSubtaskAndId,
			TaskActions taskActions,
			NetworkEnvironment network,
			IOManager ioManager,
			TaskMetricGroup metrics,
			ResultPartitionConsumableNotifier notifier) {

		this.jobId = checkNotNull(jobId);
		this.executionId = checkNotNull(executionId);
		this.taskNameWithSubtaskAndId = checkNotNull(taskNameWithSubtaskAndId);
		this.taskActions = checkNotNull(taskActions);
		this.network = checkNotNull(network);
		this.ioManager = checkNotNull(ioManager);
		this.metrics = checkNotNull(metrics);
		this.resultPartitionConsumableNotifier = checkNotNull(notifier);
	}

	public void prepareReconfigMeta(
		ReconfigID reconfigId,
		ReconfigOptions reconfigOptions,
		Collection<ResultPartitionDeploymentDescriptor> resultPartitionDeploymentDescriptors,
		Collection<InputGateDeploymentDescriptor> inputGateDeploymentDescriptors,
		Collection<Integer> srcAffectedKeygroups,
		Collection<Integer> dstAffectedKeygroups, KeyGroupRange keyGroupRange) {

		TaskReconfigMeta meta = new TaskReconfigMeta(
			reconfigId,
			reconfigOptions,
			resultPartitionDeploymentDescriptors,
			inputGateDeploymentDescriptors,
			srcAffectedKeygroups,
			dstAffectedKeygroups,
			keyGroupRange);

		long timeStart = System.currentTimeMillis();
		while (reconfigMeta != null) {
			if (System.currentTimeMillis() - timeStart > 1000) {
				throw new IllegalStateException("One reconfig is in process, cannot prepare another reconfigMeta for " + taskNameWithSubtaskAndId);
			}
		}
		reconfigMeta = meta;
	}

	public boolean isReconfigTarget() {
		return reconfigMeta != null;
	}

	public boolean isSourceOrDestination() {
		try {
			return reconfigMeta.getSrcAffectedKeygroups() != null || reconfigMeta.getDstAffectedKeygroups() != null;
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

	public boolean isSource() {
		try {
			return reconfigMeta.getSrcAffectedKeygroups() != null;
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

	public boolean isDestination() {
		try {
			return reconfigMeta.getDstAffectedKeygroups() != null;
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

	public boolean isUpdatePartitions() {
		return reconfigMeta.getReconfigOptions().isUpdatingPartitions();
	}

	public boolean isUpdateGates() {
		return reconfigMeta.getReconfigOptions().isUpdatingGates();
	}


	public boolean isUpdatingKeyGroupRange() {
		return reconfigMeta.getReconfigOptions().isUpdatingKeyGroupRange();
	}

	public KeyGroupRange getKeyGroupRange() {
		return reconfigMeta.getKeyGroupRange();
	}

	public void createNewResultPartitions() throws IOException {
		int counter = 0;
		for (ResultPartitionDeploymentDescriptor desc : reconfigMeta.getResultPartitionDeploymentDescriptors()) {
			ResultPartitionID partitionId = new ResultPartitionID(desc.getPartitionId(), executionId, reconfigMeta.getReconfigId());

			ResultPartition newPartition = new ResultPartition(
				taskNameWithSubtaskAndId,
				taskActions,
				jobId,
				partitionId,
				desc.getPartitionType(),
				desc.getNumberOfSubpartitions(),
				desc.getMaxParallelism(),
				network.getResultPartitionManager(),
				resultPartitionConsumableNotifier,
				ioManager,
				desc.sendScheduleOrUpdateConsumersMessage());

			reconfigMeta.addNewPartitions(counter, newPartition);
			network.setupPartition(newPartition);

			++counter;
		}
	}

	public ResultPartitionWriter[] substituteResultPartitions(ResultPartitionWriter[] oldWriters) {
		ResultPartitionWriter[] oldWriterCopies = Arrays.copyOf(oldWriters, oldWriters.length);

		for (int i = 0; i < oldWriters.length; i++) {
			oldWriters[i] = reconfigMeta.getNewPartitions(i);
		}

		return oldWriterCopies;
	}

	// We cannot do it immediately because downstream's gate is still polling from the old partitions (barrier haven't pass to downstream)
	// so we store the oldWriterCopies and unregister them in next scaling.
	public void unregisterPartitions(ResultPartition[] oldWriterCopies) {
		if (storedOldWriterCopies != null) {
			network.unregisterPartitions(storedOldWriterCopies);
		}

		storedOldWriterCopies = oldWriterCopies;
	}

	public void substituteInputGateChannels(SingleInputGate inputGate) throws IOException, InterruptedException {
		checkNotNull(reconfigMeta, "reconfig component cannot be null");

		InputGateDeploymentDescriptor igdd = reconfigMeta.getMatchedInputGateDescriptor(inputGate);
		InputChannelDeploymentDescriptor[] icdd = checkNotNull(igdd.getInputChannelDeploymentDescriptors());

//		inputGate.releaseAllResources();
		inputGate.reset(icdd.length);

		createChannel(inputGate, icdd);

//		network.setupInputGate(inputGate);

		inputGate.requestPartitions();
	}

	private void createChannel(SingleInputGate inputGate, InputChannelDeploymentDescriptor[] icdd) {
		for (int i = 0; i < icdd.length; i++) {
			final ResultPartitionID partitionId = icdd[i].getConsumedPartitionId();
			final ResultPartitionLocation partitionLocation = icdd[i].getConsumedPartitionLocation();

			InputChannel inputChannel = null;
			if (partitionLocation.isLocal()) {
				inputChannel = new LocalInputChannel(inputGate, i, partitionId,
					network.getResultPartitionManager(),
					network.getTaskEventDispatcher(),
					network.getPartitionRequestInitialBackoff(),
					network.getPartitionRequestMaxBackoff(),
					metrics.getIOMetricGroup()
				);
			}
			else if (partitionLocation.isRemote()) {
				inputChannel = new RemoteInputChannel(inputGate, i, partitionId,
					partitionLocation.getConnectionId(),
					network.getConnectionManager(),
					network.getPartitionRequestInitialBackoff(),
					network.getPartitionRequestMaxBackoff(),
					metrics.getIOMetricGroup()
				);
			}
			else {
				throw new IllegalStateException("Unexpected partition location.");
			}

			inputGate.setInputChannel(partitionId.getPartitionId(), inputChannel);
		}
	}

	public void finish() {
		this.reconfigMeta = null;
		LOG.info("++++++ taskConfigManager finish, set meta to null for task " + taskNameWithSubtaskAndId);
	}

	public NetworkEnvironment getNetwork() {
		return network;
	}

	public Collection<Integer> getSrcAffectedKeygroups() {
		return reconfigMeta.getSrcAffectedKeygroups();
	}

	public Collection<Integer> getDstAffectedKeygroups() {
		return reconfigMeta.getDstAffectedKeygroups();
	}
}
