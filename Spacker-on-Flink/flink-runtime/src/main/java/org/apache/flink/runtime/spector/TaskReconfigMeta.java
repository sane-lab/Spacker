package org.apache.flink.runtime.spector;

import org.apache.flink.runtime.deployment.InputGateDeploymentDescriptor;
import org.apache.flink.runtime.deployment.ResultPartitionDeploymentDescriptor;
import org.apache.flink.runtime.io.network.partition.ResultPartition;
import org.apache.flink.runtime.io.network.partition.consumer.SingleInputGate;
import org.apache.flink.runtime.spector.migration.ReconfigID;
import org.apache.flink.runtime.spector.migration.ReconfigOptions;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.taskexecutor.TaskExecutorGateway;

import java.util.Collection;
import java.util.Map;

import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

class TaskReconfigMeta {
	private final ReconfigID reconfigId;
	private final ReconfigOptions reconfigOptions;

	private final Collection<ResultPartitionDeploymentDescriptor> resultPartitionDeploymentDescriptors;
	private final Collection<InputGateDeploymentDescriptor> inputGateDeploymentDescriptors;

	private final ResultPartition[] newPartitions;

	private final Collection<Integer> srcAffectedKeyGroups;
	private final Collection<Integer> dstAffectedKeyGroups;

	private final KeyGroupRange keyGroupRange;
	private final Map<Integer, TaskExecutorGateway> srcKeyGroupsWithDstGateway;

	TaskReconfigMeta(
		ReconfigID reconfigId,
		ReconfigOptions reconfigOptions,
		Collection<ResultPartitionDeploymentDescriptor> resultPartitionDeploymentDescriptors,
		Collection<InputGateDeploymentDescriptor> inputGateDeploymentDescriptors,
		Collection<Integer> srcAffectedKeyGroups,
		Collection<Integer> dstAffectedKeyGroups,
		Map<Integer, TaskExecutorGateway> srcKeyGroupsWithDstGateway,
		KeyGroupRange keyGroupRange) {

		this.reconfigId = checkNotNull(reconfigId);
		this.reconfigOptions = checkNotNull(reconfigOptions);

		this.resultPartitionDeploymentDescriptors = checkNotNull(resultPartitionDeploymentDescriptors);
		this.inputGateDeploymentDescriptors = checkNotNull(inputGateDeploymentDescriptors);

		this.newPartitions = new ResultPartition[resultPartitionDeploymentDescriptors.size()];

		this.srcAffectedKeyGroups = srcAffectedKeyGroups;
		this.dstAffectedKeyGroups = dstAffectedKeyGroups;

		this.srcKeyGroupsWithDstGateway = srcKeyGroupsWithDstGateway;

		this.keyGroupRange = keyGroupRange;
	}

	public ReconfigID getReconfigId() {
		return reconfigId;
	}

	public ReconfigOptions getReconfigOptions() {
		return reconfigOptions;
	}

	public Collection<ResultPartitionDeploymentDescriptor> getResultPartitionDeploymentDescriptors() {
		return resultPartitionDeploymentDescriptors;
	}

	public Collection<InputGateDeploymentDescriptor> getInputGateDeploymentDescriptors() {
		return inputGateDeploymentDescriptors;
	}

	public ResultPartition getNewPartitions(int index) {
		checkState(index >= 0 && index < newPartitions.length, "given index out of boundary");

		return newPartitions[index];
	}

	public void addNewPartitions(int index, ResultPartition partition) {
		checkState(index >= 0 && index < newPartitions.length, "given index out of boundary");

		newPartitions[index] = partition;
	}

	public Collection<Integer> getSrcAffectedKeyGroups() {
		return srcAffectedKeyGroups;
	}

	public Collection<Integer> getDstAffectedKeyGroups() {
		return dstAffectedKeyGroups;
	}

	public Map<Integer, TaskExecutorGateway> getSrcKeyGroupsWithDstGateway() {
		return srcKeyGroupsWithDstGateway;
	}

	public KeyGroupRange getKeyGroupRange() {
		return keyGroupRange;
	}

	public InputGateDeploymentDescriptor getMatchedInputGateDescriptor(SingleInputGate gate) {
		for (InputGateDeploymentDescriptor igdd : inputGateDeploymentDescriptors) {
			if (gate.getConsumedResultId().equals(igdd.getConsumedResultId())
				&& gate.getConsumedSubpartitionIndex() == igdd.getConsumedSubpartitionIndex()) {
				return igdd;
			}
		}
		throw new IllegalStateException("Cannot find matched InputGateDeploymentDescriptor");
	}
}
