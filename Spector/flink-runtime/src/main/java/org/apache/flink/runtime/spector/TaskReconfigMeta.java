package org.apache.flink.runtime.spector;

import org.apache.flink.runtime.deployment.InputGateDeploymentDescriptor;
import org.apache.flink.runtime.deployment.ResultPartitionDeploymentDescriptor;
import org.apache.flink.runtime.io.network.partition.ResultPartition;
import org.apache.flink.runtime.io.network.partition.consumer.SingleInputGate;
import org.apache.flink.runtime.spector.migration.ReconfigID;
import org.apache.flink.runtime.spector.migration.ReconfigOptions;
import org.apache.flink.runtime.state.KeyGroupRange;

import java.util.Collection;

import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

class TaskReconfigMeta {
	private final ReconfigID reconfigId;
	private final ReconfigOptions reconfigOptions;

	private final Collection<ResultPartitionDeploymentDescriptor> resultPartitionDeploymentDescriptors;
	private final Collection<InputGateDeploymentDescriptor> inputGateDeploymentDescriptors;

	private final ResultPartition[] newPartitions;

	private final Collection<Integer> srcAffectedKeygroups;
	private final Collection<Integer> dstAffectedKeygroups;

	private final KeyGroupRange keyGroupRange;

	TaskReconfigMeta(
		ReconfigID reconfigId,
		ReconfigOptions reconfigOptions,
		Collection<ResultPartitionDeploymentDescriptor> resultPartitionDeploymentDescriptors,
		Collection<InputGateDeploymentDescriptor> inputGateDeploymentDescriptors,
		Collection<Integer> srcAffectedKeygroups,
		Collection<Integer> dstAffectedKeygroups, KeyGroupRange keyGroupRange) {

		this.reconfigId = checkNotNull(reconfigId);
		this.reconfigOptions = checkNotNull(reconfigOptions);

		this.resultPartitionDeploymentDescriptors = checkNotNull(resultPartitionDeploymentDescriptors);
		this.inputGateDeploymentDescriptors = checkNotNull(inputGateDeploymentDescriptors);

		this.newPartitions = new ResultPartition[resultPartitionDeploymentDescriptors.size()];

		this.srcAffectedKeygroups = srcAffectedKeygroups;
		this.dstAffectedKeygroups = dstAffectedKeygroups;
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

	public Collection<Integer> getSrcAffectedKeygroups() {
		return srcAffectedKeygroups;
	}

	public Collection<Integer> getDstAffectedKeygroups() {
		return dstAffectedKeygroups;
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
