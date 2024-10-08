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

package org.apache.flink.runtime.executiongraph.utils;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.runtime.checkpoint.CheckpointOptions;
import org.apache.flink.runtime.clusterframework.types.AllocationID;
import org.apache.flink.runtime.concurrent.FutureUtils;
import org.apache.flink.runtime.deployment.TaskDeploymentDescriptor;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.executiongraph.PartitionInfo;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.jobmanager.slots.TaskManagerGateway;
import org.apache.flink.runtime.messages.Acknowledge;
import org.apache.flink.runtime.messages.StackTraceSampleResponse;
import org.apache.flink.runtime.spector.migration.ReconfigOptions;
import org.apache.flink.runtime.state.KeyGroupRange;

import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.function.BiFunction;
import java.util.function.Consumer;

/**
 * A TaskManagerGateway that simply acks the basic operations (deploy, cancel, update) and does not
 * support any more advanced operations.
 */
public class SimpleAckingTaskManagerGateway implements TaskManagerGateway {

	private final String address = UUID.randomUUID().toString();

	private Consumer<TaskDeploymentDescriptor> submitConsumer = ignore -> { };

	private Consumer<ExecutionAttemptID> cancelConsumer = ignore -> { };

	private volatile BiFunction<AllocationID, Throwable, CompletableFuture<Acknowledge>> freeSlotFunction;

	public void setSubmitConsumer(Consumer<TaskDeploymentDescriptor> predicate) {
		submitConsumer = predicate;
	}

	public void setCancelConsumer(Consumer<ExecutionAttemptID> predicate) {
		cancelConsumer = predicate;
	}

	public void setFreeSlotFunction(BiFunction<AllocationID, Throwable, CompletableFuture<Acknowledge>> freeSlotFunction) {
		this.freeSlotFunction = freeSlotFunction;
	}

	@Override
	public String getAddress() {
		return address;
	}

	@Override
	public CompletableFuture<StackTraceSampleResponse> requestStackTraceSample(
			ExecutionAttemptID executionAttemptID,
			int sampleId,
			int numSamples,
			Time delayBetweenSamples,
			int maxStackTraceDepth,
			Time timeout) {
		return FutureUtils.completedExceptionally(new UnsupportedOperationException());
	}

	@Override
	public CompletableFuture<Acknowledge> submitTask(TaskDeploymentDescriptor tdd, Time timeout) {
		submitConsumer.accept(tdd);
		return CompletableFuture.completedFuture(Acknowledge.get());
	}

	@Override
	public CompletableFuture<Acknowledge> reconfigTask(ExecutionAttemptID executionAttemptID, TaskDeploymentDescriptor tdd, ReconfigOptions reconfigOptions, Time timeout) {
		return CompletableFuture.completedFuture(Acknowledge.get());
	}

	@Override
	public CompletableFuture<Acknowledge> stopTask(ExecutionAttemptID executionAttemptID, Time timeout) {
		return CompletableFuture.completedFuture(Acknowledge.get());
	}

	@Override
	public CompletableFuture<Acknowledge> cancelTask(ExecutionAttemptID executionAttemptID, Time timeout) {
		cancelConsumer.accept(executionAttemptID);
		return CompletableFuture.completedFuture(Acknowledge.get());
	}

	@Override
	public CompletableFuture<Acknowledge> updatePartitions(ExecutionAttemptID executionAttemptID, Iterable<PartitionInfo> partitionInfos, Time timeout) {
		return CompletableFuture.completedFuture(Acknowledge.get());
	}

	@Override
	public void failPartition(ExecutionAttemptID executionAttemptID) {}

	@Override
	public void notifyCheckpointComplete(
			ExecutionAttemptID executionAttemptID,
			JobID jobId,
			long checkpointId,
			long timestamp) {}

	@Override
	public void triggerCheckpoint(
            ExecutionAttemptID executionAttemptID,
            JobID jobId,
            long checkpointId,
            long timestamp,
            CheckpointOptions checkpointOptions) {}

	@Override
	public CompletableFuture<Acknowledge> freeSlot(AllocationID allocationId, Throwable cause, Time timeout) {
		final BiFunction<AllocationID, Throwable, CompletableFuture<Acknowledge>> currentFreeSlotFunction = freeSlotFunction;

		if (currentFreeSlotFunction != null) {
			return currentFreeSlotFunction.apply(allocationId, cause);
		} else {
			return CompletableFuture.completedFuture(Acknowledge.get());
		}
	}

	@Override
	public CompletableFuture<Acknowledge> dispatchStateToTask(ExecutionAttemptID executionAttemptID, JobVertexID jobvertexId, KeyGroupRange keyGroupRange, int idInModel, Time timeout) {
		return null;
	}

	@Override
	public CompletableFuture<Acknowledge> dispatchStandbyTaskGatewaysToTask(ExecutionAttemptID executionAttemptID, JobVertexID jobvertexId, List<String> standbyTaskGateways, Time timeout) {
		return null;
	}

	@Override
	public CompletableFuture<Acknowledge> updateBackupKeyGroupsToTask(ExecutionAttemptID attemptId, JobVertexID jobvertexId, Set<Integer> backupKeyGroups, Time rpcTimeout) {
		return null;
	}

	@Override
	public CompletableFuture<Acknowledge> testRPC(ExecutionAttemptID executionAttemptID, JobVertexID jobvertexId, String requestId, Time timeout) {
		return null;
	}
}
