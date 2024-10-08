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

package org.apache.flink.runtime.taskexecutor.rpc;

import org.apache.flink.api.common.JobID;
import org.apache.flink.runtime.checkpoint.CheckpointCoordinatorGateway;
import org.apache.flink.runtime.checkpoint.CheckpointMetrics;
import org.apache.flink.runtime.checkpoint.TaskStateSnapshot;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.messages.checkpoint.DeclineCheckpoint;
import org.apache.flink.runtime.spector.netty.CheckpointCoordinatorNettyClient;
import org.apache.flink.runtime.taskmanager.CheckpointResponder;
import org.apache.flink.util.Preconditions;

public class RpcCheckpointResponder implements CheckpointResponder {

	private final CheckpointCoordinatorGateway checkpointCoordinatorGateway;

	private final boolean nettyStateTransmissionEnabled;

	private final CheckpointCoordinatorNettyClient checkpointCoordinatorNettyClient;

	public RpcCheckpointResponder(CheckpointCoordinatorGateway checkpointCoordinatorGateway) {
		this(checkpointCoordinatorGateway, false, null);
	}

	public RpcCheckpointResponder(
		CheckpointCoordinatorGateway checkpointCoordinatorGateway,
		boolean nettyStateTransmissionEnabled,
		CheckpointCoordinatorNettyClient checkpointCoordinatorNettyClient) {
		this.checkpointCoordinatorGateway = Preconditions.checkNotNull(checkpointCoordinatorGateway);
		this.nettyStateTransmissionEnabled = nettyStateTransmissionEnabled;
		this.checkpointCoordinatorNettyClient = checkpointCoordinatorNettyClient;
	}

	@Override
	public void acknowledgeCheckpoint(
			JobID jobID,
			ExecutionAttemptID executionAttemptID,
			long checkpointId,
			CheckpointMetrics checkpointMetrics,
			TaskStateSnapshot subtaskState) {
		if (nettyStateTransmissionEnabled && checkpointCoordinatorNettyClient != null) {
			checkpointCoordinatorNettyClient.acknowledgeCheckpoint(
				jobID,
				executionAttemptID,
				checkpointId,
				checkpointMetrics,
				subtaskState);
		} else {
			checkpointCoordinatorGateway.acknowledgeCheckpoint(
				jobID,
				executionAttemptID,
				checkpointId,
				checkpointMetrics,
				subtaskState);
		}
	}

	@Override
	public void declineCheckpoint(
			JobID jobID,
			ExecutionAttemptID executionAttemptID,
			long checkpointId,
			Throwable cause) {

		checkpointCoordinatorGateway.declineCheckpoint(new DeclineCheckpoint(jobID,
			executionAttemptID,
			checkpointId,
			cause));
	}
}
