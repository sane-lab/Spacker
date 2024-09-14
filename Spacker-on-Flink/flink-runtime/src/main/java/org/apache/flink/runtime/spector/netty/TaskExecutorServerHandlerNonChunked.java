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

package org.apache.flink.runtime.spector.netty;

import org.apache.flink.api.common.time.Time;
import org.apache.flink.runtime.spector.netty.data.TaskState;
import org.apache.flink.runtime.spector.netty.data.TaskDeployment;
import org.apache.flink.runtime.taskexecutor.TaskExecutorGateway;
import org.apache.flink.shaded.netty4.io.netty.channel.ChannelHandlerContext;
import org.apache.flink.shaded.netty4.io.netty.channel.ChannelInboundHandlerAdapter;

/**
 * Server handler in task executor netty server.
 */
public class TaskExecutorServerHandlerNonChunked extends ChannelInboundHandlerAdapter {
	private static final Time DEFAULT_RPC_TIMEOUT = Time.seconds(10);
	private final TaskExecutorGateway taskExecutorGateway;

	public TaskExecutorServerHandlerNonChunked(TaskExecutorGateway taskExecutorGateway) {
		this.taskExecutorGateway = taskExecutorGateway;
	}

	@Override
	public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
		if (msg instanceof TaskDeployment) {
			TaskDeployment taskDeployment = (TaskDeployment) msg;
			taskExecutorGateway.reconfigTask(
				taskDeployment.getExecutionAttemptID(),
				taskDeployment.getTaskDeploymentDescriptor(),
				taskDeployment.getJobMasterId(),
				taskDeployment.getReconfigOptions(),
				DEFAULT_RPC_TIMEOUT);
		} else if (msg instanceof TaskState) {
			TaskState taskState = (TaskState) msg;
			taskExecutorGateway.dispatchStateToTask(
				taskState.getExecutionAttemptID(),
				taskState.getJobvertexId(),
                    taskState.getKeyGroupRange(),
				taskState.getIdInModel(),
				taskState.getTimeout()
			);
		}  else {
			throw new UnsupportedOperationException();
		}
	}
}
