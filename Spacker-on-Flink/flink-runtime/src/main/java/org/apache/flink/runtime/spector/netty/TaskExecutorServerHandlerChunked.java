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

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.memory.DataInputViewStreamWrapper;
import org.apache.flink.runtime.messages.Acknowledge;
import org.apache.flink.runtime.spector.netty.data.TaskDeployment;
import org.apache.flink.runtime.spector.netty.data.TaskRPC;
import org.apache.flink.runtime.spector.netty.data.TaskState;
import org.apache.flink.runtime.spector.netty.utils.NettySocketUtils.NettyMessageType;
import org.apache.flink.runtime.taskexecutor.TaskExecutorGateway;
import org.apache.flink.shaded.netty4.io.netty.channel.ChannelHandlerContext;
import org.apache.flink.shaded.netty4.io.netty.channel.ChannelInboundHandlerAdapter;

import java.io.ByteArrayInputStream;
import java.io.ObjectInputStream;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;

import static org.apache.flink.runtime.spector.netty.utils.NettySocketUtils.NettyMessageType.*;
import static org.apache.flink.runtime.spector.netty.utils.NettySocketUtils.chunkedChannelRead;

/**
 * Server handler in task executor netty server.
 */
public class TaskExecutorServerHandlerChunked extends ChannelInboundHandlerAdapter {
	private final TaskExecutorGateway taskExecutorGateway;

	// ExecutionAttemptID -> State Byte Array
	private final Map<String, byte[]> recv = new ConcurrentHashMap<>();
	// ExecutionAttemptID -> Latest Position of Byte Array
	private final Map<String, Tuple2<String, Integer>> metadata = new ConcurrentHashMap<>();

	public TaskExecutorServerHandlerChunked(TaskExecutorGateway taskExecutorGateway) {
		this.taskExecutorGateway = taskExecutorGateway;
	}

	@Override
	public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
		chunkedChannelRead(msg, this::fireAck, ctx, recv, metadata);
	}

	private void fireAck(byte[] bytes, String eventType) {
		try {
			if (NettyMessageType.valueOf(eventType) == TASK_STATE_RESTORE) {
				TaskState taskState = new TaskState();
				taskState.read(new DataInputViewStreamWrapper(new ByteArrayInputStream(bytes)));
				CompletableFuture<Acknowledge> future = taskExecutorGateway.dispatchStateToTask(
					taskState.getExecutionAttemptID(),
					taskState.getJobvertexId(),
                        taskState.getKeyGroupRange(),
					taskState.getIdInModel(),
					taskState.getTimeout());
				future.whenCompleteAsync((ack, failure) -> {
					if (failure != null) {
						throw new RuntimeException();
					}
				});
			} else if (NettyMessageType.valueOf(eventType) == TASK_DEPLOYMENT) {
//				TaskDeployment taskDeployment = new TaskDeployment();
//				taskDeployment.read(new DataInputViewStreamWrapper(new ByteArrayInputStream(bytes)));
				ObjectInputStream ois = new ObjectInputStream(new ByteArrayInputStream(bytes));
				TaskDeployment taskDeployment = (TaskDeployment) ois.readObject();
				CompletableFuture<Acknowledge> future = taskExecutorGateway.reconfigTask(
					taskDeployment.getExecutionAttemptID(),
					taskDeployment.getTaskDeploymentDescriptor(),
					taskDeployment.getJobMasterId(),
					taskDeployment.getReconfigOptions(),
					taskDeployment.getTimeout());
				future.whenCompleteAsync((ack, failure) -> {
					if (failure != null) {
						throw new RuntimeException();
					}
				});
//				throw new UnsupportedOperationException();
			} else if (NettyMessageType.valueOf(eventType) == TASK_RPC) {
				TaskRPC taskRPC = new TaskRPC();
				taskRPC.read(new DataInputViewStreamWrapper(new ByteArrayInputStream(bytes)));
				CompletableFuture<Acknowledge> future = taskExecutorGateway.testRPC(
					taskRPC.getExecutionAttemptID(),
					taskRPC.getJobvertexId(),
					taskRPC.getRequestId(),
					taskRPC.getTimeout());
//				future.whenCompleteAsync((ack, failure) -> {
//					if (failure != null) {
//						throw new RuntimeException();
//					}
//				});
			} else {
				throw new UnsupportedOperationException();
			}
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}
}
