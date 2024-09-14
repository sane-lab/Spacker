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
import org.apache.flink.runtime.checkpoint.CheckpointCoordinatorGateway;
import org.apache.flink.runtime.spector.netty.data.TaskAcknowledgement;
import org.apache.flink.runtime.spector.netty.utils.NettySocketUtils.NettyMessageType;
import org.apache.flink.shaded.netty4.io.netty.channel.ChannelHandlerContext;
import org.apache.flink.shaded.netty4.io.netty.channel.ChannelInboundHandlerAdapter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static org.apache.flink.runtime.spector.netty.utils.NettySocketUtils.NettyMessageType.TASK_ACKNOWLEDGEMENT;
import static org.apache.flink.runtime.spector.netty.utils.NettySocketUtils.chunkedChannelRead;

/**
 * Server handler in task executor netty server.
 */
public class CheckpointCoordinatorServerHandlerChunked extends ChannelInboundHandlerAdapter {
	private static final Logger LOG = LoggerFactory.getLogger(CheckpointCoordinatorServerHandlerChunked.class);

	private final CheckpointCoordinatorGateway checkpointCoordinatorGateway;

	private final Map<String, byte[]> recv = new ConcurrentHashMap<>();
	private final Map<String, Tuple2<String, Integer>> metadata = new ConcurrentHashMap<>();

	public CheckpointCoordinatorServerHandlerChunked(CheckpointCoordinatorGateway checkpointCoordinatorGateway) {
		this.checkpointCoordinatorGateway = checkpointCoordinatorGateway;
	}

	@Override
	public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
		chunkedChannelRead(msg, this::fireAck, ctx, recv, metadata);
	}

	public void fireAck(byte[] bytes, String eventType) {
		try {
			if (NettyMessageType.valueOf(eventType) == TASK_ACKNOWLEDGEMENT) {
				TaskAcknowledgement taskAcknowledgement = new TaskAcknowledgement();
				taskAcknowledgement.read(new DataInputViewStreamWrapper(new ByteArrayInputStream(bytes)));
				checkpointCoordinatorGateway.acknowledgeCheckpoint(
					taskAcknowledgement.getJobID(),
					taskAcknowledgement.getExecutionAttemptID(),
					taskAcknowledgement.getCheckpointId(),
					taskAcknowledgement.getCheckpointMetrics(),
					taskAcknowledgement.getSubtaskState());
			} else {
				throw new UnsupportedOperationException();
			}
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}
}
