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

import org.apache.flink.runtime.checkpoint.CheckpointCoordinatorGateway;
import org.apache.flink.runtime.spector.netty.data.TaskAcknowledgement;
import org.apache.flink.shaded.netty4.io.netty.channel.ChannelHandlerContext;
import org.apache.flink.shaded.netty4.io.netty.channel.ChannelInboundHandlerAdapter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Server handler in task executor netty server.
 */
public class CheckpointCoordinatorServerHandlerNoChunk extends ChannelInboundHandlerAdapter {
	private static final Logger LOG = LoggerFactory.getLogger(CheckpointCoordinatorServerHandlerNoChunk.class);

	private final CheckpointCoordinatorGateway checkpointCoordinatorGateway;

	public CheckpointCoordinatorServerHandlerNoChunk(CheckpointCoordinatorGateway checkpointCoordinatorGateway) {
		this.checkpointCoordinatorGateway = checkpointCoordinatorGateway;
	}

	@Override
	public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
		if (msg instanceof TaskAcknowledgement) {
			LOG.debug("++++++ Receive TaskAcknowledgement");
			TaskAcknowledgement taskAcknowledgement = (TaskAcknowledgement) msg;
			checkpointCoordinatorGateway.acknowledgeCheckpoint(
				taskAcknowledgement.getJobID(),
				taskAcknowledgement.getExecutionAttemptID(),
				taskAcknowledgement.getCheckpointId(),
				taskAcknowledgement.getCheckpointMetrics(),
				taskAcknowledgement.getSubtaskState());
		} else {
			throw new UnsupportedOperationException();
		}
	}
}
