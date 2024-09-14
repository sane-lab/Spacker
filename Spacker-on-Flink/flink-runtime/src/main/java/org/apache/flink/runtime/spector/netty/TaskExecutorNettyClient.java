package org.apache.flink.runtime.spector.netty;

import org.apache.commons.lang3.RandomUtils;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.runtime.checkpoint.JobManagerTaskRestore;
import org.apache.flink.runtime.deployment.TaskDeploymentDescriptor;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.jobmaster.JobMasterId;
import org.apache.flink.runtime.messages.Acknowledge;
import org.apache.flink.runtime.spector.migration.ReconfigOptions;
import org.apache.flink.runtime.spector.netty.codec.TaskBackupStateDecoder;
import org.apache.flink.runtime.spector.netty.codec.TaskBackupStateEncoder;
import org.apache.flink.runtime.spector.netty.data.TaskRPC;
import org.apache.flink.runtime.spector.netty.data.TaskState;
import org.apache.flink.runtime.spector.netty.data.TaskDeployment;
import org.apache.flink.runtime.spector.netty.data.TaskExecutorSocketAddress;
import org.apache.flink.runtime.spector.netty.socket.NettySocketClient;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.shaded.netty4.io.netty.channel.Channel;
import org.apache.flink.shaded.netty4.io.netty.channel.ChannelFutureListener;
import org.apache.flink.shaded.netty4.io.netty.channel.ChannelPipeline;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.serialization.ClassResolvers;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.serialization.ObjectDecoder;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.serialization.ObjectEncoder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;

import static org.apache.flink.runtime.spector.netty.utils.NettySocketUtils.chunkedWriteAndFlush;

public class TaskExecutorNettyClient implements Closeable {
	private static final Logger LOG = LoggerFactory.getLogger(TaskExecutorNettyClient.class);

	private final TaskExecutorSocketAddress socketAddress;
	private final int channelCount;
	private final int connectTimeoutMills;
	private final int lowWaterMark;
	private final int highWaterMark;
	private final List<NettySocketClient> clientList;
	private final boolean deploymentOptEnabled;
	private final boolean deploymentChunkEnabled;

	private final Object lock = new Object();

	public TaskExecutorNettyClient(
		TaskExecutorSocketAddress socketAddress,
		int channelCount,
		int connectTimeoutMills,
		int lowWaterMark,
		int highWaterMark,
		boolean deploymentOptEnabled,
		boolean deploymentChunkEnabled) {
		this.socketAddress = socketAddress;
		this.channelCount = channelCount;
		this.connectTimeoutMills = connectTimeoutMills;
		this.lowWaterMark = lowWaterMark;
		this.highWaterMark = highWaterMark;
		this.clientList = new ArrayList<>(channelCount);
		this.deploymentOptEnabled = deploymentOptEnabled;
		this.deploymentChunkEnabled = deploymentChunkEnabled;
	}

	public void start() throws Exception {
		Consumer<ChannelPipeline> channelPipelineConsumer;
		if (deploymentOptEnabled) {
			channelPipelineConsumer = channelPipeline -> channelPipeline
				.addLast(new TaskBackupStateEncoder())
				.addLast(new TaskBackupStateDecoder());
		} else {
			channelPipelineConsumer = channelPipeline -> channelPipeline
				.addLast(
				new ObjectEncoder(),
				new ObjectDecoder(Integer.MAX_VALUE, ClassResolvers.cacheDisabled(null)));
		}

		for (int i = 0; i < channelCount; i++) {
			NettySocketClient nettySocketClient = new NettySocketClient(
				socketAddress.getAddress(),
				socketAddress.getPort(),
				connectTimeoutMills,
				lowWaterMark,
				highWaterMark,
				channelPipelineConsumer);
			nettySocketClient.start();
			clientList.add(nettySocketClient);
		}
	}

	public CompletableFuture<Acknowledge> reconfigTask(
		ExecutionAttemptID executionAttemptID,
		TaskDeploymentDescriptor tdd,
		JobMasterId jobMasterId,
		ReconfigOptions reconfigOptions,
		Time timeout) {
		CompletableFuture<Acknowledge> submitFuture = new CompletableFuture<>();
		Channel channel = clientList.get(RandomUtils.nextInt(0, clientList.size())).getChannel();
		while (true) {
			if (channel.isWritable()) {
				TaskDeployment taskDeployment = new TaskDeployment(executionAttemptID, tdd, jobMasterId, reconfigOptions, timeout);
				if (deploymentOptEnabled) {
					try {
						chunkedWriteAndFlush(submitFuture, channel, taskDeployment, executionAttemptID);
					} catch (Exception e) {
						e.printStackTrace();
					}
				} else {
					channel.writeAndFlush(taskDeployment)
						.addListener((ChannelFutureListener) channelFuture -> {
							if (channelFuture.isSuccess()) {
								submitFuture.complete(Acknowledge.get());
							} else {
								submitFuture.completeExceptionally(channelFuture.cause());
							}
						});
				}
				break;
			}
			try {
				Thread.sleep(10);
			} catch (InterruptedException ignored) { }
		}
		return submitFuture;
	}

	public CompletableFuture<Acknowledge> testRPC(
		ExecutionAttemptID executionAttemptID,
		JobVertexID jobvertexId,
		String requestId, Time timeout) {
		CompletableFuture<Acknowledge> submitFuture = new CompletableFuture<>();
		Channel channel = clientList.get(RandomUtils.nextInt(0, clientList.size())).getChannel();
		while (true) {
			if (channel.isWritable()) {
				TaskRPC taskRPC = new TaskRPC(
					executionAttemptID,
					jobvertexId,
					requestId,
					timeout);
				if (deploymentChunkEnabled) {
					try {
						chunkedWriteAndFlush(submitFuture, channel, taskRPC, executionAttemptID);
					} catch (Exception e) {
						throw new RuntimeException(e);
					}
				} else {
					channel.writeAndFlush(taskRPC)
						.addListener((ChannelFutureListener) channelFuture -> {
							if (channelFuture.isSuccess()) {
								submitFuture.complete(Acknowledge.get());
							} else {
								submitFuture.completeExceptionally(channelFuture.cause());
							}
						});
				}
				break;
			}
			try {
				Thread.sleep(10);
			} catch (InterruptedException ignored) { }
		}
		return submitFuture;
	}

	public CompletableFuture<Acknowledge> dispatchStateToTask(
		ExecutionAttemptID executionAttemptID,
		JobVertexID jobvertexId,
		JobManagerTaskRestore taskRestore,
		KeyGroupRange keyGroupRange, int idInModel, Time timeout) {
		CompletableFuture<Acknowledge> submitFuture = new CompletableFuture<>();
		Channel channel = clientList.get(RandomUtils.nextInt(0, clientList.size())).getChannel();
		while (true) {
			if (channel.isWritable()) {
				TaskState taskState = new TaskState(
					executionAttemptID,
					jobvertexId,
					taskRestore,
					keyGroupRange,
					idInModel,
					timeout);
				if (deploymentChunkEnabled) {
					try {
						chunkedWriteAndFlush(submitFuture, channel, taskState, executionAttemptID);
					} catch (Exception e) {
						throw new RuntimeException(e);
					}
				} else {
					channel.writeAndFlush(taskState)
						.addListener((ChannelFutureListener) channelFuture -> {
							if (channelFuture.isSuccess()) {
								submitFuture.complete(Acknowledge.get());
							} else {
								submitFuture.completeExceptionally(channelFuture.cause());
							}
						});
				}
				break;
			}
			try {
				Thread.sleep(10);
			} catch (InterruptedException ignored) { }
		}
		return submitFuture;
	}

	@Override
	public void close() {
		for (NettySocketClient nettySocketClient : clientList) {
			try {
				nettySocketClient.closeAsync().get();
			} catch (Exception e) {
				LOG.error("Close the connection to {}:{} failed", socketAddress.getAddress(), socketAddress.getPort(), e);
			}
		}
	}
}
