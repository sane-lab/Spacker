package org.apache.flink.runtime.spector.netty;

import org.apache.flink.runtime.spector.netty.codec.TaskBackupStateDecoder;
import org.apache.flink.runtime.spector.netty.codec.TaskBackupStateEncoder;
import org.apache.flink.runtime.spector.netty.socket.NettySocketServer;
import org.apache.flink.runtime.taskexecutor.TaskExecutorGateway;
import org.apache.flink.shaded.netty4.io.netty.channel.ChannelPipeline;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.serialization.ClassResolvers;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.serialization.ObjectDecoder;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.serialization.ObjectEncoder;
import org.apache.flink.util.Preconditions;

import java.io.Closeable;
import java.io.IOException;
import java.util.function.Consumer;
import java.util.function.Supplier;

public class TaskExecutorNettyServer implements Closeable {
	private final NettySocketServer nettySocketServer;

	public TaskExecutorNettyServer(
		Supplier<TaskExecutorGateway> gatewaySupplier,
		String address,
		boolean deploymentOptEnabled,
		boolean deploymentChunkEnabled) {
		Consumer<ChannelPipeline> channelPipelineConsumer;
		if (deploymentChunkEnabled) {
			Preconditions.checkState(!deploymentOptEnabled,
				"++++++ Netty Serde optimization and Chunk cannot set true simultaneously");
			channelPipelineConsumer = channelPipeline -> channelPipeline.addLast(
				new ObjectEncoder(),
				new ObjectDecoder(Integer.MAX_VALUE, ClassResolvers.cacheDisabled(null)),
				new TaskExecutorServerHandlerChunked(gatewaySupplier.get()));
		} else {
			if (deploymentOptEnabled) {
				channelPipelineConsumer = channelPipeline -> channelPipeline.addLast(
					new TaskBackupStateEncoder(),
					new TaskBackupStateDecoder(),
					new TaskExecutorServerHandlerNonChunked(gatewaySupplier.get()));
			} else {
				channelPipelineConsumer = channelPipeline -> channelPipeline.addLast(
					new ObjectEncoder(),
					new ObjectDecoder(Integer.MAX_VALUE, ClassResolvers.cacheDisabled(null)),
					new TaskExecutorServerHandlerNonChunked(gatewaySupplier.get()));
			}
		}

		this.nettySocketServer = new NettySocketServer(
			"taskexecutor",
			address,
			"0",
			channelPipelineConsumer,
			0);
	}

	public void start() throws Exception {
		nettySocketServer.start();
	}

	public String getAddress() {
		return nettySocketServer.getAddress();
	}

	public int getPort() {
		return nettySocketServer.getPort();
	}

	@Override
	public void close() throws IOException {
		try {
			nettySocketServer.closeAsync().get();
		} catch (Exception e) {
			throw new IOException(e);
		}
	}
}
