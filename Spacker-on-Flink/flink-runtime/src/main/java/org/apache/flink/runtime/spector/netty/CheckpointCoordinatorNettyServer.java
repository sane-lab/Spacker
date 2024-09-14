package org.apache.flink.runtime.spector.netty;

import org.apache.flink.runtime.checkpoint.CheckpointCoordinatorGateway;
import org.apache.flink.runtime.spector.netty.codec.TaskAcknowledgementDecoder;
import org.apache.flink.runtime.spector.netty.codec.TaskAcknowledgementEncoder;
import org.apache.flink.runtime.spector.netty.socket.NettySocketServer;
import org.apache.flink.shaded.netty4.io.netty.channel.ChannelPipeline;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.serialization.ClassResolvers;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.serialization.ObjectDecoder;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.serialization.ObjectEncoder;
import org.apache.flink.util.Preconditions;

import java.io.Closeable;
import java.io.IOException;
import java.util.function.Consumer;
import java.util.function.Supplier;

public class CheckpointCoordinatorNettyServer implements Closeable {
	private final NettySocketServer nettySocketServer;

	public CheckpointCoordinatorNettyServer(
		Supplier<CheckpointCoordinatorGateway> gatewaySupplier,
		String address,
		boolean ackOptEnabled,
		boolean ackChunkEnabled) {
		Consumer<ChannelPipeline> channelPipelineConsumer;
		if (ackChunkEnabled) {
			Preconditions.checkState(!ackOptEnabled,
				"++++++ Netty Serde optimization and Chunk cannot set true simultaneously");
			channelPipelineConsumer = channelPipeline -> channelPipeline.addLast(
				new ObjectEncoder(),
				new ObjectDecoder(Integer.MAX_VALUE, ClassResolvers.cacheDisabled(null)),
				new CheckpointCoordinatorServerHandlerChunked(gatewaySupplier.get()));
		} else {
			if (ackOptEnabled) {
				channelPipelineConsumer = channelPipeline -> channelPipeline.addLast(
					new TaskAcknowledgementEncoder(),
					new TaskAcknowledgementDecoder(),
					new CheckpointCoordinatorServerHandlerNonChunked(gatewaySupplier.get()));

			} else {
				channelPipelineConsumer = channelPipeline -> channelPipeline.addLast(
					new ObjectEncoder(),
					new ObjectDecoder(Integer.MAX_VALUE, ClassResolvers.cacheDisabled(null)),
					new CheckpointCoordinatorServerHandlerNonChunked(gatewaySupplier.get()));
			}
		}

		this.nettySocketServer = new NettySocketServer(
			"jobmaster",
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
