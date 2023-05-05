package org.apache.flink.runtime.spector.netty.socket;

import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.IllegalConfigurationException;
import org.apache.flink.queryablestate.network.NettyBufferPool;
import org.apache.flink.runtime.concurrent.FutureUtils;
import org.apache.flink.runtime.util.ExecutorThreadFactory;
import org.apache.flink.shaded.guava18.com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.flink.shaded.netty4.io.netty.bootstrap.ServerBootstrap;
import org.apache.flink.shaded.netty4.io.netty.bootstrap.ServerBootstrapConfig;
import org.apache.flink.shaded.netty4.io.netty.channel.*;
import org.apache.flink.shaded.netty4.io.netty.channel.nio.NioEventLoopGroup;
import org.apache.flink.shaded.netty4.io.netty.channel.socket.SocketChannel;
import org.apache.flink.shaded.netty4.io.netty.channel.socket.nio.NioServerSocketChannel;
import org.apache.flink.util.AutoCloseableAsync;
import org.apache.flink.util.NetUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.BindException;
import java.net.InetSocketAddress;
import java.util.Arrays;
import java.util.Iterator;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;


public class NettySocketServer implements AutoCloseableAsync {
	private static final Logger LOG = LoggerFactory.getLogger(NettySocketServer.class);

	/** AbstractServerBase config: low water mark. */
	private static final int LOW_WATER_MARK = 8 * 1024;

	/** AbstractServerBase config: high water mark. */
	private static final int HIGH_WATER_MARK = 32 * 1024;

	private final String tag;

	private final Consumer<ChannelPipeline> channelPipelineConsumer;

	private final int workerCount;

	private final String portRange;

	private String address;

	private int port;

	private ServerBootstrap bootstrap;

	private Channel serverChannel;

	public NettySocketServer(
		String tag,
		String address,
		String portRange,
		Consumer<ChannelPipeline> channelPipelineConsumer,
		int workerCount) {
		this.tag = tag;
		this.address = address;
		this.portRange = portRange;
		this.channelPipelineConsumer = channelPipelineConsumer;
		this.workerCount = workerCount;
	}

	public String getAddress() {
		return address;
	}

	public int getPort() {
		return port;
	}

	public void start() throws Exception {
		NioEventLoopGroup bossGroup = new NioEventLoopGroup(1, new ExecutorThreadFactory("flink-" + tag + "-server-netty-boss"));
		NioEventLoopGroup workerGroup = new NioEventLoopGroup(workerCount, new ExecutorThreadFactory("flink-" + tag + "-server-netty-worker"));

		bootstrap = new ServerBootstrap();
		bootstrap
			.group(bossGroup, workerGroup)
			.channel(NioServerSocketChannel.class);

		final ThreadFactory threadFactory = new ThreadFactoryBuilder()
			.setDaemon(true)
			.setNameFormat("Flink test" + " EventLoop Thread %d")
			.build();

		final NettyBufferPool bufferPool = new NettyBufferPool(1);

		bootstrap
			.option(ChannelOption.ALLOCATOR, bufferPool)
			.childOption(ChannelOption.ALLOCATOR, bufferPool);


		bootstrap.childHandler(new ChannelInitializer<SocketChannel>() {
			@Override
			public void initChannel(SocketChannel ch) {
				if (channelPipelineConsumer != null) {
					channelPipelineConsumer.accept(ch.pipeline());
				}
			}
		});

		final int defaultHighWaterMark = 64 * 1024; // from DefaultChannelConfig (not exposed)
		//noinspection ConstantConditions
		// (ignore warning here to make this flexible in case the configuration values change)
		if (LOW_WATER_MARK > defaultHighWaterMark) {
			bootstrap.childOption(ChannelOption.WRITE_BUFFER_HIGH_WATER_MARK, HIGH_WATER_MARK);
			bootstrap.childOption(ChannelOption.WRITE_BUFFER_LOW_WATER_MARK, LOW_WATER_MARK);
		} else { // including (newHighWaterMark < defaultLowWaterMark)
			bootstrap.childOption(ChannelOption.WRITE_BUFFER_LOW_WATER_MARK, LOW_WATER_MARK);
			bootstrap.childOption(ChannelOption.WRITE_BUFFER_HIGH_WATER_MARK, HIGH_WATER_MARK);
		}

		Iterator<Integer> portsIterator;
		try {
			portsIterator = NetUtils.getPortRangeFromString(portRange);
		} catch (IllegalConfigurationException e) {
			throw e;
		} catch (Exception e) {
			throw new IllegalArgumentException("Invalid port range definition: " + portRange);
		}

		int chosenPort = 0;
		while (portsIterator.hasNext()) {
			try {
				chosenPort = portsIterator.next();
				final ChannelFuture channel;
				if (address == null) {
					channel = bootstrap.bind(chosenPort);
				} else {
					channel = bootstrap.bind(address, chosenPort);
				}
				serverChannel = channel.syncUninterruptibly().channel();
				break;
			} catch (final Exception e) {
				// continue if the exception is due to the port being in use, fail early otherwise
				if (!(e instanceof org.jboss.netty.channel.ChannelException)) {
					throw e;
				}
			}
		}
		if (serverChannel == null) {
			throw new BindException("Could not start socket endpoint on any port in port range " + portRange);
		}

		LOG.info("++++++ Binding socket endpoint to {}:{}.", address, chosenPort);

		final InetSocketAddress bindAddress = (InetSocketAddress) serverChannel.localAddress();
		final String advertisedAddress;
		if (bindAddress.getAddress().isAnyLocalAddress()) {
			advertisedAddress = this.address;
		} else {
			advertisedAddress = bindAddress.getAddress().getHostAddress();
		}
		address = advertisedAddress;
		port = bindAddress.getPort();

		LOG.info("++++++ Socket endpoint listening at {}:{}", address, port);
	}

	@Override
	public CompletableFuture<Void> closeAsync() {
		final CompletableFuture<Void> channelTerminationFuture = new CompletableFuture<>();
		CompletableFuture<?> groupFuture = new CompletableFuture<>();
		CompletableFuture<?> childGroupFuture = new CompletableFuture<>();
		final Time gracePeriod = Time.seconds(10L);

		if (bootstrap != null) {
			final ServerBootstrapConfig config = bootstrap.config();
			final EventLoopGroup group = config.group();
			if (group != null) {
				group.shutdownGracefully(0L, gracePeriod.toMilliseconds(), TimeUnit.MILLISECONDS)
					.addListener(finished -> {
						if (finished.isSuccess()) {
							groupFuture.complete(null);
						} else {
							groupFuture.completeExceptionally(finished.cause());
						}
					});
			} else {
				groupFuture.complete(null);
			}

			final EventLoopGroup childGroup = config.childGroup();
			if (childGroup != null) {
				childGroup.shutdownGracefully(0L, gracePeriod.toMilliseconds(), TimeUnit.MILLISECONDS)
					.addListener(finished -> {
						if (finished.isSuccess()) {
							childGroupFuture.complete(null);
						} else {
							childGroupFuture.completeExceptionally(finished.cause());
						}
					});
			} else {
				childGroupFuture.complete(null);
			}

			bootstrap = null;
		} else {
			// complete the group futures since there is nothing to stop
			groupFuture.complete(null);
			childGroupFuture.complete(null);
		}

		CompletableFuture<Void> combinedFuture = FutureUtils.completeAll(Arrays.asList(groupFuture, childGroupFuture));

		combinedFuture.whenComplete(
			(Void ignored, Throwable throwable) -> {
				if (throwable != null) {
					channelTerminationFuture.completeExceptionally(throwable);
				} else {
					channelTerminationFuture.complete(null);
				}
			});

		return channelTerminationFuture;
	}
}
