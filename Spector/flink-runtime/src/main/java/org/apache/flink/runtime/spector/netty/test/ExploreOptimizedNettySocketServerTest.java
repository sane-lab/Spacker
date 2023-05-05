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

package org.apache.flink.runtime.spector.netty.test;

import org.apache.flink.queryablestate.network.ChunkedByteBuf;
import org.apache.flink.runtime.spector.netty.socket.NettySocketClient;
import org.apache.flink.runtime.spector.netty.socket.NettySocketServer;
import org.apache.flink.shaded.netty4.io.netty.buffer.ByteBuf;
import org.apache.flink.shaded.netty4.io.netty.buffer.ByteBufAllocator;
import org.apache.flink.shaded.netty4.io.netty.buffer.CompositeByteBuf;
import org.apache.flink.shaded.netty4.io.netty.channel.ChannelHandlerContext;
import org.apache.flink.shaded.netty4.io.netty.channel.ChannelInboundHandlerAdapter;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import org.apache.flink.shaded.netty4.io.netty.handler.stream.ChunkedWriteHandler;
import org.apache.flink.shaded.netty4.io.netty.util.ReferenceCountUtil;

import java.util.Arrays;
import java.util.Random;
import java.util.concurrent.CompletableFuture;


/**
 * Test case for netty socket server.
 */
public class ExploreOptimizedNettySocketServerTest {

	/** AbstractServerBase config: low water mark. */
	private static final int LOW_WATER_MARK = 8 * 1024;

	/** AbstractServerBase config: high water mark. */
	private static final int HIGH_WATER_MARK = 32 * 1024;

	ExploreOptimizedNettySocketServerTest() {

	}

	public void run() throws Exception {
		int stringLength = 1024 * 1024;
		int chunkSize = 32 * 1024;

//		String message = StringUtils.repeat("*", stringLength);
		byte[] message = new byte[stringLength];
		Random rd = new Random();
		rd.nextBytes(message);
		byte[] chunk;
		CompletableFuture<byte[]> receiveFuture = new CompletableFuture<>();

		final CompositeByteBuf receiveBuf = ByteBufAllocator.DEFAULT.compositeDirectBuffer();


		try (NettySocketServer nettySocketServer = new NettySocketServer(
			"test",
			"localhost",
			"0",
			channelPipeline -> channelPipeline
				.addLast(new ChunkedWriteHandler())
				.addLast(new LengthFieldBasedFrameDecoder(Integer.MAX_VALUE, 0, 4, 0, 4))
				.addLast(
				new ChannelInboundHandlerAdapter() {
					@Override
					public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
						try {
							final ByteBuf buf = (ByteBuf) msg;
							receiveBuf.addComponent(buf);
							System.out.println(buf.readableBytes());
							byte[] content = new byte[buf.readableBytes()];
							buf.readBytes(content);
							receiveFuture.complete(content);
						} catch (Throwable t) {
							System.out.println("error");
							ctx.writeAndFlush("err");
						} finally {
							ReferenceCountUtil.release(msg);
						}
					}
				}
			), 0)) {
			nettySocketServer.start();

			try (NettySocketClient nettySocketClient = new NettySocketClient(
				nettySocketServer.getAddress(),
				nettySocketServer.getPort(),
				10000,
				LOW_WATER_MARK,
				HIGH_WATER_MARK,
				channelPipeline ->
					channelPipeline
						.addLast(new LengthFieldBasedFrameDecoder(Integer.MAX_VALUE, 0, 4, 0, 4))
						.addLast(new ChunkedWriteHandler())
//						.addLast(
//					new ObjectEncoder(),
//					new ObjectDecoder(Integer.MAX_VALUE, ClassResolvers.cacheDisabled(null)))
			)) {
				nettySocketClient.start();

				long start = System.currentTimeMillis();

				int numOfChunk = (int) Math.ceil((double) stringLength / chunkSize);

				int from = 0;
				int to = 0;
				for (int i = 0; i < numOfChunk; i++) {
					from = i * chunkSize;
					to = Math.min(i * chunkSize + chunkSize, stringLength);
					chunk = Arrays.copyOfRange(message, from, to);

					final ByteBuf byteBuf = nettySocketClient.getChannel().alloc().directBuffer(chunk.length + Integer.BYTES);

					byteBuf.writeInt(chunk.length);
					byteBuf.writeBytes(chunk);

					nettySocketClient.getChannel().writeAndFlush(new ChunkedByteBuf(byteBuf, LOW_WATER_MARK));
				}


				nettySocketClient.getChannel().writeAndFlush("-1");

				System.out.println(Arrays.equals(message, receiveFuture.get()));

				System.out.println(System.currentTimeMillis() - start);
			}
		}
	}

	public static void main(String[] args) throws Exception {
		ExploreOptimizedNettySocketServerTest nettySocketServerTest = new ExploreOptimizedNettySocketServerTest();
		nettySocketServerTest.run();
	}
}
