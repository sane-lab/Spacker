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

import org.apache.flink.runtime.io.network.netty.NettyBufferPool;
import org.apache.flink.runtime.spector.netty.socket.NettySocketClient;
import org.apache.flink.runtime.spector.netty.socket.NettySocketServer;
import org.apache.flink.shaded.netty4.io.netty.channel.ChannelHandlerContext;
import org.apache.flink.shaded.netty4.io.netty.channel.ChannelInboundHandlerAdapter;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.LengthFieldPrepender;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.bytes.ByteArrayDecoder;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.bytes.ByteArrayEncoder;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.serialization.ClassResolvers;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.serialization.ObjectDecoder;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.serialization.ObjectEncoder;


import java.util.Arrays;
import java.util.Random;
import java.util.concurrent.CompletableFuture;

/**
 * Test case for netty socket server.
 */
public class ExploreNettySocketServerTest {

	private final NettyBufferPool bufferPool;

	ExploreNettySocketServerTest() {
		this.bufferPool = new NettyBufferPool(1);
	}

	public void run() throws Exception {
		int stringLength = 1024 * 1024 * 1024;

//		String message = StringUtils.repeat("*", stringLength);
		byte[] message = new byte[stringLength];
		Random rd = new Random();
		rd.nextBytes(message);
		CompletableFuture<byte[]> receiveFuture = new CompletableFuture<>();

		try (NettySocketServer nettySocketServer = new NettySocketServer(
			"test",
			"localhost",
			"0",
			channelPipeline -> channelPipeline.addLast(
				new ObjectEncoder(),
//				new ObjectDecoder(Integer.MAX_VALUE, ClassResolvers.cacheDisabled(null)),
				new LengthFieldBasedFrameDecoder(Integer.MAX_VALUE, 0, 4, 0, 4),
			 new ByteArrayDecoder(),
				new ChannelInboundHandlerAdapter() {
					@Override
					public void channelRead(ChannelHandlerContext ctx, Object msg) {
						receiveFuture.complete((byte[]) msg);
					}
				}
			), 0)) {
			nettySocketServer.start();

			try (NettySocketClient nettySocketClient = new NettySocketClient(
				nettySocketServer.getAddress(),
				nettySocketServer.getPort(),
				10000,
				0,
				0,
				channelPipeline ->
					channelPipeline.addLast(
						new LengthFieldPrepender(4),
						new ByteArrayEncoder(),
						new ObjectDecoder(Integer.MAX_VALUE, ClassResolvers.cacheDisabled(null)))
			)) {
				nettySocketClient.start();

				long start = System.currentTimeMillis();

					nettySocketClient.getChannel().writeAndFlush(message);

				nettySocketClient.getChannel().writeAndFlush("-1");

				System.out.println(Arrays.equals(message, receiveFuture.get()));

				System.out.println(System.currentTimeMillis() - start);
			}
		}
	}

	public static void main(String[] args) throws Exception {
		ExploreNettySocketServerTest exploreNettySocketServerTest = new ExploreNettySocketServerTest();
		exploreNettySocketServerTest.run();
	}
}
