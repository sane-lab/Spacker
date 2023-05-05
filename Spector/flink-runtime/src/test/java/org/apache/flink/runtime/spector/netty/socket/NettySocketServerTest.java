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

package org.apache.flink.runtime.spector.netty.socket;

import org.apache.commons.lang.StringUtils;
import org.apache.flink.shaded.netty4.io.netty.channel.ChannelHandlerContext;
import org.apache.flink.shaded.netty4.io.netty.channel.ChannelInboundHandlerAdapter;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.serialization.ClassResolvers;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.serialization.ObjectDecoder;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.serialization.ObjectEncoder;

import org.junit.Test;

import java.util.concurrent.CompletableFuture;

import static org.junit.Assert.assertEquals;

/**
 * Test case for netty socket server.
 */
public class NettySocketServerTest {
	@Test
	public void testServerReceiveString() throws Exception {
		int stringLength = 1024 * 1024 * 1024;
		int chunkSize = 128 * 1024 * 1024;

		String message = StringUtils.repeat("*", stringLength);
		String chunk = StringUtils.repeat("*", chunkSize);
		final StringBuilder recv = new StringBuilder();
		CompletableFuture<String> receiveFuture = new CompletableFuture<>();

		try (NettySocketServer nettySocketServer = new NettySocketServer(
			"test",
			"localhost",
			"0",
			channelPipeline -> channelPipeline.addLast(
				new ObjectEncoder(),
				new ObjectDecoder(Integer.MAX_VALUE, ClassResolvers.cacheDisabled(null)),
				new ChannelInboundHandlerAdapter() {
					@Override
					public void channelRead(ChannelHandlerContext ctx, Object msg) {
						if (!msg.equals("-1")) {
							recv.append(msg);
						} else {
							receiveFuture.complete(recv.toString());
						}
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
					new ObjectEncoder(),
					new ObjectDecoder(Integer.MAX_VALUE, ClassResolvers.cacheDisabled(null)))
			)) {
				nettySocketClient.start();

				long start = System.currentTimeMillis();

				for (int i = 0; i < stringLength / chunkSize; i++) {
					nettySocketClient.getChannel().writeAndFlush(chunk);
				}

				nettySocketClient.getChannel().writeAndFlush("-1");

				assertEquals(message, receiveFuture.get());

				System.out.println(System.currentTimeMillis() - start);
			}
		}
	}
}
