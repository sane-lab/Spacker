package org.apache.flink.runtime.spector.netty.utils;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.core.memory.DataOutputViewStreamWrapper;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.messages.Acknowledge;
import org.apache.flink.runtime.spector.netty.data.*;
import org.apache.flink.shaded.netty4.io.netty.buffer.ByteBuf;
import org.apache.flink.shaded.netty4.io.netty.buffer.Unpooled;
import org.apache.flink.shaded.netty4.io.netty.channel.Channel;
import org.apache.flink.shaded.netty4.io.netty.channel.ChannelFutureListener;
import org.apache.flink.shaded.netty4.io.netty.channel.ChannelHandlerContext;
import org.apache.flink.util.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.function.BiConsumer;

import static org.apache.flink.runtime.spector.netty.utils.NettySocketUtils.NettyMessageType.*;

public class NettySocketUtils {
	private static final Logger LOG = LoggerFactory.getLogger(NettySocketUtils.class);

	public enum NettyMessageType {
		TASK_STATE_RESTORE,
		TASK_ACKNOWLEDGEMENT,
		TASK_DEPLOYMENT,
		TASK_RPC
	}

	public static byte[] getBytes(NettyMessage nettyMessage) throws Exception {
		ByteArrayOutputStream baos = new ByteArrayOutputStream();
		DataOutputView dataOutputView = new DataOutputViewStreamWrapper(baos);
		nettyMessage.write(dataOutputView);
		baos.flush();

		return baos.toByteArray();
	}

	public static void chunkedWriteAndFlush(
		CompletableFuture<Acknowledge> submitFuture, Channel channel, NettyMessage nettyMessage, ExecutionAttemptID executionAttemptID) throws Exception {
		byte[] data = getBytes(nettyMessage);
		LOG.debug("++++++ channel: " + channel.id() + " Sending message: " + data.length);
		byte[] chunk;
		int dataSize = data.length;
		int chunkSize = 32 * 1024;
		int numOfChunk = (int) Math.ceil((double) dataSize / chunkSize);

		byte[] id = executionAttemptID.getBytes();

		NettyMessageType eventType;
		if (nettyMessage instanceof TaskState) {
			eventType = TASK_STATE_RESTORE;
		} else if (nettyMessage instanceof TaskDeployment) {
			eventType = TASK_DEPLOYMENT;
		} else if (nettyMessage instanceof TaskAcknowledgement) {
			eventType = TASK_ACKNOWLEDGEMENT;
		} else if (nettyMessage instanceof TaskRPC) {
			eventType = TASK_RPC;
		}else {
			throw new UnsupportedOperationException();
		}

		// fire the tranmission start
		channel.writeAndFlush(executionAttemptID + ":" + eventType + "-" + dataSize);

		byte[] newChunk;

		// data transmission
		for (int i = 0; i < numOfChunk; i++) {
			chunk = Arrays.copyOfRange(
				data,
				i * chunkSize,
				Math.min(i * chunkSize + chunkSize, dataSize));
			newChunk = new byte[chunk.length + id.length];
			System.arraycopy(id, 0, newChunk, 0, id.length);
			System.arraycopy(chunk, 0, newChunk, id.length, chunk.length);
			channel.writeAndFlush(newChunk);
		}

		// fire an identifier as end of file
		channel.writeAndFlush(executionAttemptID.toHexString() + ":-1");
//			.addListener((ChannelFutureListener) channelFuture -> {
//			if (channelFuture.isSuccess()) {
//				submitFuture.complete(Acknowledge.get());
//			} else {
//				submitFuture.completeExceptionally(channelFuture.cause());
//			}
//		});

		submitFuture.complete(Acknowledge.get());
	}

	public static void chunkedChannelRead(
		Object msg,
		BiConsumer<byte[], String> consumer,
		ChannelHandlerContext ctx,
		Map<String, byte[]> recv,
		Map<String, Tuple2<String, Integer>> metadata) {
		if (msg instanceof String) {
			String[] msgArray = ((String) msg).split(":");
			String executionAttemptId = msgArray[0];
			if (msgArray[1].equals("-1")) {
				byte[] bytes = recv.remove(executionAttemptId);
				// release resource.
				Tuple2<String, Integer> curMetadata = metadata.remove(executionAttemptId);
				consumer.accept(bytes, curMetadata.f0);

				LOG.debug("++++++ complete new object: " + bytes.length);
			} else {
				String[] metadataArray = msgArray[1].split("-");
				int length = Integer.parseInt(metadataArray[1]);
				recv.computeIfAbsent(executionAttemptId, t -> new byte[length]);
 				metadata.putIfAbsent(executionAttemptId, Tuple2.of(metadataArray[0], 0));
				LOG.debug("++++++ receive new object: " + length + " " + Thread.currentThread().getId());
			}
		} else {
			ByteBuf byteBuf = Unpooled.copiedBuffer((byte[]) msg, 0, 16);
			ExecutionAttemptID executionAttemptID = ExecutionAttemptID.fromByteBuf(byteBuf);
			byte[] chunk = Arrays.copyOfRange((byte[]) msg, 16, (((byte[]) msg).length));
			byte[] curRecv = recv.get(executionAttemptID.toHexString());
			int curPos = metadata.get(executionAttemptID.toHexString()).f1;
			Preconditions.checkState(chunk.length + curPos <= curRecv.length,
				"++++++ channel: " + ctx.channel().id() + " received message length exceeded: " + curRecv.length + " : " + chunk.length + curPos);
			System.arraycopy(chunk, 0, curRecv, curPos, chunk.length);
			curPos += chunk.length;
//			metadata.put(executionAttemptID.toHexString(), curPos);
			metadata.get(executionAttemptID.toHexString()).f1 = curPos;
		}
	}
}
