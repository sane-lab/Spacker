package org.apache.flink.runtime.spector.netty.codec;

import org.apache.flink.core.memory.DataInputViewStreamWrapper;
import org.apache.flink.runtime.spector.netty.data.TaskDeployment;
import org.apache.flink.shaded.netty4.io.netty.buffer.ByteBuf;
import org.apache.flink.shaded.netty4.io.netty.channel.ChannelHandlerContext;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.ByteToMessageDecoder;

import java.io.ByteArrayInputStream;
import java.util.List;

public class TaskDeploymentDecoder extends ByteToMessageDecoder {
	private static final int BODY_LENGTH = 4;

	@Override
	protected void decode(
		ChannelHandlerContext channelHandlerContext,
		ByteBuf byteBuf,
		List<Object> list) throws Exception {
		if (byteBuf.readableBytes() >= BODY_LENGTH) {
			byteBuf.markReaderIndex();
			int dataSize = byteBuf.readInt();
			if (dataSize < 0 || byteBuf.readableBytes() < 0) {
				return;
			}
			if (byteBuf.readableBytes() < dataSize) {
				byteBuf.resetReaderIndex();
				return;
			}
			byte[] data = new byte[dataSize];
			byteBuf.readBytes(data);
			TaskDeployment td = new TaskDeployment();
			td.read(new DataInputViewStreamWrapper(new ByteArrayInputStream(data)));
			list.add(td);
		}
	}
}
