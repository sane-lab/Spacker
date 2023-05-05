package org.apache.flink.runtime.spector.netty.codec;

import org.apache.flink.core.memory.DataInputViewStreamWrapper;
import org.apache.flink.runtime.spector.netty.data.TaskAcknowledgement;
import org.apache.flink.runtime.spector.netty.data.TaskDeployment;
import org.apache.flink.shaded.netty4.io.netty.buffer.ByteBuf;
import org.apache.flink.shaded.netty4.io.netty.channel.ChannelHandlerContext;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.ByteToMessageDecoder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.util.List;

public class TaskAcknowledgementDecoder extends ByteToMessageDecoder {
	private static final Logger LOG = LoggerFactory.getLogger(TaskAcknowledgementDecoder.class);

	private static final int BODY_LENGTH = 4;

	@Override
	protected void decode(
		ChannelHandlerContext channelHandlerContext,
		ByteBuf byteBuf,
		List<Object> list) throws Exception {
		if (byteBuf.readableBytes() >= BODY_LENGTH) {
			LOG.info("++++++ Starting Decoding ACK");
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
			TaskAcknowledgement ta = new TaskAcknowledgement();
			ta.read(new DataInputViewStreamWrapper(new ByteArrayInputStream(data)));
			list.add(ta);
			LOG.info("++++++ Complete Decoding ACK");
		}
	}
}
