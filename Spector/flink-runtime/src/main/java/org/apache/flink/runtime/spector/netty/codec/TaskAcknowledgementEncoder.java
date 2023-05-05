package org.apache.flink.runtime.spector.netty.codec;

import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.core.memory.DataOutputViewStreamWrapper;
import org.apache.flink.runtime.spector.netty.TaskExecutorNettyClient;
import org.apache.flink.runtime.spector.netty.data.TaskAcknowledgement;
import org.apache.flink.shaded.netty4.io.netty.buffer.ByteBuf;
import org.apache.flink.shaded.netty4.io.netty.channel.ChannelHandlerContext;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.MessageToByteEncoder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.Serializable;

public class TaskAcknowledgementEncoder extends MessageToByteEncoder<Serializable> {
	private static final Logger LOG = LoggerFactory.getLogger(TaskAcknowledgementEncoder.class);

	@Override
	protected void encode(
		ChannelHandlerContext channelHandlerContext,
		Serializable serializable,
		ByteBuf byteBuf) throws Exception {
		if (serializable instanceof TaskAcknowledgement) {
			LOG.info("++++++ Starting Encoding ACK");
			TaskAcknowledgement ta = (TaskAcknowledgement) serializable;
			ByteArrayOutputStream baos = new ByteArrayOutputStream();
			DataOutputView dataOutputView = new DataOutputViewStreamWrapper(baos);
			ta.write(dataOutputView);
			baos.flush();

			byte[] data = baos.toByteArray();
			int dataSize = data.length;
			byteBuf.writeInt(dataSize);
			byteBuf.writeBytes(data);
			baos.close();
			LOG.info("++++++ Complete Encoding ACK");
		}
	}
}
