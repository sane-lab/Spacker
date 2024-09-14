package org.apache.flink.runtime.spector.netty.codec;

import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.core.memory.DataOutputViewStreamWrapper;
import org.apache.flink.runtime.spector.netty.data.TaskState;
import org.apache.flink.shaded.netty4.io.netty.buffer.ByteBuf;
import org.apache.flink.shaded.netty4.io.netty.channel.ChannelHandlerContext;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.MessageToByteEncoder;

import java.io.ByteArrayOutputStream;
import java.io.Serializable;

public class TaskBackupStateEncoder extends MessageToByteEncoder<Serializable> {
	@Override
	protected void encode(
		ChannelHandlerContext channelHandlerContext,
		Serializable serializable,
		ByteBuf byteBuf) throws Exception {
		if (serializable instanceof TaskState) {
			TaskState tbs = (TaskState) serializable;
			ByteArrayOutputStream baos = new ByteArrayOutputStream();
			DataOutputView dataOutputView = new DataOutputViewStreamWrapper(baos);
			tbs.write(dataOutputView);
			baos.flush();

			byte[] data = baos.toByteArray();
			int dataSize = data.length;
			byteBuf.writeInt(dataSize);
			byteBuf.writeBytes(data);
			baos.close();
		}
	}
}
