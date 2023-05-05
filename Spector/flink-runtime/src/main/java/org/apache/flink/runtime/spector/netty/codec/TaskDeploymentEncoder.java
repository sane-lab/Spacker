package org.apache.flink.runtime.spector.netty.codec;

import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.core.memory.DataOutputViewStreamWrapper;
import org.apache.flink.runtime.spector.netty.data.TaskDeployment;
import org.apache.flink.shaded.netty4.io.netty.buffer.ByteBuf;
import org.apache.flink.shaded.netty4.io.netty.channel.ChannelHandlerContext;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.MessageToByteEncoder;

import java.io.ByteArrayOutputStream;
import java.io.Serializable;

public class TaskDeploymentEncoder extends MessageToByteEncoder<Serializable> {
	@Override
	protected void encode(
		ChannelHandlerContext channelHandlerContext,
		Serializable serializable,
		ByteBuf byteBuf) throws Exception {
		if (serializable instanceof TaskDeployment) {
			TaskDeployment td = (TaskDeployment) serializable;
			ByteArrayOutputStream baos = new ByteArrayOutputStream();
			DataOutputView dataOutputView = new DataOutputViewStreamWrapper(baos);
			td.write(dataOutputView);
			baos.flush();

			byte[] data = baos.toByteArray();
			int dataSize = data.length;
			byteBuf.writeInt(dataSize);
			byteBuf.writeBytes(data);
			baos.close();
		}
	}
}
