package org.apache.flink.runtime.spector.netty.data;

import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;

public interface NettyMessage {
	void write(DataOutputView out) throws Exception;

	void read(DataInputView in) throws Exception;
}
