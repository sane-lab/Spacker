package org.apache.flink.runtime.spector.netty.data;

import org.apache.flink.api.common.time.Time;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.runtime.checkpoint.JobManagerTaskRestore;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.state.KeyGroupRange;

import javax.annotation.Nullable;
import java.io.Serializable;

public class TaskRPC implements NettyMessage, Serializable {
	private ExecutionAttemptID executionAttemptID;
	private JobVertexID jobvertexId;

	private String requestId;

	private Time timeout;

	public TaskRPC() {}

	public TaskRPC(
		ExecutionAttemptID executionAttemptID,
		JobVertexID jobvertexId,
		String requestId,
		Time timeout) {
		this.executionAttemptID = executionAttemptID;
		this.jobvertexId = jobvertexId;
		this.requestId = requestId;
		this.timeout = timeout;
	}

	public ExecutionAttemptID getExecutionAttemptID() {
		return executionAttemptID;
	}

	public JobVertexID getJobvertexId() {
		return jobvertexId;
	}

	public String getRequestId() {
		return requestId;
	}

	public Time getTimeout() {
		return timeout;
	}

    public void write(DataOutputView out) throws Exception {
		out.writeLong(executionAttemptID.getLowerPart());
		out.writeLong(executionAttemptID.getUpperPart());
		out.writeLong(jobvertexId.getLowerPart());
		out.writeLong(jobvertexId.getUpperPart());
		out.writeUTF(requestId);
    }

	public void read(DataInputView in) throws Exception {
		executionAttemptID = new ExecutionAttemptID(in.readLong(), in.readLong());
		jobvertexId = new JobVertexID(in.readLong(), in.readLong());
		requestId = in.readUTF();
	}
}
