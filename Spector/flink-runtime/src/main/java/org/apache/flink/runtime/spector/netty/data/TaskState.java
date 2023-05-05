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

public class TaskState implements NettyMessage, Serializable {
	private ExecutionAttemptID executionAttemptID;
	private JobVertexID jobvertexId;
	private JobManagerTaskRestore taskRestore;

	private KeyGroupRange keyGroupRange;

	private int idInModel;

	private Time timeout;

	public TaskState() {}

	public TaskState(
		ExecutionAttemptID executionAttemptID,
		JobVertexID jobvertexId,
		JobManagerTaskRestore taskRestore,
		@Nullable KeyGroupRange keyGroupRange,
		@Nullable int idInModel,
		Time timeout) {
		this.executionAttemptID = executionAttemptID;
		this.jobvertexId = jobvertexId;
		this.taskRestore = taskRestore;
		this.keyGroupRange = keyGroupRange;
		this.idInModel = idInModel;
		this.timeout = timeout;
	}

	public ExecutionAttemptID getExecutionAttemptID() {
		return executionAttemptID;
	}

	public JobVertexID getJobvertexId() {
		return jobvertexId;
	}

	public JobManagerTaskRestore getTaskRestore() {
		return taskRestore;
	}

	public KeyGroupRange getKeyGroupRange() {
		return keyGroupRange;
	}

	public int getIdInModel() {
		return idInModel;
	}

	public Time getTimeout() {
		return timeout;
	}

    public void write(DataOutputView out) throws Exception {
		out.writeLong(executionAttemptID.getLowerPart());
		out.writeLong(executionAttemptID.getUpperPart());
		out.writeLong(jobvertexId.getLowerPart());
		out.writeLong(jobvertexId.getUpperPart());
		taskRestore.write(out);
		out.writeInt(idInModel);
		if (keyGroupRange != null) {
			out.writeBoolean(true);
			keyGroupRange.write(out);
		} else {
			out.writeBoolean(false);
		}
    }

	public void read(DataInputView in) throws Exception {
		executionAttemptID = new ExecutionAttemptID(in.readLong(), in.readLong());
		jobvertexId = new JobVertexID(in.readLong(), in.readLong());
		taskRestore = new JobManagerTaskRestore();
		taskRestore.read(in);
		idInModel = in.readInt();
		if (in.readBoolean()) {
			keyGroupRange = new KeyGroupRange();
			keyGroupRange.read(in);
		}
	}
}
