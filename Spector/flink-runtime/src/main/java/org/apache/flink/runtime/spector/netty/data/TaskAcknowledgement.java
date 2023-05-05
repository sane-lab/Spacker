package org.apache.flink.runtime.spector.netty.data;

import org.apache.flink.api.common.JobID;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.runtime.checkpoint.CheckpointMetrics;
import org.apache.flink.runtime.checkpoint.TaskStateSnapshot;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;

import java.io.Serializable;

public class TaskAcknowledgement implements Serializable, NettyMessage {
	private JobID jobID;
	private ExecutionAttemptID executionAttemptID;
	private long checkpointId;
	private CheckpointMetrics checkpointMetrics;
	private TaskStateSnapshot subtaskState;

	public TaskAcknowledgement() {

	}

	public TaskAcknowledgement(
		JobID jobID,
		ExecutionAttemptID executionAttemptID,
		long checkpointId,
		CheckpointMetrics checkpointMetrics,
		TaskStateSnapshot subtaskState) {
		this.jobID = jobID;
		this.executionAttemptID = executionAttemptID;
		this.checkpointId = checkpointId;
		this.checkpointMetrics = checkpointMetrics;
		this.subtaskState = subtaskState;
	}

	public JobID getJobID() {
		return jobID;
	}

	public ExecutionAttemptID getExecutionAttemptID() {
		return executionAttemptID;
	}

	public long getCheckpointId() {
		return checkpointId;
	}

	public CheckpointMetrics getCheckpointMetrics() {
		return checkpointMetrics;
	}

	public TaskStateSnapshot getSubtaskState() {
		return subtaskState;
	}

	@Override
	public void write(DataOutputView out) throws Exception {
		out.writeLong(jobID.getLowerPart());
		out.writeLong(jobID.getUpperPart());
		out.writeLong(executionAttemptID.getLowerPart());
		out.writeLong(executionAttemptID.getUpperPart());
		out.writeLong(checkpointId);
		checkpointMetrics.write(out);
		if (subtaskState != null) {
			out.writeBoolean(true);
			subtaskState.write(out);
		} else {
			out.writeBoolean(false);
		}
	}

	@Override
	public void read(DataInputView in) throws Exception {
		jobID = new JobID(in.readLong(), in.readLong());
		executionAttemptID = new ExecutionAttemptID(in.readLong(), in.readLong());
		checkpointId = in.readLong();
		checkpointMetrics = new CheckpointMetrics();
		checkpointMetrics.read(in);
		if (in.readBoolean()) {
			subtaskState = new TaskStateSnapshot();
			subtaskState.read(in);
		} else {
			subtaskState = null;
		}
	}
}
