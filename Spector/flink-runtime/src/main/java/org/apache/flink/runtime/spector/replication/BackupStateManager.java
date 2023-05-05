package org.apache.flink.runtime.spector.replication;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.core.fs.FSDataInputStream;
import org.apache.flink.core.memory.DataInputViewStreamWrapper;
import org.apache.flink.runtime.checkpoint.JobManagerTaskRestore;
import org.apache.flink.runtime.checkpoint.OperatorSubtaskState;
import org.apache.flink.runtime.checkpoint.StateObjectCollection;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.state.*;
import org.apache.flink.runtime.state.memory.ByteStreamStateHandle;
import org.apache.flink.runtime.taskmanager.Task;
import org.apache.flink.util.Preconditions;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;

/**
 * Instantiated on each task manager, each Job need to instantiate one TaskStateManager on each TaskManager
 * TODO: maybe we do not need this class, we assign a task to be a backup task, other task can access the task to get the backup state.
 */
public class BackupStateManager {
	public Map<JobVertexID, TaskStateManager> replicas;

	public BackupStateManager() {
		replicas = new ConcurrentHashMap<>();
	}

	/**
	 * The API to retrieve task restore from the replica tasks.
	 * @param jobvertexId
	 * @return
	 */
	public JobManagerTaskRestore getTaskRestoreFromReplica(JobVertexID jobvertexId) {
		TaskStateManager taskStateManager = this.replicas.get(jobvertexId);
		// directly assign backup taskrestore for the targeting task, and it will be updated in each operator during
		// state initialization.
		Preconditions.checkNotNull(taskStateManager);
		return ((TaskStateManagerImpl) taskStateManager).getTaskRestore();
	}

	public void put(JobVertexID jobvertexId, TaskStateManager taskStateManager) {
		this.replicas.put(jobvertexId, taskStateManager);
	}

	/**
	 * Substitute keyed state that is shown up in
	 */
	public void mergeState(JobVertexID jobvertexId, JobManagerTaskRestore newStateRestore) {
		TaskStateManager taskStateManager = this.replicas.get(jobvertexId);
		JobManagerTaskRestore oldTaskRestore = ((TaskStateManagerImpl) taskStateManager).getTaskRestore();

		newStateRestore.merge(oldTaskRestore);

		taskStateManager.setTaskRestore(newStateRestore);
	}

}
