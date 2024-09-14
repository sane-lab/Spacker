package org.apache.flink.runtime.spector.replication;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.checkpoint.JobManagerTaskRestore;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.state.*;
import org.apache.flink.util.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Instantiated on each task manager, each Job need to instantiate one TaskStateManager on each TaskManager
 * TODO: maybe we do not need this class, we assign a task to be a backup task, other task can access the task to get the backup state.
 */
public class ReplicaStateManager {
	protected final Logger log = LoggerFactory.getLogger(getClass());

	private final Map<JobVertexID, TaskStateManager> replicas;

	public ReplicaStateManager() {
		replicas = new ConcurrentHashMap<>();
	}

	/**
	 * The API to retrieve task restore from the replica tasks.
	 *
	 * @param jobvertexId
	 * @param dstAffectedKeyGroups
	 * @param keyGroupRange
	 * @return
	 */
	public JobManagerTaskRestore getTaskRestoreFromReplica(JobVertexID jobvertexId, Collection<Integer> migratingInKeygroups, KeyGroupRange keyGroupRange) {
		TaskStateManager taskStateManager = replicas.get(jobvertexId);
		// directly assign backup taskrestore for the targeting task, and it will be updated in each operator during
		// state initialization.
		Preconditions.checkNotNull(taskStateManager);

		return ((TaskStateManagerImpl) taskStateManager).getTaskRestoreFromStateHandle(jobvertexId, migratingInKeygroups, keyGroupRange);
	}

	public void put(JobVertexID jobvertexId, TaskStateManager taskStateManager) {
		replicas.put(jobvertexId, taskStateManager);
	}

	/**
	 * Substitute keyed state that is shown up in
	 */
	public void mergeState(JobVertexID jobvertexId, JobManagerTaskRestore newStateRestore) {
		TaskStateManager taskStateManager = replicas.get(jobvertexId);
		JobManagerTaskRestore oldTaskRestore = ((TaskStateManagerImpl) taskStateManager).getTaskRestore();

		newStateRestore.merge(oldTaskRestore);

		taskStateManager.setTaskRestore(newStateRestore);
	}

	/**
	 * Substitute keyed state that is shown up in
	 */
	public void mergeState(JobVertexID jobvertexId, Map<Integer, Tuple2<Long, StreamStateHandle>> hashedKeyGroupToHandle) {
		TaskStateManager taskStateManager = replicas.get(jobvertexId);
		Map<Integer, Tuple2<Long, StreamStateHandle>> localReplicatedStateHandle = ((TaskStateManagerImpl) taskStateManager).getHashedKeyGroupToHandles();
		Preconditions.checkNotNull(localReplicatedStateHandle);
		localReplicatedStateHandle.putAll(hashedKeyGroupToHandle);
//		long stateSize = localReplicatedStateHandle.values().stream().mapToLong(stateHandle -> stateHandle.f1.getStateSize()).sum();
//        log.debug("++++--- Current total merged state size: " + stateSize);
	}

	public Map<JobVertexID, TaskStateManager> getReplicas() {
		return replicas;
	}
}
