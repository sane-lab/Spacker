package org.apache.flink.runtime.spector.replication;

import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.taskexecutor.TaskExecutorGateway;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Each operator maintains a set of replicas, after operator instances/tasks snapshoted state,
 * they leverage this registry to find remote replicas and send replicated state directly to remote.
 */
public class RemoteReplicaRegistry {
	private final Map<JobVertexID, List<TaskExecutorGateway>> remoteReplicaGateways;

	public RemoteReplicaRegistry() {
		remoteReplicaGateways = new ConcurrentHashMap<>();
	}

	public void put(JobVertexID jobvertexId, List<TaskExecutorGateway> standbyTaskGateways) {
		getRemoteReplicaGateways().put(jobvertexId, standbyTaskGateways);
	}

	public Map<JobVertexID, List<TaskExecutorGateway>> getRemoteReplicaGateways() {
		return remoteReplicaGateways;
	}
}
