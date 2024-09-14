package org.apache.flink.runtime.spector.migration;

import org.apache.flink.runtime.executiongraph.ExecutionGraph;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.spector.migration.JobExecutionPlan;

public interface JobReconfigActor {

	JobGraph getJobGraph();

	ExecutionGraph getExecutionGraph();

	void setInitialJobExecutionPlan(JobVertexID vertexID, JobExecutionPlan JobExecutionPlan);

	boolean checkReplicationProgress();

	void updateBackupKeyGroups(int filter);

	void repartition(JobVertexID vertexID, JobExecutionPlan jobExecutionPlan) throws InterruptedException;

//	void scaleOut(JobVertexID vertexID, int newParallelism, JobExecutionPlan jobExecutionPlan);

//	void scaleIn(JobVertexID vertexID, int newParallelism, JobExecutionPlan jobExecutionPlan);

	enum ActionType {
		REPARTITION,
		SCALE_OUT,
		SCALE_IN
	}
}
