package org.apache.flink.runtime.spector.controller;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobVertexID;

import java.util.List;

public interface OperatorController {

	void start();

	void stopGracefully();

	//Method used to inform Controller
	void onMigrationExecutorsStopped();
	void onMigrationCompleted();

	void initMetrics(JobGraph jobGraph, JobVertexID vertexID, Configuration jobConfiguration, int parallelism);

	void onForceRetrieveMetrics();

	StateMigrationPlanner getStateMigrationPlanner();
}
