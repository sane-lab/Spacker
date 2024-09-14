/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.spector;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobVertexID;

import java.lang.reflect.Constructor;
import java.util.List;
import java.util.Map;

public abstract class JobGraphUpdater {

	protected final JobGraph jobGraph;
	protected final ClassLoader userCodeLoader;

	public JobGraphUpdater(JobGraph jobGraph, ClassLoader userCodeLoader) {
		this.jobGraph = jobGraph;
		this.userCodeLoader = userCodeLoader;
	}

	public abstract void rescale(JobVertexID id, int newParallelism, Map<Integer, List<Integer>> partitionAssignment, List<JobVertexID> involvedUpstream, List<JobVertexID> involvedDownstream);

	public abstract void repartition(JobVertexID id, Map<Integer, List<Integer>> partitionAssignment, List<JobVertexID> involvedUpstream, List<JobVertexID> involvedDownstream);

	public abstract String print(Configuration config);

	public static JobGraphUpdater instantiate(JobGraph jobGraph, ClassLoader userCodeLoader) {
		try {
			Class<? extends JobGraphUpdater> jobRescaleClass = Class
				.forName(jobGraph.getJobUpdaterClassName(), true, userCodeLoader).asSubclass(JobGraphUpdater.class);

			Constructor<? extends JobGraphUpdater> statelessCtor = jobRescaleClass.getConstructor(JobGraph.class, ClassLoader.class);

			return statelessCtor.newInstance(jobGraph, userCodeLoader);
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

//	public static JobGraphUpdater instantiate(ClassLoader userCodeLoader) throws Exception {
//		Class<? extends JobGraphUpdater> jobRescaleClass = Class
//			.forName("org.apache.flink.streaming.api.graph.StreamJobRescale", true, userCodeLoader).asSubclass(JobGraphUpdater.class);
//
//		Constructor<? extends JobGraphUpdater> statelessCtor = jobRescaleClass.getConstructor(JobGraph.class, ClassLoader.class);
//
//		return statelessCtor.newInstance(null, userCodeLoader);
//	}
}
