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

package org.apache.flink.runtime.executiongraph;

import org.apache.flink.api.common.time.Time;
import org.apache.flink.runtime.akka.AkkaUtils;
import org.apache.flink.runtime.clusterframework.types.AllocationID;
import org.apache.flink.runtime.deployment.ResultPartitionDeploymentDescriptor;
import org.apache.flink.runtime.deployment.TaskDeploymentDescriptor;
import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.executiongraph.utils.SimpleAckingTaskManagerGateway;
import org.apache.flink.runtime.io.network.partition.ResultPartitionType;
import org.apache.flink.runtime.jobgraph.IntermediateDataSetID;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.jobgraph.ScheduleMode;
import org.apache.flink.runtime.jobmaster.LogicalSlot;
import org.apache.flink.runtime.jobmaster.SlotContext;
import org.apache.flink.runtime.jobmaster.TestingLogicalSlot;
import org.apache.flink.runtime.messages.Acknowledge;
import org.apache.flink.runtime.testutils.DirectScheduledExecutorService;
import org.apache.flink.util.TestLogger;

import org.junit.Test;

import java.util.Collection;
import java.util.HashSet;
import java.util.concurrent.CompletableFuture;

import static org.apache.flink.runtime.executiongraph.ExecutionGraphTestUtils.ERROR_MESSAGE;
import static org.apache.flink.runtime.executiongraph.ExecutionGraphTestUtils.getExecutionVertex;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class ExecutionVertexDeploymentTest extends TestLogger {

	@Test
	public void testDeployCall() {
		try {
			final JobVertexID jid = new JobVertexID();

			final ExecutionJobVertex ejv = getExecutionVertex(jid);

			final LogicalSlot slot = new TestingLogicalSlot();

			final ExecutionVertex vertex = new ExecutionVertex(ejv, 0, new IntermediateResult[0],
				AkkaUtils.getDefaultTimeout());

			assertEquals(ExecutionState.CREATED, vertex.getExecutionState());
			vertex.deployToSlot(slot);
			assertEquals(ExecutionState.DEPLOYING, vertex.getExecutionState());

			// no repeated scheduling
			try {
				vertex.deployToSlot(slot);
				fail("Scheduled from wrong state");
			} catch (IllegalStateException e) {
				// as expected
			}

			assertNull(vertex.getFailureCause());

			assertTrue(vertex.getStateTimestamp(ExecutionState.CREATED) > 0);
			assertTrue(vertex.getStateTimestamp(ExecutionState.DEPLOYING) > 0);
		} catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}

	@Test
	public void testDeployWithSynchronousAnswer() {
		try {
			final JobVertexID jid = new JobVertexID();

			final ExecutionJobVertex ejv = getExecutionVertex(jid, new DirectScheduledExecutorService());

			final LogicalSlot slot = new TestingLogicalSlot();

			final ExecutionVertex vertex = new ExecutionVertex(ejv, 0, new IntermediateResult[0],
				AkkaUtils.getDefaultTimeout());

			assertEquals(ExecutionState.CREATED, vertex.getExecutionState());

			vertex.deployToSlot(slot);

			assertEquals(ExecutionState.DEPLOYING, vertex.getExecutionState());

			// no repeated scheduling
			try {
				vertex.deployToSlot(slot);
				fail("Scheduled from wrong state");
			} catch (IllegalStateException e) {
				// as expected
			}

			assertNull(vertex.getFailureCause());

			assertTrue(vertex.getStateTimestamp(ExecutionState.CREATED) > 0);
			assertTrue(vertex.getStateTimestamp(ExecutionState.DEPLOYING) > 0);
			assertTrue(vertex.getStateTimestamp(ExecutionState.RUNNING) == 0);
		} catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}

	@Test
	public void testDeployWithAsynchronousAnswer() {
		try {
			final JobVertexID jid = new JobVertexID();
			final ExecutionJobVertex ejv = getExecutionVertex(jid);

			final ExecutionVertex vertex = new ExecutionVertex(ejv, 0, new IntermediateResult[0],
				AkkaUtils.getDefaultTimeout());

			final LogicalSlot slot = new TestingLogicalSlot();

			assertEquals(ExecutionState.CREATED, vertex.getExecutionState());

			vertex.deployToSlot(slot);

			// no repeated scheduling
			try {
				vertex.deployToSlot(slot);
				fail("Scheduled from wrong state");
			} catch (IllegalStateException e) {
				// as expected
			}

			assertEquals(ExecutionState.DEPLOYING, vertex.getExecutionState());

			// no repeated scheduling
			try {
				vertex.deployToSlot(slot);
				fail("Scheduled from wrong state");
			} catch (IllegalStateException e) {
				// as expected
			}

			assertTrue(vertex.getStateTimestamp(ExecutionState.CREATED) > 0);
			assertTrue(vertex.getStateTimestamp(ExecutionState.DEPLOYING) > 0);
			assertTrue(vertex.getStateTimestamp(ExecutionState.RUNNING) == 0);
		} catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}

	@Test
	public void testDeployFailedSynchronous() {
		try {
			final JobVertexID jid = new JobVertexID();
			final ExecutionJobVertex ejv = getExecutionVertex(jid, new DirectScheduledExecutorService());

			final ExecutionVertex vertex = new ExecutionVertex(ejv, 0, new IntermediateResult[0],
				AkkaUtils.getDefaultTimeout());

			final LogicalSlot slot = new TestingLogicalSlot(new SubmitFailingSimpleAckingTaskManagerGateway());

			assertEquals(ExecutionState.CREATED, vertex.getExecutionState());

			vertex.deployToSlot(slot);

			assertEquals(ExecutionState.FAILED, vertex.getExecutionState());
			assertNotNull(vertex.getFailureCause());
			assertTrue(vertex.getFailureCause().getMessage().contains(ERROR_MESSAGE));

			assertTrue(vertex.getStateTimestamp(ExecutionState.CREATED) > 0);
			assertTrue(vertex.getStateTimestamp(ExecutionState.DEPLOYING) > 0);
			assertTrue(vertex.getStateTimestamp(ExecutionState.FAILED) > 0);
		} catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}

	@Test
	public void testDeployFailedAsynchronously() {
		try {
			final JobVertexID jid = new JobVertexID();
			final ExecutionJobVertex ejv = getExecutionVertex(jid);
			final ExecutionVertex vertex = new ExecutionVertex(ejv, 0, new IntermediateResult[0],
				AkkaUtils.getDefaultTimeout());

			final LogicalSlot slot = new TestingLogicalSlot(new SubmitFailingSimpleAckingTaskManagerGateway());

			assertEquals(ExecutionState.CREATED, vertex.getExecutionState());

			vertex.deployToSlot(slot);

			// wait until the state transition must be done
			for (int i = 0; i < 100; i++) {
				if (vertex.getExecutionState() == ExecutionState.FAILED && vertex.getFailureCause() != null) {
					break;
				} else {
					Thread.sleep(10);
				}
			}

			assertEquals(ExecutionState.FAILED, vertex.getExecutionState());
			assertNotNull(vertex.getFailureCause());
			assertTrue(vertex.getFailureCause().getMessage().contains(ERROR_MESSAGE));

			assertTrue(vertex.getStateTimestamp(ExecutionState.CREATED) > 0);
			assertTrue(vertex.getStateTimestamp(ExecutionState.DEPLOYING) > 0);
			assertTrue(vertex.getStateTimestamp(ExecutionState.FAILED) > 0);
		} catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}

	@Test
	public void testFailExternallyDuringDeploy() {
		try {
			final JobVertexID jid = new JobVertexID();

			final ExecutionJobVertex ejv = getExecutionVertex(jid, new DirectScheduledExecutorService());

			final ExecutionVertex vertex = new ExecutionVertex(ejv, 0, new IntermediateResult[0],
				AkkaUtils.getDefaultTimeout());

			TestingLogicalSlot testingLogicalSlot = new TestingLogicalSlot(new SubmitBlockingSimpleAckingTaskManagerGateway());

			assertEquals(ExecutionState.CREATED, vertex.getExecutionState());
			vertex.deployToSlot(testingLogicalSlot);
			assertEquals(ExecutionState.DEPLOYING, vertex.getExecutionState());

			Exception testError = new Exception("test error");
			vertex.fail(testError);

			assertEquals(ExecutionState.FAILED, vertex.getExecutionState());
			assertEquals(testError, vertex.getFailureCause());

			assertTrue(vertex.getStateTimestamp(ExecutionState.CREATED) > 0);
			assertTrue(vertex.getStateTimestamp(ExecutionState.DEPLOYING) > 0);
			assertTrue(vertex.getStateTimestamp(ExecutionState.FAILED) > 0);
		} catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}

	private static class SubmitFailingSimpleAckingTaskManagerGateway extends SimpleAckingTaskManagerGateway {
		@Override
		public CompletableFuture<Acknowledge> submitTask(TaskDeploymentDescriptor tdd, Time timeout) {
			CompletableFuture<Acknowledge> future = new CompletableFuture<>();
			future.completeExceptionally(new Exception(ERROR_MESSAGE));
			return future;
		}
	}

	private static class SubmitBlockingSimpleAckingTaskManagerGateway extends SimpleAckingTaskManagerGateway {
		@Override
		public CompletableFuture<Acknowledge> submitTask(TaskDeploymentDescriptor tdd, Time timeout) {
			return new CompletableFuture<>();
		}
	}

	/**
	 * Tests that the lazy scheduling flag is correctly forwarded to the produced partition descriptors.
	 */
	@Test
	public void testTddProducedPartitionsLazyScheduling() throws Exception {
		ExecutionJobVertex jobVertex = getExecutionVertex(new JobVertexID(), new DirectScheduledExecutorService());

		IntermediateResult result =
				new IntermediateResult(new IntermediateDataSetID(), jobVertex, 1, ResultPartitionType.PIPELINED);

		ExecutionVertex vertex =
				new ExecutionVertex(jobVertex, 0, new IntermediateResult[]{result}, Time.minutes(1));

		ExecutionEdge mockEdge = createMockExecutionEdge(1);

		result.getPartitions()[0].addConsumerGroup();
		result.getPartitions()[0].addConsumer(mockEdge, 0);

		SlotContext slotContext = mock(SlotContext.class);
		when(slotContext.getAllocationId()).thenReturn(new AllocationID());

		LogicalSlot slot = mock(LogicalSlot.class);
		when(slot.getAllocationId()).thenReturn(new AllocationID());

		for (ScheduleMode mode : ScheduleMode.values()) {
			vertex.getExecutionGraph().setScheduleMode(mode);

			TaskDeploymentDescriptor tdd = vertex.createDeploymentDescriptor(new ExecutionAttemptID(), slot, null, 1, false, null, null, null, new HashSet<>());

			Collection<ResultPartitionDeploymentDescriptor> producedPartitions = tdd.getProducedPartitions();

			assertEquals(1, producedPartitions.size());
			ResultPartitionDeploymentDescriptor desc = producedPartitions.iterator().next();
			assertEquals(mode.allowLazyDeployment(), desc.sendScheduleOrUpdateConsumersMessage());
		}
	}



	private ExecutionEdge createMockExecutionEdge(int maxParallelism) {
		ExecutionVertex targetVertex = mock(ExecutionVertex.class);
		ExecutionJobVertex targetJobVertex = mock(ExecutionJobVertex.class);

		when(targetVertex.getJobVertex()).thenReturn(targetJobVertex);
		when(targetJobVertex.getMaxParallelism()).thenReturn(maxParallelism);

		ExecutionEdge edge = mock(ExecutionEdge.class);
		when(edge.getTarget()).thenReturn(targetVertex);
		return edge;
	}
}
