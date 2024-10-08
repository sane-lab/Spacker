/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.api.operators;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.core.fs.CloseableRegistry;
import org.apache.flink.core.testutils.OneShotLatch;
import org.apache.flink.runtime.checkpoint.CheckpointOptions;
import org.apache.flink.runtime.checkpoint.StateObjectCollection;
import org.apache.flink.runtime.state.CheckpointStreamFactory;
import org.apache.flink.runtime.state.DefaultOperatorStateBackendBuilder;
import org.apache.flink.runtime.state.OperatorStateBackend;
import org.apache.flink.runtime.state.OperatorStateHandle;
import org.apache.flink.runtime.state.SnapshotResult;
import org.apache.flink.runtime.state.memory.MemCheckpointStreamFactory;
import org.apache.flink.runtime.util.BlockingFSDataInputStream;
import org.apache.flink.util.FlinkException;
import org.apache.flink.util.TestLogger;
import org.apache.flink.util.function.FunctionWithException;

import org.junit.Assert;
import org.junit.Test;

import java.util.*;
import java.util.concurrent.RunnableFuture;
import java.util.concurrent.atomic.AtomicReference;

import static org.mockito.Mockito.verify;
import static org.powermock.api.mockito.PowerMockito.mock;
import static org.powermock.api.mockito.PowerMockito.spy;
import static org.powermock.api.mockito.PowerMockito.verifyZeroInteractions;
import static org.powermock.api.mockito.PowerMockito.when;

/**
 * Tests for {@link BackendRestorerProcedure}.
 */
public class BackendRestorerProcedureTest extends TestLogger {

	private final FunctionWithException<Collection<OperatorStateHandle>, OperatorStateBackend, Exception> backendSupplier =
		(stateHandles) -> new DefaultOperatorStateBackendBuilder(
			getClass().getClassLoader(),
			new ExecutionConfig(),
			true,
			stateHandles,
			new CloseableRegistry()).build();

	/**
	 * Tests that the restore procedure follows the order of the iterator and will retries failed attempts if there are
	 * more options.
	 */
	@Test
	public void testRestoreProcedureOrderAndFailure() throws Exception {

		CloseableRegistry closeableRegistry = new CloseableRegistry();
		CheckpointStreamFactory checkpointStreamFactory = new MemCheckpointStreamFactory(1024);

		ListStateDescriptor<Integer> stateDescriptor = new ListStateDescriptor<>("test-state", Integer.class);
		OperatorStateBackend originalBackend = backendSupplier.apply(Collections.emptyList());
		SnapshotResult<OperatorStateHandle> snapshotResult;

		try {
			ListState<Integer> listState = originalBackend.getListState(stateDescriptor);

			listState.add(0);
			listState.add(1);
			listState.add(2);
			listState.add(3);

			RunnableFuture<SnapshotResult<OperatorStateHandle>> snapshot =
				originalBackend.snapshot(0L, 0L, checkpointStreamFactory, CheckpointOptions.forCheckpointWithDefaultLocation(), true, new HashSet<>());

			snapshot.run();
			snapshotResult = snapshot.get();

		} finally {
			originalBackend.close();
			originalBackend.dispose();
		}

		OperatorStateHandle firstFailHandle = mock(OperatorStateHandle.class);
		OperatorStateHandle secondSuccessHandle = spy(snapshotResult.getJobManagerOwnedSnapshot());
		OperatorStateHandle thirdNotUsedHandle = mock(OperatorStateHandle.class);

		List<StateObjectCollection<OperatorStateHandle>> sortedRestoreOptions = Arrays.asList(
			new StateObjectCollection<>(Collections.singletonList(firstFailHandle)),
			new StateObjectCollection<>(Collections.singletonList(secondSuccessHandle)),
			new StateObjectCollection<>(Collections.singletonList(thirdNotUsedHandle)));

		BackendRestorerProcedure<OperatorStateBackend, OperatorStateHandle> restorerProcedure =
			new BackendRestorerProcedure<>(backendSupplier, closeableRegistry, "test op state backend");

		OperatorStateBackend restoredBackend = restorerProcedure.createAndRestore(sortedRestoreOptions);
		Assert.assertNotNull(restoredBackend);

		try {
			verify(firstFailHandle).openInputStream();
			verify(secondSuccessHandle).openInputStream();
			verifyZeroInteractions(thirdNotUsedHandle);

			ListState<Integer> listState = restoredBackend.getListState(stateDescriptor);

			Iterator<Integer> stateIterator = listState.get().iterator();
			Assert.assertEquals(0, (int) stateIterator.next());
			Assert.assertEquals(1, (int) stateIterator.next());
			Assert.assertEquals(2, (int) stateIterator.next());
			Assert.assertEquals(3, (int) stateIterator.next());
			Assert.assertFalse(stateIterator.hasNext());

		} finally {
			restoredBackend.close();
			restoredBackend.dispose();
		}
	}

	/**
	 * Tests if there is an exception if all restore attempts are exhausted and failed.
	 */
	@Test
	public void testExceptionThrownIfAllRestoresFailed() throws Exception {

		CloseableRegistry closeableRegistry = new CloseableRegistry();

		OperatorStateHandle firstFailHandle = mock(OperatorStateHandle.class);
		OperatorStateHandle secondFailHandle = mock(OperatorStateHandle.class);
		OperatorStateHandle thirdFailHandle = mock(OperatorStateHandle.class);

		List<StateObjectCollection<OperatorStateHandle>> sortedRestoreOptions = Arrays.asList(
			new StateObjectCollection<>(Collections.singletonList(firstFailHandle)),
			new StateObjectCollection<>(Collections.singletonList(secondFailHandle)),
			new StateObjectCollection<>(Collections.singletonList(thirdFailHandle)));

		BackendRestorerProcedure<OperatorStateBackend, OperatorStateHandle> restorerProcedure =
			new BackendRestorerProcedure<>(backendSupplier, closeableRegistry, "test op state backend");

		try {
			restorerProcedure.createAndRestore(sortedRestoreOptions);
			Assert.fail();
		} catch (Exception ignore) {
		}

		verify(firstFailHandle).openInputStream();
		verify(secondFailHandle).openInputStream();
		verify(thirdFailHandle).openInputStream();
	}

	/**
	 * Test that the restore can be stopped via the provided closeable registry.
	 */
	@Test
	public void testCanBeCanceledViaRegistry() throws Exception {
		CloseableRegistry closeableRegistry = new CloseableRegistry();
		OneShotLatch waitForBlock = new OneShotLatch();
		OneShotLatch unblock = new OneShotLatch();
		OperatorStateHandle blockingRestoreHandle = mock(OperatorStateHandle.class);
		when(blockingRestoreHandle.openInputStream()).thenReturn(new BlockingFSDataInputStream(waitForBlock, unblock));

		List<StateObjectCollection<OperatorStateHandle>> sortedRestoreOptions =
			Collections.singletonList(new StateObjectCollection<>(Collections.singletonList(blockingRestoreHandle)));

		BackendRestorerProcedure<OperatorStateBackend, OperatorStateHandle> restorerProcedure =
			new BackendRestorerProcedure<>(backendSupplier, closeableRegistry, "test op state backend");

		AtomicReference<Exception> exceptionReference = new AtomicReference<>(null);
		Thread restoreThread = new Thread(() -> {
			try {
				restorerProcedure.createAndRestore(sortedRestoreOptions);
			} catch (Exception e) {
				exceptionReference.set(e);
			}
		});

		restoreThread.start();
		waitForBlock.await();
		closeableRegistry.close();
		unblock.trigger();
		restoreThread.join();

		Exception exception = exceptionReference.get();
		Assert.assertTrue(exception instanceof FlinkException);
	}
}
