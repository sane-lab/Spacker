/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.runtime.tasks;

import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.TaskInfo;
import org.apache.flink.api.common.accumulators.Accumulator;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.core.fs.CloseableRegistry;
import org.apache.flink.core.fs.FSDataInputStream;
import org.apache.flink.core.fs.FileSystemSafetyNet;
import org.apache.flink.core.memory.DataInputViewStreamWrapper;
import org.apache.flink.runtime.checkpoint.*;
import org.apache.flink.runtime.concurrent.FutureUtils;
import org.apache.flink.runtime.execution.CancelTaskException;
import org.apache.flink.runtime.execution.Environment;
import org.apache.flink.runtime.io.network.api.CancelCheckpointMarker;
import org.apache.flink.runtime.io.network.api.writer.RecordWriter;
import org.apache.flink.runtime.io.network.api.writer.ResultPartitionWriter;
import org.apache.flink.runtime.io.network.partition.ResultPartition;
import org.apache.flink.runtime.io.network.partition.consumer.InputGate;
import org.apache.flink.runtime.io.network.partition.consumer.SingleInputGate;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.jobgraph.tasks.AbstractInvokable;
import org.apache.flink.runtime.messages.Acknowledge;
import org.apache.flink.runtime.plugable.SerializationDelegate;
import org.apache.flink.runtime.spector.TaskConfigManager;
import org.apache.flink.runtime.state.*;
import org.apache.flink.runtime.state.memory.ByteStreamStateHandle;
import org.apache.flink.runtime.taskexecutor.TaskExecutorGateway;
import org.apache.flink.runtime.taskmanager.DispatcherThreadFactory;
import org.apache.flink.runtime.taskmanager.RuntimeEnvironment;
import org.apache.flink.runtime.util.profiling.MetricsManager;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.graph.StreamConfig;
import org.apache.flink.streaming.api.graph.StreamEdge;
import org.apache.flink.streaming.api.operators.OperatorSnapshotFinalizer;
import org.apache.flink.streaming.api.operators.OperatorSnapshotFutures;
import org.apache.flink.streaming.api.operators.StreamOperator;
import org.apache.flink.streaming.api.operators.StreamTaskStateInitializer;
import org.apache.flink.streaming.api.operators.StreamTaskStateInitializerImpl;
import org.apache.flink.streaming.runtime.io.RecordWriterOutput;
import org.apache.flink.streaming.runtime.partitioner.ConfigurableStreamPartitioner;
import org.apache.flink.streaming.runtime.partitioner.StreamPartitioner;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.streamstatus.StreamStatusMaintainer;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.Preconditions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.Closeable;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import static org.apache.flink.runtime.checkpoint.CheckpointType.RECONFIGPOINT;
import static org.apache.flink.runtime.spector.SpectorOptions.SNAPSHOT_CHANGELOG_ENABLED;

/**
 * Base class for all streaming tasks. A task is the unit of local processing that is deployed
 * and executed by the TaskManagers. Each task runs one or more {@link StreamOperator}s which form
 * the Task's operator chain. Operators that are chained together execute synchronously in the
 * same thread and hence on the same stream partition. A common case for these chains
 * are successive map/flatmap/filter tasks.
 *
 * <p>The task chain contains one "head" operator and multiple chained operators.
 * The StreamTask is specialized for the type of the head operator: one-input and two-input tasks,
 * as well as for sources, iteration heads and iteration tails.
 *
 * <p>The Task class deals with the setup of the streams read by the head operator, and the streams
 * produced by the operators at the ends of the operator chain. Note that the chain may fork and
 * thus have multiple ends.
 *
 * <p>The life cycle of the task is set up as follows:
 * <pre>{@code
 *  -- setInitialState -> provides state of all operators in the chain
 *
 *  -- invoke()
 *        |
 *        +----> Create basic utils (config, etc) and load the chain of operators
 *        +----> operators.setup()
 *        +----> task specific init()
 *        +----> initialize-operator-states()
 *        +----> open-operators()
 *        +----> run()
 *        +----> close-operators()
 *        +----> dispose-operators()
 *        +----> common cleanup
 *        +----> task specific cleanup()
 * }</pre>
 *
 * <p>The {@code StreamTask} has a lock object called {@code lock}. All calls to methods on a
 * {@code StreamOperator} must be synchronized on this lock object to ensure that no methods
 * are called concurrently.
 *
 * @param <OUT>
 * @param <OP>
 */
@Internal
public abstract class StreamTask<OUT, OP extends StreamOperator<OUT>>
		extends AbstractInvokable
		implements AsyncExceptionHandler {

	/** The thread group that holds all trigger timer threads. */
	public static final ThreadGroup TRIGGER_THREAD_GROUP = new ThreadGroup("Triggers");

	/** The logger used by the StreamTask and its subclasses. */
	private static final Logger LOG = LoggerFactory.getLogger(StreamTask.class);

	// ------------------------------------------------------------------------

	/**
	 * All interaction with the {@code StreamOperator} must be synchronized on this lock object to
	 * ensure that we don't have concurrent method calls that void consistent checkpoints.
	 */
	private final Object lock = new Object();

	/** the head operator that consumes the input streams of this task. */
	protected OP headOperator;

	/** The chain of operators executed by this task. */
	protected OperatorChain<OUT, OP> operatorChain;

	/** The configuration of this streaming task. */
	protected final StreamConfig configuration;

	/** Our state backend. We use this to create checkpoint streams and a keyed state backend. */
	protected StateBackend stateBackend;

	/** The external storage where checkpoint data is persisted. */
	private CheckpointStorage checkpointStorage;

	/**
	 * The internal {@link ProcessingTimeService} used to define the current
	 * processing time (default = {@code System.currentTimeMillis()}) and
	 * register timers for tasks to be executed in the future.
	 */
	protected ProcessingTimeService timerService;

	/** The map of user-defined accumulators of this task. */
	private final Map<String, Accumulator<?, ?>> accumulatorMap;

	/** The currently active background materialization threads. */
	private final CloseableRegistry cancelables = new CloseableRegistry();

	/**
	 * Flag to mark the task "in operation", in which case check needs to be initialized to true,
	 * so that early cancel() before invoke() behaves correctly.
	 */
	private volatile boolean isRunning;

	/** Flag to mark this task as canceled. */
	private volatile boolean canceled;

	/** Thread pool for async snapshot workers. */
	private ExecutorService asyncOperationsThreadPool;

	/** Handler for exceptions during checkpointing in the stream task. Used in synchronous part of the checkpoint. */
	private CheckpointExceptionHandler synchronousCheckpointExceptionHandler;

	/** Wrapper for synchronousCheckpointExceptionHandler to deal with rethrown exceptions. Used in the async part. */
	private AsyncCheckpointExceptionHandler asynchronousCheckpointExceptionHandler;

	private List<RecordWriter<SerializationDelegate<StreamRecord<OUT>>>> recordWriters;

	private final KeyGroupRange assignedKeyGroupRange;

	private volatile int idInModel;

	/**
	 * Future for standby tasks that completes when they are required to run
	 */
	private final CompletableFuture<Void> standbyFuture;

	// ------------------------------------------------------------------------

	/**
	 * Constructor for initialization, possibly with initial state (recovery / savepoint / etc).
	 *
	 * @param env The task environment for this task.
	 */
	protected StreamTask(Environment env) {
		this(env, null);
	}

	/**
	 * Constructor for initialization, possibly with initial state (recovery / savepoint / etc).
	 *
	 * <p>This constructor accepts a special {@link ProcessingTimeService}. By default (and if
	 * null is passes for the time provider) a {@link SystemProcessingTimeService DefaultTimerService}
	 * will be used.
	 *
	 * @param environment The task environment for this task.
	 * @param timeProvider Optionally, a specific time provider to use.
	 */
	protected StreamTask(
			Environment environment,
			@Nullable ProcessingTimeService timeProvider) {

		super(environment);

		this.timerService = timeProvider;
		this.configuration = new StreamConfig(getTaskConfiguration());
		this.accumulatorMap = getEnvironment().getAccumulatorRegistry().getUserMap();

		if (isStandby()) {
			this.standbyFuture = new CompletableFuture<>();
		} else {
			this.standbyFuture = null;
			this.recordWriters = createRecordWriters(configuration, environment);
		}

		KeyGroupRange range = ((RuntimeEnvironment) getEnvironment()).keyGroupRange;
		TaskInfo taskInfo = getEnvironment().getTaskInfo();

		this.assignedKeyGroupRange = range != null ? range :
			KeyGroupRangeAssignment.computeKeyGroupRangeForOperatorIndex(
				taskInfo.getMaxNumberOfParallelSubtasks(),
				taskInfo.getNumberOfParallelSubtasks(),
				taskInfo.getIndexOfThisSubtask());

		this.idInModel = getEnvironment().getTaskInfo().getIndexOfThisSubtask();
	}

	// ------------------------------------------------------------------------
	//  Life cycle methods for specific implementations
	// ------------------------------------------------------------------------

	protected abstract void init() throws Exception;

	protected abstract void run() throws Exception;

	protected abstract void cleanup() throws Exception;

	protected abstract void cancelTask() throws Exception;

	// ------------------------------------------------------------------------
	//  Core work methods of the Stream Task
	// ------------------------------------------------------------------------

	public StreamTaskStateInitializer createStreamTaskStateInitializer() {
		return new StreamTaskStateInitializerImpl(
			getEnvironment(),
			stateBackend,
			timerService);
	}

	@Override
	public final void invoke() throws Exception {

		boolean disposed = false;
		try {
			// Block until the standby task is requested to run.
			// In the meantime checkpointed state snapshots of the running task mirrored by the
			// standby task are dispatched to the standby task. See Task.dispatchStateToStandbyTask().
			// Also block until input channel connections are ready, determinants have arrived and we are ready to
			// replay.
			if (isStandby())
				blockStandbyTask();

			// -------- Initialize ---------
			LOG.debug("Initializing {}.", getName());

			asyncOperationsThreadPool = Executors.newCachedThreadPool();

			CheckpointExceptionHandlerFactory cpExceptionHandlerFactory = createCheckpointExceptionHandlerFactory();

			synchronousCheckpointExceptionHandler = cpExceptionHandlerFactory.createCheckpointExceptionHandler(
				getExecutionConfig().isFailTaskOnCheckpointError(),
				getEnvironment());

			asynchronousCheckpointExceptionHandler = new AsyncCheckpointExceptionHandler(this);

			stateBackend = createStateBackend();
			checkpointStorage = stateBackend.createCheckpointStorage(getEnvironment().getJobID());

			// if the clock is not already set, then assign a default TimeServiceProvider
			if (timerService == null) {
				ThreadFactory timerThreadFactory = new DispatcherThreadFactory(TRIGGER_THREAD_GROUP,
					"Time Trigger for " + getName(), getUserCodeClassLoader());

				timerService = new SystemProcessingTimeService(this, getCheckpointLock(), timerThreadFactory);
			}

			operatorChain = new OperatorChain<>(this, recordWriters);
			headOperator = operatorChain.getHeadOperator();

			// task specific initialization
			init();

			// save the work of reloading state, etc, if the task is already canceled
			if (canceled) {
				throw new CancelTaskException();
			}

			// -------- Invoke --------
			LOG.debug("Invoking {}", getName());

			// we need to make sure that any triggers scheduled in open() cannot be
			// executed before all operators are opened
			synchronized (lock) {

				// both the following operations are protected by the lock
				// so that we avoid race conditions in the case that initializeState()
				// registers a timer, that fires before the open() is called.

				initializeState();
			}

			// we need to make sure that any triggers scheduled in open() cannot be
			// executed before all operators are opened
			synchronized (lock) {
				openAllOperators();
			}

			// final check to exit early before starting to run
			if (canceled) {
				throw new CancelTaskException();
			}

			// let the task do its work
			isRunning = true;
			run();

			// if this left the run() method cleanly despite the fact that this was canceled,
			// make sure the "clean shutdown" is not attempted
			if (canceled) {
				throw new CancelTaskException();
			}

			LOG.debug("Finished task {}", getName());

			// make sure no further checkpoint and notification actions happen.
			// we make sure that no other thread is currently in the locked scope before
			// we close the operators by trying to acquire the checkpoint scope lock
			// we also need to make sure that no triggers fire concurrently with the close logic
			// at the same time, this makes sure that during any "regular" exit where still
			synchronized (lock) {
				// this is part of the main logic, so if this fails, the task is considered failed
				closeAllOperators();

				// make sure no new timers can come
				timerService.quiesce();

				// only set the StreamTask to not running after all operators have been closed!
				// See FLINK-7430
				isRunning = false;
			}

			// make sure all timers finish
			timerService.awaitPendingAfterQuiesce();

			LOG.debug("Closed operators for task {}", getName());

			// make sure all buffered data is flushed
			operatorChain.flushOutputs();

			// make an attempt to dispose the operators such that failures in the dispose call
			// still let the computation fail
			tryDisposeAllOperators();
			disposed = true;
		}
		finally {
			// clean up everything we initialized
			isRunning = false;

			// Now that we are outside the user code, we do not want to be interrupted further
			// upon cancellation. The shutdown logic below needs to make sure it does not issue calls
			// that block and stall shutdown.
			// Additionally, the cancellation watch dog will issue a hard-cancel (kill the TaskManager
			// process) as a backup in case some shutdown procedure blocks outside our control.
			setShouldInterruptOnCancel(false);

			// clear any previously issued interrupt for a more graceful shutdown
			Thread.interrupted();

			// stop all timers and threads
			tryShutdownTimerService();

			// stop all asynchronous checkpoint threads
			try {
				cancelables.close();
				shutdownAsyncThreads();
			}
			catch (Throwable t) {
				// catch and log the exception to not replace the original exception
				LOG.error("Could not shut down async checkpoint threads", t);
			}

			// we must! perform this cleanup
			try {
				cleanup();
			}
			catch (Throwable t) {
				// catch and log the exception to not replace the original exception
				LOG.error("Error during cleanup of stream task", t);
			}

			// if the operators were not disposed before, do a hard dispose
			if (!disposed) {
				disposeAllOperators();
			}

			// release the output resources. this method should never fail.
			if (operatorChain != null) {
				// beware: without synchronization, #performCheckpoint() may run in
				//         parallel and this call is not thread-safe
				synchronized (lock) {
					operatorChain.releaseOutputs();
				}
			}
		}
	}

	private void blockStandbyTask() throws InterruptedException, ExecutionException {
		getEnvironment().getContainingTask().transitionToStandbyState();
		standbyFuture.get();
		// TODO: By far, we do not need to make standby tasks to replay/switch to running
		LOG.debug("Task {} starts recovery after standbyFuture {}.", getName(), standbyFuture);
	}


	@Override
	public final void cancel() throws Exception {
		isRunning = false;
		canceled = true;

		// the "cancel task" call must come first, but the cancelables must be
		// closed no matter what
		try {
			cancelTask();
		}
		finally {
			cancelables.close();
		}
	}

	public final boolean isRunning() {
		return isRunning;
	}

	public final boolean isCanceled() {
		return canceled;
	}

	public final boolean isStandby() {
		return getEnvironment().getContainingTask().getIsStandby();
	}

	/**
	 * Execute {@link StreamOperator#open()} of each operator in the chain of this
	 * {@link StreamTask}. Opening happens from <b>tail to head</b> operator in the chain, contrary
	 * to {@link StreamOperator#close()} which happens <b>head to tail</b>
	 * (see {@link #closeAllOperators()}.
	 */
	private void openAllOperators() throws Exception {
		for (StreamOperator<?> operator : operatorChain.getAllOperators()) {
			if (operator != null) {
				operator.open();
			}
		}
	}

	/**
	 * Execute {@link StreamOperator#close()} of each operator in the chain of this
	 * {@link StreamTask}. Closing happens from <b>head to tail</b> operator in the chain,
	 * contrary to {@link StreamOperator#open()} which happens <b>tail to head</b>
	 * (see {@link #openAllOperators()}.
	 */
	private void closeAllOperators() throws Exception {
		// We need to close them first to last, since upstream operators in the chain might emit
		// elements in their close methods.
		StreamOperator<?>[] allOperators = operatorChain.getAllOperators();
		for (int i = allOperators.length - 1; i >= 0; i--) {
			StreamOperator<?> operator = allOperators[i];
			if (operator != null) {
				operator.close();
			}
		}
	}

	/**
	 * Execute {@link StreamOperator#dispose()} of each operator in the chain of this
	 * {@link StreamTask}. Disposing happens from <b>tail to head</b> operator in the chain.
	 */
	private void tryDisposeAllOperators() throws Exception {
		for (StreamOperator<?> operator : operatorChain.getAllOperators()) {
			if (operator != null) {
				operator.dispose();
			}
		}
	}

	private void shutdownAsyncThreads() throws Exception {
		if (!asyncOperationsThreadPool.isShutdown()) {
			asyncOperationsThreadPool.shutdownNow();
		}
	}

	/**
	 * Execute @link StreamOperator#dispose()} of each operator in the chain of this
	 * {@link StreamTask}. Disposing happens from <b>tail to head</b> operator in the chain.
	 *
	 * <p>The difference with the {@link #tryDisposeAllOperators()} is that in case of an
	 * exception, this method catches it and logs the message.
	 */
	private void disposeAllOperators() {
		if (operatorChain != null) {
			for (StreamOperator<?> operator : operatorChain.getAllOperators()) {
				try {
					if (operator != null) {
						operator.dispose();
					}
				}
				catch (Throwable t) {
					LOG.error("Error during disposal of stream operator.", t);
				}
			}
		}
	}

	/**
	 * The finalize method shuts down the timer. This is a fail-safe shutdown, in case the original
	 * shutdown method was never called.
	 *
	 * <p>This should not be relied upon! It will cause shutdown to happen much later than if manual
	 * shutdown is attempted, and cause threads to linger for longer than needed.
	 */
	@Override
	protected void finalize() throws Throwable {
		super.finalize();
		if (timerService != null) {
			if (!timerService.isTerminated()) {
				LOG.info("Timer service is shutting down.");
				timerService.shutdownService();
			}
		}

		cancelables.close();
	}

	boolean isSerializingTimestamps() {
		TimeCharacteristic tc = configuration.getTimeCharacteristic();
		return tc == TimeCharacteristic.EventTime | tc == TimeCharacteristic.IngestionTime;
	}

	// ------------------------------------------------------------------------
	//  Access to properties and utilities
	// ------------------------------------------------------------------------

	/**
	 * Gets the name of the task, in the form "taskname (2/5)".
	 * @return The name of the task.
	 */
	public String getName() {
		return getEnvironment().getTaskInfo().getTaskNameWithSubtasks();
	}

	public MetricsManager getMetricsManager() {
		return getEnvironment().getMetricsManager();
	}

	/**
	 * Gets the lock object on which all operations that involve data and state mutation have to lock.
	 * @return The checkpoint lock object.
	 */
	public Object getCheckpointLock() {
		return lock;
	}

	public CheckpointStorage getCheckpointStorage() {
		return checkpointStorage;
	}

	public StreamConfig getConfiguration() {
		return configuration;
	}

	public Map<String, Accumulator<?, ?>> getAccumulatorMap() {
		return accumulatorMap;
	}

	public StreamStatusMaintainer getStreamStatusMaintainer() {
		return operatorChain;
	}

	RecordWriterOutput<?>[] getStreamOutputs() {
		return operatorChain.getStreamOutputs();
	}

	public KeyGroupRange getAssignedKeyGroupRange() {
		return assignedKeyGroupRange;
	}

	// ------------------------------------------------------------------------
	//  Checkpoint and Restore
	// ------------------------------------------------------------------------

	@Override
	public boolean triggerCheckpoint(CheckpointMetaData checkpointMetaData, CheckpointOptions checkpointOptions) throws Exception {
		try {
			// No alignment if we inject a checkpoint
			CheckpointMetrics checkpointMetrics = new CheckpointMetrics()
					.setBytesBufferedInAlignment(0L)
					.setAlignmentDurationNanos(0L);

			return performCheckpoint(checkpointMetaData, checkpointOptions, checkpointMetrics);
		}
		catch (Exception e) {
			// propagate exceptions only if the task is still in "running" state
			if (isRunning) {
				throw new Exception("Could not perform checkpoint " + checkpointMetaData.getCheckpointId() +
					" for operator " + getName() + '.', e);
			} else {
				LOG.debug("Could not perform checkpoint {} for operator {} while the " +
					"invokable was not in state running.", checkpointMetaData.getCheckpointId(), getName(), e);
				return false;
			}
		}
	}

	@Override
	public void triggerCheckpointOnBarrier(
			CheckpointMetaData checkpointMetaData,
			CheckpointOptions checkpointOptions,
			CheckpointMetrics checkpointMetrics) throws Exception {

		try {
			performCheckpoint(checkpointMetaData, checkpointOptions, checkpointMetrics);
		}
		catch (CancelTaskException e) {
			LOG.info("Operator {} was cancelled while performing checkpoint {}.",
					getName(), checkpointMetaData.getCheckpointId());
			throw e;
		}
		catch (Exception e) {
			throw new Exception("Could not perform checkpoint " + checkpointMetaData.getCheckpointId() + " for operator " +
				getName() + '.', e);
		}
	}

	@Override
	public void abortCheckpointOnBarrier(long checkpointId, Throwable cause) throws Exception {
		LOG.debug("Aborting checkpoint via cancel-barrier {} for task {}", checkpointId, getName());

		// notify the coordinator that we decline this checkpoint
		getEnvironment().declineCheckpoint(checkpointId, cause);

		// notify all downstream operators that they should not wait for a barrier from us
		synchronized (lock) {
			operatorChain.broadcastCheckpointCancelMarker(checkpointId);
		}
	}

	private boolean performCheckpoint(
			CheckpointMetaData checkpointMetaData,
			CheckpointOptions checkpointOptions,
			CheckpointMetrics checkpointMetrics) throws Exception {

		LOG.debug("Starting checkpoint ({}) {} on task {}",
			checkpointMetaData.getCheckpointId(), checkpointOptions.getCheckpointType(), getName());

		synchronized (lock) {
			if (isRunning) {
				// we can do a checkpoint

				// All of the following steps happen as an atomic step from the perspective of barriers and
				// records/watermarks/timers/callbacks.
				// We generally try to emit the checkpoint barrier as soon as possible to not affect downstream
				// checkpoint alignments

				// Step (1): Prepare the checkpoint, allow operators to do some pre-barrier work.
				//           The pre-barrier work should be nothing or minimal in the common case.
				operatorChain.prepareSnapshotPreBarrier(checkpointMetaData.getCheckpointId());

				// Step (2): Send the checkpoint barrier downstream
				operatorChain.broadcastCheckpointBarrier(
						checkpointMetaData.getCheckpointId(),
						checkpointMetaData.getTimestamp(),
						checkpointOptions);

//				// Step (3): Take the state snapshot. This should be largely asynchronous, to not
//				//           impact progress of the streaming topology
//				checkpointState(checkpointMetaData, checkpointOptions, checkpointMetrics);
//
//				// Step (4): Check whether the checkpoint is reconfigpoint type, and do rescaling if it is.
//				checkReconfigPoint(checkpointMetaData, checkpointOptions, checkpointMetrics);


				// Step (3): Take the state snapshot. This should be largely asynchronous, to not
				//           impact progress of the streaming topology
				if (checkpointOptions.getCheckpointType() != RECONFIGPOINT) {
					checkpointState(checkpointMetaData, checkpointOptions, checkpointMetrics);
				} else {
					// TODO: only the affected task i.e. the task to migrate should take snapshot, other tasks should keep unaffected.
					// TODO: only snapshot on affected state tables and ack to coordinator accordingly
					checkpointAffectedState(checkpointMetaData, checkpointOptions, checkpointMetrics);
					// Step (4): Check whether the checkpoint is rescalepoint type, and do rescaling if it is.
					checkReconfigPoint(checkpointMetaData, checkpointOptions, checkpointMetrics);
				}

				return true;
			}
			else {
				// we cannot perform our checkpoint - let the downstream operators know that they
				// should not wait for any input from this operator

				// we cannot broadcast the cancellation markers on the 'operator chain', because it may not
				// yet be created
				final CancelCheckpointMarker message = new CancelCheckpointMarker(checkpointMetaData.getCheckpointId());
				Exception exception = null;

				for (RecordWriter<SerializationDelegate<StreamRecord<OUT>>> recordWriter : recordWriters) {
					try {
						recordWriter.broadcastEvent(message);
					} catch (Exception e) {
						exception = ExceptionUtils.firstOrSuppressed(
							new Exception("Could not send cancel checkpoint marker to downstream tasks.", e),
							exception);
					}
				}

				if (exception != null) {
					throw exception;
				}

				return false;
			}
		}
	}

	public ExecutorService getAsyncOperationsThreadPool() {
		return asyncOperationsThreadPool;
	}

	@Override
	public void notifyCheckpointComplete(long checkpointId) throws Exception {
		synchronized (lock) {
			if (isRunning) {
				LOG.debug("Notification of complete checkpoint for task {}", getName());

				for (StreamOperator<?> operator : operatorChain.getAllOperators()) {
					if (operator != null) {
						operator.notifyCheckpointComplete(checkpointId);
					}
				}
			}
			else {
				LOG.debug("Ignoring notification of complete checkpoint for not-running task {}", getName());
			}
		}
	}

	private void tryShutdownTimerService() {

		if (timerService != null && !timerService.isTerminated()) {

			try {
				final long timeoutMs = getEnvironment().getTaskManagerInfo().getConfiguration().
					getLong(TaskManagerOptions.TASK_CANCELLATION_TIMEOUT_TIMERS);

				if (!timerService.shutdownServiceUninterruptible(timeoutMs)) {
					LOG.warn("Timer service shutdown exceeded time limit of {} ms while waiting for pending " +
						"timers. Will continue with shutdown procedure.", timeoutMs);
				}
			} catch (Throwable t) {
				// catch and log the exception to not replace the original exception
				LOG.error("Could not shut down timer service", t);
			}
		}
	}

	private void checkpointState(
			CheckpointMetaData checkpointMetaData,
			CheckpointOptions checkpointOptions,
			CheckpointMetrics checkpointMetrics) throws Exception {

		CheckpointStreamFactory storage = checkpointStorage.resolveCheckpointStorageLocation(
				checkpointMetaData.getCheckpointId(),
				checkpointOptions.getTargetLocation());

		CheckpointingOperation checkpointingOperation = new CheckpointingOperation(
			this,
			checkpointMetaData,
			checkpointOptions,
			storage,
			checkpointMetrics,
			getEnvironment().getJobConfiguration().getBoolean(SNAPSHOT_CHANGELOG_ENABLED),
			((RuntimeEnvironment) getEnvironment()).getTaskConfigManager().getBackupKeyGroups());

		checkpointingOperation.executeCheckpointing();
	}

	private void checkpointAffectedState(
		CheckpointMetaData checkpointMetaData,
		CheckpointOptions checkpointOptions,
		CheckpointMetrics checkpointMetrics) throws Exception {

		CheckpointStreamFactory storage = checkpointStorage.resolveCheckpointStorageLocation(
			checkpointMetaData.getCheckpointId(),
			checkpointOptions.getTargetLocation());

		TaskConfigManager taskConfigManager = ((RuntimeEnvironment) getEnvironment()).getTaskConfigManager();
		List<Integer> affectedKeyGroups = new ArrayList<>();
		if (taskConfigManager.isReconfigTarget()) {
			if (taskConfigManager.isSource()) {
				affectedKeyGroups.addAll(taskConfigManager.getSrcAffectedKeyGroups());
			}
		}

		SnapshotAffectedStateOperation snapshotAffectedStateOperation = new SnapshotAffectedStateOperation(
			this,
			checkpointMetaData,
			checkpointOptions,
			storage,
			checkpointMetrics,
			affectedKeyGroups,
			((RuntimeEnvironment) getEnvironment()).getTaskConfigManager().getBackupKeyGroups());

		snapshotAffectedStateOperation.executeAffectedStateSnapshot();
	}

	private void initializeState() throws Exception {

		StreamOperator<?>[] allOperators = operatorChain.getAllOperators();

		for (StreamOperator<?> operator : allOperators) {
			if (null != operator) {
				operator.initializeState();
			}
		}
	}

	// ------------------------------------------------------------------------
	//  State backend
	// ------------------------------------------------------------------------

	private StateBackend createStateBackend() throws Exception {
		final StateBackend fromApplication = configuration.getStateBackend(getUserCodeClassLoader());

		return StateBackendLoader.fromApplicationOrConfigOrDefault(
				fromApplication,
				getEnvironment().getTaskManagerInfo().getConfiguration(),
				getUserCodeClassLoader(),
				LOG);
	}

	protected CheckpointExceptionHandlerFactory createCheckpointExceptionHandlerFactory() {
		return new CheckpointExceptionHandlerFactory();
	}

	/**
	 * Returns the {@link ProcessingTimeService} responsible for telling the current
	 * processing time and registering timers.
	 */
	public ProcessingTimeService getProcessingTimeService() {
		if (timerService == null) {
			throw new IllegalStateException("The timer service has not been initialized.");
		}
		return timerService;
	}

	/**
	 * Handles an exception thrown by another thread (e.g. a TriggerTask),
	 * other than the one executing the main task by failing the task entirely.
	 *
	 * <p>In more detail, it marks task execution failed for an external reason
	 * (a reason other than the task code itself throwing an exception). If the task
	 * is already in a terminal state (such as FINISHED, CANCELED, FAILED), or if the
	 * task is already canceling this does nothing. Otherwise it sets the state to
	 * FAILED, and, if the invokable code is running, starts an asynchronous thread
	 * that aborts that code.
	 *
	 * <p>This method never blocks.
	 */
	@Override
	public void handleAsyncException(String message, Throwable exception) {
		if (isRunning) {
			// only fail if the task is still running
			getEnvironment().failExternally(exception);
		}
	}

	// ------------------------------------------------------------------------
	//  Reconfig
	// ------------------------------------------------------------------------

	protected void reconnect() { // reconnect to upstream and downstream
		throw new UnsupportedOperationException("++++++ reconnect not supported for current extension");
	}
	protected void resume() { // resume processing of updated keys
		throw new UnsupportedOperationException("++++++ resume not supported for current extension");
	}

	@Override
	public void resume(int keygroup) { // resume processing of updated keys
		throw new UnsupportedOperationException("++++++ resume not supported for current extension");
	}

	private void initReconnect() {
		TaskConfigManager taskConfigManager = ((RuntimeEnvironment) getEnvironment()).getTaskConfigManager();

		if (!taskConfigManager.isReconfigTarget()) {
			return;
		}
		LOG.info("++++++ trigger target vertex rescaling: " + this);

		try {
			// update gate
			if (taskConfigManager.isUpdateGates()) {
				for (InputGate gate : getEnvironment().getAllInputGates()) {
					taskConfigManager.substituteInputGateChannels((SingleInputGate) gate);
				}
			}

			// update output (writers)
			if (taskConfigManager.isUpdatePartitions()) {
				ResultPartitionWriter[] oldWriterCopies =
					taskConfigManager.substituteResultPartitions(getEnvironment().getAllWriters());

				recordWriters = createRecordWriters(configuration, getEnvironment());

				RecordWriterOutput[] oldStreamOutputCopies =
					operatorChain.substituteRecordWriter(this, recordWriters);

				//  close old output and unregister partitions
				for (RecordWriterOutput<?> streamOutput : oldStreamOutputCopies) {
					streamOutput.flush();
					streamOutput.close();
				}

				taskConfigManager.unregisterPartitions((ResultPartition[]) oldWriterCopies);
			}

			if (taskConfigManager.isUpdateWriters()) {
				//  close old output and flush its buffers
				recordWriters = createRecordWriters(configuration, getEnvironment());

				RecordWriterOutput[] oldStreamOutputCopies =
					operatorChain.substituteRecordWriter(this, recordWriters);

				//  close old output and unregister partitions
				for (RecordWriterOutput<?> streamOutput : oldStreamOutputCopies) {
					streamOutput.flush();
					streamOutput.close();
				}
			}

//			if (taskConfigManager.isUpdatingKeyGroupRange()) {
//				LOG.info("++++++ update task keyGroupRange for subtask");
//				this.updateKeyGroupRange(taskConfigManager.getKeyGroupRange());
//			}

			reconnect();
		} catch (Exception e) {
			LOG.info("++++++ error", e);
		} finally {
			// only unaffected tasks will finish their connection update, affected tasks will finish until its state has been updated.
			if (!taskConfigManager.isSourceOrDestination()) {
				taskConfigManager.finish();
			}
		}
	}

	private void checkReconfigPoint(
			CheckpointMetaData checkpointMetaData,
			CheckpointOptions checkpointOptions,
			CheckpointMetrics checkpointMetrics) {

		if (checkpointOptions.getCheckpointType() != RECONFIGPOINT) {
			return;
		}

		// force append latest status into metrics queue.
		getMetricsManager().updateMetrics();

		initReconnect();
	}

	@Override
	public void reinitializeState(KeyGroupRange keyGroupRange, int idInModel) {
		LOG.info("++++++ let's reinitialize state: " + this + "  " + keyGroupRange + "  idInModel: " + idInModel);
		try {
			synchronized (lock) {
				this.assignedKeyGroupRange.update(keyGroupRange);
				this.idInModel = idInModel;

				TaskConfigManager taskConfigManager = ((RuntimeEnvironment) getEnvironment()).getTaskConfigManager();


				if (taskConfigManager.getDstAffectedKeyGroups() != null) {
					LOG.info("++++++ " + this + " Migrated in: " + taskConfigManager.getDstAffectedKeyGroups());
					updateState(this.assignedKeyGroupRange, getEnvironment().getTaskInfo().getMaxNumberOfParallelSubtasks(), taskConfigManager.getDstAffectedKeyGroups());
				}

				getEnvironment().getMetricsManager().updateTaskId(
					getEnvironment().getTaskInfo().getTaskNameWithSubtasks(), idInModel);

				resume(); // resume the buffered processing
			}
		} catch (Exception e) {
			LOG.info("++++++ error", e);
			throw new RuntimeException(e);
		} finally {
			TaskConfigManager taskConfigManager = ((RuntimeEnvironment) getEnvironment()).getTaskConfigManager();
			Preconditions.checkState(taskConfigManager.isSourceOrDestination(), "++++++ Cannot reinitialize state for an unaffected task.");
			taskConfigManager.finish();
		}
	}

	private void updateState(KeyGroupRange keyGroupRange, int maxNumberOfParallelSubtasks, Collection<Integer> migrateInKeygroup) throws Exception {

		StreamOperator<?>[] allOperators = operatorChain.getAllOperators();

		for (StreamOperator<?> operator : allOperators) {
			if (null != operator) {
				operator.updateStateTable(keyGroupRange, maxNumberOfParallelSubtasks, migrateInKeygroup);
			}
		}
	}

	@Override
	public void updateKeyGroupRange(KeyGroupRange keyGroupRange) {
		LOG.info("++++++ updateKeyGroupRange: "  + this + "  " + keyGroupRange);

		TaskConfigManager taskConfigManager = ((RuntimeEnvironment) getEnvironment()).getTaskConfigManager();

//		synchronized (lock) {
//			this.assignedKeyGroupRange.update(keyGroupRange);
//		}
		this.assignedKeyGroupRange.update(keyGroupRange);
		taskConfigManager.finish();
	}

	// ------------------------------------------------------------------------
	//  Utilities
	// ------------------------------------------------------------------------

	@Override
	public String toString() {
		return getName();
	}

	// ------------------------------------------------------------------------

	/**
	 * This runnable executes the asynchronous parts of all involved backend snapshots for the subtask.
	 */
	@VisibleForTesting
	protected static final class AsyncCheckpointRunnable implements Runnable, Closeable {

		private final StreamTask<?, ?> owner;

		private final Map<OperatorID, OperatorSnapshotFutures> operatorSnapshotsInProgress;

		private final CheckpointMetaData checkpointMetaData;
		private final CheckpointMetrics checkpointMetrics;

		private final long asyncStartNanos;

		private final boolean isReconfigPoint;

		private final AtomicReference<CheckpointingOperation.AsyncCheckpointState> asyncCheckpointState = new AtomicReference<>(
			CheckpointingOperation.AsyncCheckpointState.RUNNING);

		AsyncCheckpointRunnable(
			StreamTask<?, ?> owner,
			Map<OperatorID, OperatorSnapshotFutures> operatorSnapshotsInProgress,
			CheckpointMetaData checkpointMetaData,
			CheckpointMetrics checkpointMetrics,
			long asyncStartNanos) {

			this.owner = Preconditions.checkNotNull(owner);
			this.operatorSnapshotsInProgress = Preconditions.checkNotNull(operatorSnapshotsInProgress);
			this.checkpointMetaData = Preconditions.checkNotNull(checkpointMetaData);
			this.checkpointMetrics = Preconditions.checkNotNull(checkpointMetrics);
			this.asyncStartNanos = asyncStartNanos;
			this.isReconfigPoint = false;
		}

		AsyncCheckpointRunnable(
			StreamTask<?, ?> owner,
			Map<OperatorID, OperatorSnapshotFutures> operatorSnapshotsInProgress,
			CheckpointMetaData checkpointMetaData,
			CheckpointMetrics checkpointMetrics,
			long asyncStartNanos,
			boolean isReconfigPoint) {

			this.owner = Preconditions.checkNotNull(owner);
			this.operatorSnapshotsInProgress = Preconditions.checkNotNull(operatorSnapshotsInProgress);
			this.checkpointMetaData = Preconditions.checkNotNull(checkpointMetaData);
			this.checkpointMetrics = Preconditions.checkNotNull(checkpointMetrics);
			this.asyncStartNanos = asyncStartNanos;
			this.isReconfigPoint = isReconfigPoint;
		}

		@Override
		public void run() {
			FileSystemSafetyNet.initializeSafetyNetForThread();
			try {

				LOG.debug("++++--- Start construct state handle for snapshot");

				TaskStateSnapshot jobManagerTaskOperatorSubtaskStates =
					new TaskStateSnapshot(operatorSnapshotsInProgress.size());

				TaskStateSnapshot localTaskOperatorSubtaskStates =
					new TaskStateSnapshot(operatorSnapshotsInProgress.size());

				for (Map.Entry<OperatorID, OperatorSnapshotFutures> entry : operatorSnapshotsInProgress.entrySet()) {

					OperatorID operatorID = entry.getKey();
					OperatorSnapshotFutures snapshotInProgress = entry.getValue();

					// finalize the async part of all by executing all snapshot runnables
					OperatorSnapshotFinalizer finalizedSnapshots =
						new OperatorSnapshotFinalizer(snapshotInProgress);

					OperatorSubtaskState operatorSubtaskState = finalizedSnapshots.getJobManagerOwnedState();
					LOG.info("++++--- State size: " + operatorSubtaskState.getStateSize());
					for (KeyedStateHandle keyedStateHandle : operatorSubtaskState.getManagedKeyedState()) {
						if (keyedStateHandle != null) {
							if (keyedStateHandle instanceof KeyGroupsStateHandle) {
								KeyGroupsStateHandle keyGroupsStateHandle = (KeyGroupsStateHandle) keyedStateHandle;
								// Step 1: Separate the Combined StateHandle into per KeyGroup KeyedStateHandle, i.e., Map<Integer, KeyedStateHandle>
								// Step 2: Reporting an empty taskLocalSnapshot to JobManager.
								Map<Integer, Tuple2<Long, StreamStateHandle>> hashedKeyGroupToHandle = composeSnapshotToJM(keyGroupsStateHandle);
								if (!isReconfigPoint) {
									// Step 3.1: Sends Map<Integer, KeyedStateHandle> to remote replicaStateManagers.
									replicateStateHandleToReplicas(hashedKeyGroupToHandle);
								} else {
									// Step 3.2: If the stream task is source task to send out state, it sends Map<Integer, KeyedStateHandle> to the remote destination tasks.
									TaskConfigManager taskConfigManager = ((RuntimeEnvironment) owner.getEnvironment()).getTaskConfigManager();
									if (taskConfigManager.isReconfigTarget() && taskConfigManager.isSource()) {
										transferStateHandleToDstTasks(hashedKeyGroupToHandle);
									}
								}
							}
						}
					}

					jobManagerTaskOperatorSubtaskStates.putSubtaskStateByOperatorID(
						operatorID,
						finalizedSnapshots.getJobManagerOwnedState());

					localTaskOperatorSubtaskStates.putSubtaskStateByOperatorID(
						operatorID,
						finalizedSnapshots.getTaskLocalState());
				}

				LOG.debug("++++--- Complete the construction, start to report");


				final long asyncEndNanos = System.nanoTime();
				final long asyncDurationMillis = (asyncEndNanos - asyncStartNanos) / 1_000_000L;

				checkpointMetrics.setAsyncDurationMillis(asyncDurationMillis);

				if (asyncCheckpointState.compareAndSet(CheckpointingOperation.AsyncCheckpointState.RUNNING,
					CheckpointingOperation.AsyncCheckpointState.COMPLETED)) {

					reportCompletedSnapshotStates(
						jobManagerTaskOperatorSubtaskStates,
						localTaskOperatorSubtaskStates,
						asyncDurationMillis);

				} else {
					LOG.debug("{} - asynchronous part of checkpoint {} could not be completed because it was closed before.",
						owner.getName(),
						checkpointMetaData.getCheckpointId());
				}
			} catch (Exception e) {
				handleExecutionException(e);
			} finally {
				owner.cancelables.unregisterCloseable(this);
				FileSystemSafetyNet.closeSafetyNetAndGuardedResourcesForThread();
			}
		}

		private void transferStateHandleToDstTasks(Map<Integer, Tuple2<Long, StreamStateHandle>> hashedKeyGroupToHandle) throws ExecutionException, InterruptedException {
			long start = System.currentTimeMillis();
			Map<Integer, TaskExecutorGateway> srcKeyGroupsWithDstGateway = ((RuntimeEnvironment) owner.getEnvironment()).getTaskConfigManager().getSrcKeyGroupsWithDstGateway();
			// Prepare futures for transferring each individual state handle
			List<CompletableFuture<Acknowledge>> dispatchStateToStandbyTaskFutures = new ArrayList<>();

			// Transfer each entry of the hashedKeyGroupToHandle individually
			for (Map.Entry<Integer, Tuple2<Long, StreamStateHandle>> entry : hashedKeyGroupToHandle.entrySet()) {
				int keyGroup = entry.getKey();
				Tuple2<Long, StreamStateHandle> stateHandle = entry.getValue();

				TaskExecutorGateway dstGateway = srcKeyGroupsWithDstGateway.get(keyGroup);
				if (dstGateway != null) {
					CompletableFuture<Acknowledge> future = dstGateway.dispatchStateToStandbyTask(
						owner.getEnvironment().getJobVertexId(),
						Collections.singletonMap(keyGroup, stateHandle));
					dispatchStateToStandbyTaskFutures.add(future);
				}
			}


			// Log the completion and time taken for the transfer
			FutureUtils.combineAll(dispatchStateToStandbyTaskFutures)
				.whenComplete((ignored, failure) -> {
				if (failure != null) {
					throw new CompletionException(failure);
				}
				LOG.info("++++--- Complete transfer: " + (System.currentTimeMillis() - start));
			}).get(); // Block until all transfers are completed
		}

		private void replicateStateHandleToReplicas(Map<Integer, Tuple2<Long, StreamStateHandle>> hashedKeyGroupToHandle) {
			if (!hashedKeyGroupToHandle.isEmpty()) {
				long start = System.currentTimeMillis();
				TaskStateManager taskStateManager = owner.getEnvironment().getTaskStateManager();
				List<TaskExecutorGateway> standbyTaskExecutorGateways = taskStateManager.getStandbyTaskExecutorGateways();

				// Prepare the list of futures for dispatching state
				List<CompletableFuture<Acknowledge>> dispatchStateToStandbyTaskFutures = new ArrayList<>();

				// Transfer each entry of the hashedKeyGroupToHandle individually to each standby task executor gateway
				for (Map.Entry<Integer, Tuple2<Long, StreamStateHandle>> entry : hashedKeyGroupToHandle.entrySet()) {
					int keyGroup = entry.getKey();
					Tuple2<Long, StreamStateHandle> stateHandle = entry.getValue();

					standbyTaskExecutorGateways.forEach(gateway -> {
						CompletableFuture<Acknowledge> future = gateway.dispatchStateToStandbyTask(
							owner.getEnvironment().getJobVertexId(),
							Collections.singletonMap(keyGroup, stateHandle)
						);
						dispatchStateToStandbyTaskFutures.add(future);
					});
				}

				// Combine all futures and handle completion
				FutureUtils.combineAll(dispatchStateToStandbyTaskFutures)
					.whenComplete((ignored, failure) -> {
						if (failure != null) {
							LOG.error("Failed to replicate snapshot to standby tasks", failure);
							throw new CompletionException(failure);
						}
						LOG.info("++++--- Complete replication: " + (System.currentTimeMillis() - start));
					}).join(); // Block until all transfers are completed
			}
		}

		private void reportCompletedSnapshotStates(
			TaskStateSnapshot acknowledgedTaskStateSnapshot,
			TaskStateSnapshot localTaskStateSnapshot,
			long asyncDurationMillis) {

			TaskStateManager taskStateManager = owner.getEnvironment().getTaskStateManager();

			boolean hasAckState = acknowledgedTaskStateSnapshot.hasState();
			boolean hasLocalState = localTaskStateSnapshot.hasState();

			LOG.info("++++++ {} - reported the following states in snapshot for checkpoint {}: {}, {}.",
				owner.getName(), checkpointMetaData.getCheckpointId(), acknowledgedTaskStateSnapshot, checkpointMetrics);

			Preconditions.checkState(hasAckState || !hasLocalState,
				"Found cached state but no corresponding primary state is reported to the job " +
					"manager. This indicates a problem.");

			// we signal stateless tasks by reporting null, so that there are no attempts to assign empty state
			// to stateless tasks on restore. This enables simple job modifications that only concern
			// stateless without the need to assign them uids to match their (always empty) states.
			taskStateManager.reportTaskStateSnapshots(
				checkpointMetaData,
				checkpointMetrics,
				hasAckState ? acknowledgedTaskStateSnapshot : null,
				hasLocalState ? localTaskStateSnapshot : null);

			LOG.debug("{} - finished asynchronous part of checkpoint {}. Asynchronous duration: {} ms",
				owner.getName(), checkpointMetaData.getCheckpointId(), asyncDurationMillis);

			LOG.trace("{} - reported the following states in snapshot for checkpoint {}: {}.",
				owner.getName(), checkpointMetaData.getCheckpointId(), acknowledgedTaskStateSnapshot);
		}

		/**
		 * propose an empty state handle to JM and also prepare per KeyGroup KeyedStateHandle to be sent to remote destinations
		 */
		static Map<Integer, Tuple2<Long, StreamStateHandle>> composeSnapshotToJM(KeyGroupsStateHandle keyGroupsStateHandle) {
			Map<Integer, Tuple2<Long, StreamStateHandle>> hashedKeyGroupToHandle = new HashMap<>();

			KeyGroupRangeOffsets keyGroupRangeOffsets = keyGroupsStateHandle.getGroupRangeOffsets();
			StreamStateHandle jobManagerOwnedSnapshot = keyGroupsStateHandle.getDelegateStateHandle();
			// compose an empty state snapshot that can be sent to JobManager
			StreamStateHandle jobManagerOwnedSnapshotToJM;
			KeyGroupRangeOffsets keyGroupRangeOffsetsToJM;

			// Step 1: Separate the Combined StateHandle into per KeyGroup KeyedStateHandle, i.e., Map<Integer, KeyedStateHandle>
			if (jobManagerOwnedSnapshot instanceof ByteStreamStateHandle) {

				ByteStreamStateHandle bytedJobManagerOwnedSnapshot = (ByteStreamStateHandle) jobManagerOwnedSnapshot;
				try {
					FSDataInputStream fsDataInputStream = bytedJobManagerOwnedSnapshot.openInputStream();
					DataInputViewStreamWrapper inView = new DataInputViewStreamWrapper(fsDataInputStream);
					KeyGroupRange keyGroupRange = keyGroupRangeOffsets.getKeyGroupRange();
					if (keyGroupRange.equals(KeyGroupRange.EMPTY_KEY_GROUP_RANGE)) {
						// Do nothing because the state is empty
						jobManagerOwnedSnapshotToJM = bytedJobManagerOwnedSnapshot;
						keyGroupRangeOffsetsToJM = keyGroupRangeOffsets;
					} else {
						// update original KeyGroupsStateHandle

						int start = keyGroupRange.getStartKeyGroup();
						int end = keyGroupRange.getEndKeyGroup();

						// for byte stream state handle usage, to reduce the state size to be migrated over Akka rpc
						long startOffset = keyGroupRangeOffsets.getKeyGroupOffset(start);

						// compose an empty state snapshot that can be sent to JobManager
						jobManagerOwnedSnapshotToJM =
							bytedJobManagerOwnedSnapshot.copyOfRange(startOffset, 0, 0);
						final long[] offsets = new long[keyGroupRange.getNumberOfKeyGroups()];
						Arrays.fill(offsets, startOffset);
						keyGroupRangeOffsetsToJM = new KeyGroupRangeOffsets(keyGroupRangeOffsets.getKeyGroupRange(), offsets, keyGroupRangeOffsets.getChangelogs());

						for (int alignedOldKeyGroup = start; alignedOldKeyGroup <= end; alignedOldKeyGroup++) {
							long offset = keyGroupRangeOffsets.getKeyGroupOffset(alignedOldKeyGroup);
							boolean isModified = keyGroupRangeOffsets.getIsModified(alignedOldKeyGroup);

							int nextAlignedOldKeyGroup = alignedOldKeyGroup + 1;

							long nextOffset = nextAlignedOldKeyGroup <= end ?
								keyGroupRangeOffsets.getKeyGroupOffset(nextAlignedOldKeyGroup)
								: bytedJobManagerOwnedSnapshot.getStateSize();

							// skip if the keygroup does not have any state snapshot
							if (nextOffset == offset) continue;

							fsDataInputStream.seek(offset);
							int hashedKeyGroup = inView.readInt();

							StreamStateHandle newStateHandlePerKeyGroup =
								bytedJobManagerOwnedSnapshot.copyOfRange(startOffset, offset, nextOffset);

							hashedKeyGroupToHandle.put(
								hashedKeyGroup,
								Tuple2.of(startOffset, newStateHandlePerKeyGroup));
						}
					}
				} catch (IOException e) {
					throw new RuntimeException(e);
				}
			} else {
				jobManagerOwnedSnapshotToJM = jobManagerOwnedSnapshot;
				keyGroupRangeOffsetsToJM = keyGroupRangeOffsets;
			}

			keyGroupsStateHandle.setDelegateStateHandle(jobManagerOwnedSnapshotToJM);
			keyGroupsStateHandle.setGroupRangeOffsets(keyGroupRangeOffsetsToJM);
			return hashedKeyGroupToHandle;
		}

		private void handleExecutionException(Exception e) {

			boolean didCleanup = false;
			CheckpointingOperation.AsyncCheckpointState currentState = asyncCheckpointState.get();

			while (CheckpointingOperation.AsyncCheckpointState.DISCARDED != currentState) {

				if (asyncCheckpointState.compareAndSet(
					currentState,
					CheckpointingOperation.AsyncCheckpointState.DISCARDED)) {

					didCleanup = true;

					try {
						cleanup();
					} catch (Exception cleanupException) {
						e.addSuppressed(cleanupException);
					}

					Exception checkpointException = new Exception(
						"Could not materialize checkpoint " + checkpointMetaData.getCheckpointId() + " for operator " +
							owner.getName() + '.',
						e);

					// We only report the exception for the original cause of fail and cleanup.
					// Otherwise this followup exception could race the original exception in failing the task.
					owner.asynchronousCheckpointExceptionHandler.tryHandleCheckpointException(
						checkpointMetaData,
						checkpointException);

					currentState = CheckpointingOperation.AsyncCheckpointState.DISCARDED;
				} else {
					currentState = asyncCheckpointState.get();
				}
			}

			if (!didCleanup) {
				LOG.trace("Caught followup exception from a failed checkpoint thread. This can be ignored.", e);
			}
		}

		@Override
		public void close() {
			if (asyncCheckpointState.compareAndSet(
				CheckpointingOperation.AsyncCheckpointState.RUNNING,
				CheckpointingOperation.AsyncCheckpointState.DISCARDED)) {

				try {
					cleanup();
				} catch (Exception cleanupException) {
					LOG.warn("Could not properly clean up the async checkpoint runnable.", cleanupException);
				}
			} else {
				logFailedCleanupAttempt();
			}
		}

		private void cleanup() throws Exception {
			LOG.debug(
				"Cleanup AsyncCheckpointRunnable for checkpoint {} of {}.",
				checkpointMetaData.getCheckpointId(),
				owner.getName());

			Exception exception = null;

			// clean up ongoing operator snapshot results and non partitioned state handles
			for (OperatorSnapshotFutures operatorSnapshotResult : operatorSnapshotsInProgress.values()) {
				if (operatorSnapshotResult != null) {
					try {
						operatorSnapshotResult.cancel();
					} catch (Exception cancelException) {
						exception = ExceptionUtils.firstOrSuppressed(cancelException, exception);
					}
				}
			}

			if (null != exception) {
				throw exception;
			}
		}

		private void logFailedCleanupAttempt() {
			LOG.debug("{} - asynchronous checkpointing operation for checkpoint {} has " +
					"already been completed. Thus, the state handles are not cleaned up.",
				owner.getName(),
				checkpointMetaData.getCheckpointId());
		}
	}

	public CloseableRegistry getCancelables() {
		return cancelables;
	}

	// ------------------------------------------------------------------------

	private static final class SnapshotAffectedStateOperation {

		private final StreamTask<?, ?> owner;

		private final CheckpointMetaData checkpointMetaData;
		private final CheckpointOptions checkpointOptions;
		private final CheckpointMetrics checkpointMetrics;
		private final CheckpointStreamFactory storageLocation;

		private final StreamOperator<?>[] allOperators;

		private long startSyncPartNano;
		private long startAsyncPartNano;

		private final Collection<Integer> affectedKeyGroups;

		private final Set<Integer> backupKeyGroups;

		// ------------------------

		private final Map<OperatorID, OperatorSnapshotFutures> operatorSnapshotsInProgress;

		public SnapshotAffectedStateOperation(
			StreamTask<?, ?> owner,
			CheckpointMetaData checkpointMetaData,
			CheckpointOptions checkpointOptions,
			CheckpointStreamFactory checkpointStorageLocation,
			CheckpointMetrics checkpointMetrics,
			Collection<Integer> affectedKeyGroups,
			Set<Integer> backupKeyGroups) {

			this.owner = Preconditions.checkNotNull(owner);
			this.checkpointMetaData = Preconditions.checkNotNull(checkpointMetaData);
			this.checkpointOptions = Preconditions.checkNotNull(checkpointOptions);
			this.checkpointMetrics = Preconditions.checkNotNull(checkpointMetrics);
			this.storageLocation = Preconditions.checkNotNull(checkpointStorageLocation);
			this.allOperators = owner.operatorChain.getAllOperators();
			this.operatorSnapshotsInProgress = new HashMap<>(allOperators.length);
			Preconditions.checkState(affectedKeyGroups != null);
			this.affectedKeyGroups = affectedKeyGroups;
			this.backupKeyGroups = backupKeyGroups;
		}

		public void executeAffectedStateSnapshot() throws Exception {
			startSyncPartNano = System.nanoTime();

			try {
				// TODO: only the affected operators do snapshot, by far, we temporarily use the same logic as before
				for (StreamOperator<?> op : allOperators) {
					checkpointStreamOperatorWithAffectedState(op);
				}

				if (LOG.isDebugEnabled()) {
					LOG.debug("Finished synchronous checkpoints for checkpoint {} on task {}",
						checkpointMetaData.getCheckpointId(), owner.getName());
				}

				startAsyncPartNano = System.nanoTime();

				checkpointMetrics.setSyncDurationMillis((startAsyncPartNano - startSyncPartNano) / 1_000_000);

				// we are transferring ownership over snapshotInProgressList for cleanup to the thread, active on submit
				AsyncCheckpointRunnable asyncCheckpointRunnable = new AsyncCheckpointRunnable(
					owner,
					operatorSnapshotsInProgress,
					checkpointMetaData,
					checkpointMetrics,
					startAsyncPartNano,
					true);

				owner.cancelables.registerCloseable(asyncCheckpointRunnable);
				owner.asyncOperationsThreadPool.execute(asyncCheckpointRunnable);

				if (LOG.isDebugEnabled()) {
					LOG.debug("{} - finished synchronous part of checkpoint {}. " +
							"Alignment duration: {} ms, snapshot duration {} ms",
						owner.getName(), checkpointMetaData.getCheckpointId(),
						checkpointMetrics.getAlignmentDurationNanos() / 1_000_000,
						checkpointMetrics.getSyncDurationMillis());
				}
			} catch (Exception ex) {
				// Cleanup to release resources
				for (OperatorSnapshotFutures operatorSnapshotResult : operatorSnapshotsInProgress.values()) {
					if (null != operatorSnapshotResult) {
						try {
							operatorSnapshotResult.cancel();
						} catch (Exception e) {
							LOG.warn("Could not properly cancel an operator snapshot result.", e);
						}
					}
				}

				if (LOG.isDebugEnabled()) {
					LOG.debug("{} - did NOT finish synchronous part of checkpoint {}. " +
							"Alignment duration: {} ms, snapshot duration {} ms",
						owner.getName(), checkpointMetaData.getCheckpointId(),
						checkpointMetrics.getAlignmentDurationNanos() / 1_000_000,
						checkpointMetrics.getSyncDurationMillis());
				}

				owner.synchronousCheckpointExceptionHandler.tryHandleCheckpointException(checkpointMetaData, ex);
			}
		}

		@SuppressWarnings("deprecation")
		private void checkpointStreamOperatorWithAffectedState(StreamOperator<?> op) throws Exception {
			if (null != op) {
				// only snapshot on the affected keygroup set, we can simply hard code it to snapshot on the first keygroup to test the effectiveness
				OperatorSnapshotFutures snapshotInProgress = op.snapshotAffectedState(
					checkpointMetaData.getCheckpointId(),
					checkpointMetaData.getTimestamp(),
					checkpointOptions,
					storageLocation,
					affectedKeyGroups,
					backupKeyGroups);
				operatorSnapshotsInProgress.put(op.getOperatorID(), snapshotInProgress);
			}
		}

		private enum AsyncCheckpointState {
			RUNNING,
			DISCARDED,
			COMPLETED
		}
	}

	private static final class CheckpointingOperation {

		private final StreamTask<?, ?> owner;

		private final CheckpointMetaData checkpointMetaData;
		private final CheckpointOptions checkpointOptions;
		private final CheckpointMetrics checkpointMetrics;
		private final CheckpointStreamFactory storageLocation;

		private final StreamOperator<?>[] allOperators;
		private final boolean isChangelogEnabled;

		private final Set<Integer> backupKeyGroups;

		private long startSyncPartNano;
		private long startAsyncPartNano;

		// ------------------------

		private final Map<OperatorID, OperatorSnapshotFutures> operatorSnapshotsInProgress;

		public CheckpointingOperation(
			StreamTask<?, ?> owner,
			CheckpointMetaData checkpointMetaData,
			CheckpointOptions checkpointOptions,
			CheckpointStreamFactory checkpointStorageLocation,
			CheckpointMetrics checkpointMetrics,
			boolean isChangelogEnabled,
			Set<Integer> backupKeyGroups) {

			this.owner = Preconditions.checkNotNull(owner);
			this.checkpointMetaData = Preconditions.checkNotNull(checkpointMetaData);
			this.checkpointOptions = Preconditions.checkNotNull(checkpointOptions);
			this.checkpointMetrics = Preconditions.checkNotNull(checkpointMetrics);
			this.storageLocation = Preconditions.checkNotNull(checkpointStorageLocation);
			this.allOperators = owner.operatorChain.getAllOperators();
			this.operatorSnapshotsInProgress = new HashMap<>(allOperators.length);
			this.isChangelogEnabled = isChangelogEnabled;
			this.backupKeyGroups = backupKeyGroups;
		}

		public void executeCheckpointing() throws Exception {
			startSyncPartNano = System.nanoTime();

			try {
				for (StreamOperator<?> op : allOperators) {
					checkpointStreamOperator(op);
				}

				if (LOG.isDebugEnabled()) {
					LOG.debug("Finished synchronous checkpoints for checkpoint {} on task {}",
						checkpointMetaData.getCheckpointId(), owner.getName());
				}

				startAsyncPartNano = System.nanoTime();

				checkpointMetrics.setSyncDurationMillis((startAsyncPartNano - startSyncPartNano) / 1_000_000);

				// we are transferring ownership over snapshotInProgressList for cleanup to the thread, active on submit
				AsyncCheckpointRunnable asyncCheckpointRunnable = new AsyncCheckpointRunnable(
					owner,
					operatorSnapshotsInProgress,
					checkpointMetaData,
					checkpointMetrics,
					startAsyncPartNano,
					false);

				owner.cancelables.registerCloseable(asyncCheckpointRunnable);
				owner.asyncOperationsThreadPool.submit(asyncCheckpointRunnable);

				if (LOG.isDebugEnabled()) {
					LOG.debug("{} - finished synchronous part of checkpoint {}. " +
							"Alignment duration: {} ms, snapshot duration {} ms",
						owner.getName(), checkpointMetaData.getCheckpointId(),
						checkpointMetrics.getAlignmentDurationNanos() / 1_000_000,
						checkpointMetrics.getSyncDurationMillis());
				}
			} catch (Exception ex) {
				// Cleanup to release resources
				for (OperatorSnapshotFutures operatorSnapshotResult : operatorSnapshotsInProgress.values()) {
					if (null != operatorSnapshotResult) {
						try {
							operatorSnapshotResult.cancel();
						} catch (Exception e) {
							LOG.warn("Could not properly cancel an operator snapshot result.", e);
						}
					}
				}

				if (LOG.isDebugEnabled()) {
					LOG.debug("{} - did NOT finish synchronous part of checkpoint {}. " +
							"Alignment duration: {} ms, snapshot duration {} ms",
						owner.getName(), checkpointMetaData.getCheckpointId(),
						checkpointMetrics.getAlignmentDurationNanos() / 1_000_000,
						checkpointMetrics.getSyncDurationMillis());
				}

				owner.synchronousCheckpointExceptionHandler.tryHandleCheckpointException(checkpointMetaData, ex);
			}
		}

		@SuppressWarnings("deprecation")
		private void checkpointStreamOperator(StreamOperator<?> op) throws Exception {
			if (null != op) {

				OperatorSnapshotFutures snapshotInProgress = op.snapshotState(
						checkpointMetaData.getCheckpointId(),
						checkpointMetaData.getTimestamp(),
						checkpointOptions,
						storageLocation,
						isChangelogEnabled,
						backupKeyGroups);
				operatorSnapshotsInProgress.put(op.getOperatorID(), snapshotInProgress);
			}
		}

		private enum AsyncCheckpointState {
			RUNNING,
			DISCARDED,
			COMPLETED
		}
	}

	/**
	 * Wrapper for synchronous {@link CheckpointExceptionHandler}. This implementation catches unhandled, rethrown
	 * exceptions and reports them through {@link #handleAsyncException(String, Throwable)}. As this implementation
	 * always handles the exception in some way, it never rethrows.
	 */
	static final class AsyncCheckpointExceptionHandler implements CheckpointExceptionHandler {

		/** Owning stream task to which we report async exceptions. */
		final StreamTask<?, ?> owner;

		/** Synchronous exception handler to which we delegate. */
		final CheckpointExceptionHandler synchronousCheckpointExceptionHandler;

		AsyncCheckpointExceptionHandler(StreamTask<?, ?> owner) {
			this.owner = Preconditions.checkNotNull(owner);
			this.synchronousCheckpointExceptionHandler =
				Preconditions.checkNotNull(owner.synchronousCheckpointExceptionHandler);
		}

		@Override
		public void tryHandleCheckpointException(CheckpointMetaData checkpointMetaData, Exception exception) {
			try {
				synchronousCheckpointExceptionHandler.tryHandleCheckpointException(checkpointMetaData, exception);
			} catch (Exception unhandled) {
				AsynchronousException asyncException = new AsynchronousException(unhandled);
				owner.handleAsyncException("Failure in asynchronous checkpoint materialization", asyncException);
			}
		}
	}

	@VisibleForTesting
	public static <OUT> List<RecordWriter<SerializationDelegate<StreamRecord<OUT>>>> createRecordWriters(
			StreamConfig configuration,
			Environment environment) {
		List<RecordWriter<SerializationDelegate<StreamRecord<OUT>>>> recordWriters = new ArrayList<>();
		List<StreamEdge> outEdgesInOrder = configuration.getOutEdgesInOrder(environment.getUserClassLoader());
		Map<Integer, StreamConfig> chainedConfigs = configuration.getTransitiveChainedTaskConfigsWithSelf(environment.getUserClassLoader());

		for (int i = 0; i < outEdgesInOrder.size(); i++) {
			StreamEdge edge = outEdgesInOrder.get(i);
			recordWriters.add(
				createRecordWriter(
					edge,
					i,
					environment,
					environment.getTaskInfo().getTaskName(),
					chainedConfigs.get(edge.getSourceId()).getBufferTimeout()));
		}
		return recordWriters;
	}

	private static <OUT> RecordWriter<SerializationDelegate<StreamRecord<OUT>>> createRecordWriter(
			StreamEdge edge,
			int outputIndex,
			Environment environment,
			String taskName,
			long bufferTimeout) {
		@SuppressWarnings("unchecked")
		StreamPartitioner<OUT> outputPartitioner = (StreamPartitioner<OUT>) edge.getPartitioner();

		LOG.debug("Using partitioner {} for output {} of task {}", outputPartitioner, outputIndex, taskName);

		ResultPartitionWriter bufferWriter = environment.getWriter(outputIndex);

		// we initialize the partitioner here with the number of key groups (aka max. parallelism)
		if (outputPartitioner instanceof ConfigurableStreamPartitioner) {
			int numKeyGroups = bufferWriter.getNumTargetKeyGroups();
			if (0 < numKeyGroups) {
				((ConfigurableStreamPartitioner) outputPartitioner).configure(numKeyGroups);
			}
		}

		RecordWriter<SerializationDelegate<StreamRecord<OUT>>> output =
			RecordWriter.createRecordWriter(bufferWriter, outputPartitioner, bufferTimeout, taskName);
		output.setMetricGroup(environment.getMetricGroup().getIOMetricGroup());
		return output;
	}
}
