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

package org.apache.flink.streaming.runtime.io;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.SimpleCounter;
import org.apache.flink.runtime.event.AbstractEvent;
import org.apache.flink.runtime.io.disk.iomanager.IOManager;
import org.apache.flink.runtime.io.network.api.EndOfPartitionEvent;
import org.apache.flink.runtime.io.network.api.serialization.RecordDeserializer;
import org.apache.flink.runtime.io.network.api.serialization.RecordDeserializer.DeserializationResult;
import org.apache.flink.runtime.io.network.api.serialization.SpillingAdaptiveSpanningRecordDeserializer;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.partition.consumer.BufferOrEvent;
import org.apache.flink.runtime.io.network.partition.consumer.InputGate;
import org.apache.flink.runtime.metrics.groups.OperatorMetricGroup;
import org.apache.flink.runtime.metrics.groups.TaskIOMetricGroup;
import org.apache.flink.runtime.plugable.DeserializationDelegate;
import org.apache.flink.runtime.plugable.NonReusingDeserializationDelegate;
import org.apache.flink.runtime.util.profiling.MetricsManager;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.metrics.WatermarkGauge;
import org.apache.flink.streaming.runtime.streamrecord.StreamElement;
import org.apache.flink.streaming.runtime.streamrecord.StreamElementSerializer;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.streamstatus.StatusWatermarkValve;
import org.apache.flink.streaming.runtime.streamstatus.StreamStatus;
import org.apache.flink.streaming.runtime.streamstatus.StreamStatusMaintainer;
import org.apache.flink.streaming.runtime.tasks.StreamTask;
import org.apache.flink.util.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.ConcurrentSkipListSet;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Input reader for {@link org.apache.flink.streaming.runtime.tasks.OneInputStreamTask}.
 *
 * <p>This internally uses a {@link StatusWatermarkValve} to keep track of {@link Watermark} and
 * {@link StreamStatus} events, and forwards them to event subscribers once the
 * {@link StatusWatermarkValve} determines the {@link Watermark} from all inputs has advanced, or
 * that a {@link StreamStatus} needs to be propagated downstream to denote a status change.
 *
 * <p>Forwarding elements, watermarks, or status status elements must be protected by synchronizing
 * on the given lock object. This ensures that we don't call methods on a
 * {@link OneInputStreamOperator} concurrently with the timer callback or other things.
 *
 * @param <IN> The type of the record that can be read with this record reader.
 */
@Internal
public class StreamInputProcessor<IN> {

	private static final Logger LOG = LoggerFactory.getLogger(StreamInputProcessor.class);

	private volatile RecordDeserializer<DeserializationDelegate<StreamElement>>[] recordDeserializers;

	private RecordDeserializer<DeserializationDelegate<StreamElement>> currentRecordDeserializer;

	private final DeserializationDelegate<StreamElement> deserializationDelegate;

	private final CheckpointBarrierHandler barrierHandler;

	private final InputGate inputGate;

	private final IOManager ioManager;

	private final Object lock;

	// ---------------- Status and Watermark Valve ------------------

	/** Valve that controls how watermarks and stream statuses are forwarded. */
	private StatusWatermarkValve statusWatermarkValve;

	/** Number of input channels the valve needs to handle. */
	private volatile int numInputChannels;

	/**
	 * The channel from which a buffer came, tracked so that we can appropriately map
	 * the watermarks and watermark statuses to channel indexes of the valve.
	 */
	private int currentChannel = -1;

	private final StreamStatusMaintainer streamStatusMaintainer;

	private final OneInputStreamOperator<IN, ?> streamOperator;

	// ---------------- Metrics ------------------

	private final WatermarkGauge watermarkGauge;
	private Counter numRecordsIn;

	private boolean isFinished;

//	private final ArrayDeque<StreamRecord<IN>> bufferedRecord;
	private final Map<Integer, ArrayDeque<StreamRecord<IN>>> bufferedRecordsByKeys;

	/**
	 * Future for standby tasks that completes when they are required to run
	 */
	private final Set<Integer> migratingKeys;
	private final Deque<Integer> migratedKeys;
	private volatile boolean isUnderMigration;

	private MetricsManager metricsManager;
	private long deserializationDuration = 0;
	private long processingDuration = 0;
	private long recordsProcessed = 0;
	private long endToEndLatency = 0;

	@SuppressWarnings("unchecked")
	public StreamInputProcessor(
			InputGate[] inputGates,
			TypeSerializer<IN> inputSerializer,
			StreamTask<?, ?> checkpointedTask,
			CheckpointingMode checkpointMode,
			Object lock,
			IOManager ioManager,
			Configuration taskManagerConfig,
			StreamStatusMaintainer streamStatusMaintainer,
			OneInputStreamOperator<IN, ?> streamOperator,
			TaskIOMetricGroup metrics,
			WatermarkGauge watermarkGauge) throws IOException {

		this.inputGate = InputGateUtil.createInputGate(inputGates);
		this.ioManager = ioManager;

		this.barrierHandler = InputProcessorUtil.createCheckpointBarrierHandler(
			checkpointedTask, checkpointMode, ioManager, inputGate, taskManagerConfig);

		this.lock = checkNotNull(lock);

		StreamElementSerializer<IN> ser = new StreamElementSerializer<>(inputSerializer);
		this.deserializationDelegate = new NonReusingDeserializationDelegate<>(ser);

		// Initialize one deserializer per input channel
		this.recordDeserializers = new SpillingAdaptiveSpanningRecordDeserializer[inputGate.getNumberOfInputChannels()];

		for (int i = 0; i < recordDeserializers.length; i++) {
			recordDeserializers[i] = new SpillingAdaptiveSpanningRecordDeserializer<>(
				ioManager.getSpillingDirectoriesPaths());
		}

		this.numInputChannels = inputGate.getNumberOfInputChannels();

		this.streamStatusMaintainer = checkNotNull(streamStatusMaintainer);
		this.streamOperator = checkNotNull(streamOperator);

		this.statusWatermarkValve = new StatusWatermarkValve(
				numInputChannels,
				new ForwardingValveOutputHandler(streamOperator, lock));

		this.watermarkGauge = watermarkGauge;
		metrics.gauge("checkpointAlignmentTime", barrierHandler::getAlignmentDurationNanos);

		this.isUnderMigration = false;
//		this.bufferedRecord = new ArrayDeque<>();
		this.bufferedRecordsByKeys = new HashMap<>();
		this.migratingKeys = new HashSet<>();
		this.migratedKeys = new ConcurrentLinkedDeque<>();
	}

	public boolean processInput() throws Exception {
		if (isFinished) {
			return false;
		}
		if (numRecordsIn == null) {
			try {
				numRecordsIn = ((OperatorMetricGroup) streamOperator.getMetricGroup()).getIOMetricGroup().getNumRecordsInCounter();
			} catch (Exception e) {
				LOG.warn("An exception occurred during the metrics setup.", e);
				numRecordsIn = new SimpleCounter();
			}
		}

		while (true) {
			// check difference between migratingKeys and bufferedRecord to process those state that have been recovered
			if (!bufferedRecordsByKeys.isEmpty()) {
				if (isUnderMigration && !migratedKeys.isEmpty()) { // progressively continue
					LOG.info("++++++ Progressively consuming buffered records for keys: " + migratedKeys);
					// process buffered state when the migrating keys are empty
					Integer migratedKeygroup = migratedKeys.poll();
					while (migratedKeygroup != null) {
						ArrayDeque<StreamRecord<IN>> bufferedRecordForKey = bufferedRecordsByKeys.remove(migratedKeygroup);
						consumeBufferForKey(migratedKeygroup, bufferedRecordForKey);
						migratedKeygroup = migratedKeys.poll();
					}
				}
				if (!isUnderMigration) { // continue after all state migrated
					LOG.info("++++++ Consuming all buffered records for keys: " + bufferedRecordsByKeys.keySet());
					for (int migratedKeygroup : bufferedRecordsByKeys.keySet()) {
						ArrayDeque<StreamRecord<IN>> bufferedRecordForKey = bufferedRecordsByKeys.get(migratedKeygroup);
						consumeBufferForKey(migratedKeygroup, bufferedRecordForKey);
					}
					bufferedRecordsByKeys.clear();
				}
			}
			if (currentRecordDeserializer != null) {
				long start = System.nanoTime();

				DeserializationResult result = currentRecordDeserializer.getNextRecord(deserializationDelegate);

				deserializationDuration += System.nanoTime() - start;

				if (result.isBufferConsumed()) {
					currentRecordDeserializer.getCurrentBuffer().recycleBuffer();
					currentRecordDeserializer = null;
				}

				if (result.isFullRecord()) {
					StreamElement recordOrMark = deserializationDelegate.getInstance();

					if (recordOrMark.isWatermark()) {
						long processingStart = System.nanoTime();

						// handle watermark
						statusWatermarkValve.inputWatermark(recordOrMark.asWatermark(), currentChannel);

						processingDuration += System.nanoTime() - processingStart;
						recordsProcessed++;

						continue;
					} else if (recordOrMark.isStreamStatus()) {
						// handle stream status
						statusWatermarkValve.inputStreamStatus(recordOrMark.asStreamStatus(), currentChannel);
						continue;
					} else if (recordOrMark.isLatencyMarker()) {
						// handle latency marker
						synchronized (lock) {
							streamOperator.processLatencyMarker(recordOrMark.asLatencyMarker());
						}
						continue;
					} else {
						// now we can do the actual processing
						StreamRecord<IN> record = recordOrMark.asRecord();
						int keyGroup = record.getKeyGroup();
						if (isUnderMigration && migratingKeys.contains(keyGroup)) {
							// buffer the records for migrating keys
//							bufferedRecord.add(record);
							ArrayDeque<StreamRecord<IN>> bufferedRecordsForKey = bufferedRecordsByKeys.computeIfAbsent(keyGroup, t -> new ArrayDeque<>());
							bufferedRecordsForKey.add(record);
						} else {
							synchronized (lock) {
								processElement(record);
							}
						}
						return true;
					}
				}
			}

			// the buffer got empty
			if (deserializationDuration > 0) {
				// inform the MetricsManager that the buffer is consumed
				metricsManager.inputBufferConsumed(System.nanoTime(), deserializationDuration, processingDuration, recordsProcessed, endToEndLatency);

				deserializationDuration = 0;
				processingDuration = 0;
				recordsProcessed = 0;
				endToEndLatency = 0;
			}

			final BufferOrEvent bufferOrEvent = barrierHandler.getNextNonBlocked();
			if (bufferOrEvent != null) {
				if (bufferOrEvent.isBuffer()) {
					currentChannel = bufferOrEvent.getChannelIndex();
					currentRecordDeserializer = recordDeserializers[currentChannel];
					currentRecordDeserializer.setNextBuffer(bufferOrEvent.getBuffer());

					// inform the MetricsManager that we got a new input buffer
					metricsManager.newInputBuffer(System.nanoTime());
				}
				else {
					// Event received
					final AbstractEvent event = bufferOrEvent.getEvent();
					if (event.getClass() != EndOfPartitionEvent.class) {
						throw new IOException("Unexpected event: " + event);
					}
				}
			}
			else {
				isFinished = true;
				if (!barrierHandler.isEmpty()) {
					throw new IllegalStateException("Trailing data in checkpoint barrier handler.");
				}
				return false;
			}
		}
	}

	private void consumeBufferForKey(int migratedKeygroup, ArrayDeque<StreamRecord<IN>> bufferedRecordForKey) throws Exception {
		if (bufferedRecordForKey != null) {
			LOG.info("++++++ Consuming buffered records for migrated key: " + migratedKeygroup + " size: " + bufferedRecordForKey.size());
			StreamRecord<IN> record = bufferedRecordForKey.poll();
			while (record != null) {
				synchronized (lock) {
					processElement(record);
				}
				record = bufferedRecordForKey.poll();
			}
		}
	}

	private void processElement(StreamRecord<IN> record) throws Exception {
		numRecordsIn.inc();
		streamOperator.setKeyContextElement1(record);

//		if (streamOperator instanceof WindowOperator) {
//			long delayStart = System.nanoTime(); // if window, delay 0.1ms
//			while (System.nanoTime() - delayStart < 100_000) {}
//		}

		long arrvialTs = record.getLatencyTimestamp();
		long queuingDelay = System.currentTimeMillis() - arrvialTs;

		metricsManager.incRecordIn(record.getKeyGroup());

		long processingStart = System.nanoTime();
		streamOperator.processElement(record);
		long processingDelay = System.nanoTime() - processingStart;


		processingDuration += processingDelay;
		recordsProcessed++;

//		metricsManager.groundTruth(arrvialTs, queuingDelay + processingDelay / 1000_000);
		metricsManager.groundTruth(System.currentTimeMillis(), queuingDelay + processingDelay / 1000_000);
//		processingDuration = 0;
//		recordsProcessed = 0;
//		deserializationDuration = 0;
	}

	public void cleanup() throws IOException {
		// clear the buffers first. this part should not ever fail
		for (RecordDeserializer<?> deserializer : recordDeserializers) {
			Buffer buffer = deserializer.getCurrentBuffer();
			if (buffer != null && !buffer.isRecycled()) {
				buffer.recycleBuffer();
			}
			deserializer.clear();
		}

		// cleanup the barrier handler resources
		barrierHandler.cleanup();
	}

	@SuppressWarnings("unchecked")
	public void reconnect() {
		numInputChannels = inputGate.getNumberOfInputChannels();

//		System.out.println(this.metricsManager.getJobVertexId() + ": " + numInputChannels);

		RecordDeserializer<DeserializationDelegate<StreamElement>>[] oldDeserializer =
				Arrays.copyOf(recordDeserializers, recordDeserializers.length);
		recordDeserializers = new SpillingAdaptiveSpanningRecordDeserializer[numInputChannels];

		for (int i = 0; i < recordDeserializers.length; i++) {
			if (i < oldDeserializer.length) {
				recordDeserializers[i] = oldDeserializer[i];
			} else {
				recordDeserializers[i] = new SpillingAdaptiveSpanningRecordDeserializer<>(
					ioManager.getSpillingDirectoriesPaths());
			}
		}

		statusWatermarkValve.rescale(numInputChannels);
		barrierHandler.updateTotalNumberOfInputChannels(numInputChannels);
	}

	public void setMigratingKeys(Collection<Integer> affectedKeygroups) {
		Preconditions.checkState(!isUnderMigration
				&& migratedKeys.isEmpty()
				&& migratingKeys.isEmpty()
				&& bufferedRecordsByKeys.isEmpty(),
			"++++++ The last state migration state is not cleared");
		LOG.info("++++++ Add affected keygroup: " + affectedKeygroups);
		isUnderMigration = true;
		migratingKeys.addAll(affectedKeygroups);
	}

	public void completeMigration() {
		LOG.info("++++++ Complete migrating all affected keygroups: " + migratingKeys);
		isUnderMigration = false;
		migratingKeys.clear();
		migratedKeys.clear();
	}

	public void completeMigrationForKey(int keygroup) {
		LOG.debug("++++++ Complete migrating affected keygroup: " + keygroup);
		migratingKeys.remove(keygroup);
		migratedKeys.add(keygroup);
	}

	private class ForwardingValveOutputHandler implements StatusWatermarkValve.ValveOutputHandler {
		private final OneInputStreamOperator<IN, ?> operator;
		private final Object lock;

		private ForwardingValveOutputHandler(final OneInputStreamOperator<IN, ?> operator, final Object lock) {
			this.operator = checkNotNull(operator);
			this.lock = checkNotNull(lock);
		}

		@Override
		public void handleWatermark(Watermark watermark) {
			try {
				synchronized (lock) {
					watermarkGauge.setCurrentWatermark(watermark.getTimestamp());
					operator.processWatermark(watermark);
				}
			} catch (Exception e) {
				throw new RuntimeException("Exception occurred while processing valve output watermark: ", e);
			}
		}

		@SuppressWarnings("unchecked")
		@Override
		public void handleStreamStatus(StreamStatus streamStatus) {
			try {
				synchronized (lock) {
					streamStatusMaintainer.toggleStreamStatus(streamStatus);
				}
			} catch (Exception e) {
				throw new RuntimeException("Exception occurred while processing valve output stream status: ", e);
			}
		}
	}

	public void setMetricsManager(MetricsManager metricsManager) {
		this.metricsManager = metricsManager;
	}
}
