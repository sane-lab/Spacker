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

package org.apache.flink.runtime.util.profiling;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.OutputStream;
import java.io.Serializable;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.flink.runtime.spector.SpectorOptions.*;

/**
 * The MetricsManager is responsible for logging activity profiling information (except for messages).
 * It gathers start and end events for deserialization, processing, serialization, blocking on read and write buffers
 * and records activity durations. There is one MetricsManager instance per Task (operator instance).
 * The MetricsManager aggregates metrics in a {@link ProcessingStatus} object and outputs processing and output rates
 * periodically to a designated rates file.
 */
public class FSMetricsManager implements Serializable, MetricsManager {
	private static final Logger LOG = LoggerFactory.getLogger(FSMetricsManager.class);

	private String taskId; // Flink's task description
	private String workerName; // The task description string logged in the rates file
	private int instanceId; // The operator instance id
	private int numInstances; // The total number of instances for this operator
	private final JobVertexID jobVertexId; // To interact with StreamSwitch

	private long recordsIn = 0;	// Total number of records ingested since the last flush
	private long recordsOut = 0;	// Total number of records produced since the last flush
	private long usefulTime = 0;	// Total period of useful time since last flush
	private long waitingTime = 0;	// Total waiting time for input/output buffers since last flush
	private long latency = 0;	// Total end to end latency

	private long totalRecordsIn = 0;	// Total number of records ingested since the last flush
	private long totalRecordsOut = 0;	// Total number of records produced since the last flush

	private long currentWindowStart;

	private final ProcessingStatus status;

	private final long windowSize;	// The aggregation interval
//	private final String ratesPath;	// The file path where to output aggregated rates

	private long epoch = 0;	// The current aggregation interval. The MetricsManager outputs one rates file per epoch.

	// keygroup -> avgLatency
	// keygroup -> nRecords

	private final OutputStreamDecorator outputStreamDecorator;

	/**
	 * @param taskDescription the String describing the owner operator instance
	 * @param configuration this job's configuration
	 */
	public FSMetricsManager(String taskDescription, JobVertexID jobVertexId, Configuration configuration, int idInModel, int maximumKeygroups) {

		taskId = taskDescription;
		String workerId = taskId.replace("Timestamps/Watermarks", "Timestamps-Watermarks");
		workerName = workerId.substring(0, workerId.indexOf("(") - 1);
		instanceId = Integer.parseInt(workerId.substring(workerId.lastIndexOf("(") + 1, workerId.lastIndexOf("/"))) - 1; // need to consistent with partitionAssignment
		instanceId = idInModel;
//		System.out.println("----updated task with instance id is: " + workerName + "-" + instanceId);
		System.out.println("start execution: " + workerName + "-" + instanceId + " time: " + System.currentTimeMillis());
		numInstances = Integer.parseInt(workerId.substring(workerId.lastIndexOf("/") + 1, workerId.lastIndexOf(")")));
		status = new ProcessingStatus();

		windowSize = configuration.getLong(WINDOW_SIZE);
		int nRecords = configuration.getInteger(N_RECORDS);

		currentWindowStart = status.getProcessingStart();

		this.jobVertexId = jobVertexId;

		OutputStream outputStream;
		try {
			String expDir = configuration.getString(EXP_DIR);
			outputStream = new FileOutputStream(expDir + "/" + getJobVertexId() + ".output");
		} catch (FileNotFoundException e) {
			e.printStackTrace();
			outputStream = System.out;
		}
		LOG.info("###### " + getJobVertexId() + " new task created");
		outputStreamDecorator = new OutputStreamDecorator(outputStream);
	}

	public void updateTaskId(String taskDescription, Integer idInModel) {
//		synchronized (status) {
		LOG.debug("###### Starting update task metricsmanager from " + workerName + "-" + instanceId + " to " + workerName + "-" + idInModel);
		// not only need to update task id, but also the states.
		if (idInModel == Integer.MAX_VALUE/2) {
			System.out.println("end execution: " + workerName + "-" + instanceId + " time: " + System.currentTimeMillis());
		} else if (instanceId != idInModel && instanceId == Integer.MAX_VALUE/2) {
			System.out.println("start execution: " + workerName + "-" + idInModel + " time: " + System.currentTimeMillis());
		}

		instanceId = idInModel; // need to consistent with partitionAssignment

		taskId = taskDescription;
		String workerId = taskId.replace("Timestamps/Watermarks", "Timestamps-Watermarks");
		numInstances = Integer.parseInt(workerId.substring(workerId.lastIndexOf("/") + 1, workerId.lastIndexOf(")")));

		status.reset();

		totalRecordsIn = 0;	// Total number of records ingested since the last flush
		totalRecordsOut = 0;	// Total number of records produced since the last flush

		// clear counters
		recordsIn = 0;
		recordsOut = 0;
		usefulTime = 0;
		currentWindowStart = 0;
		latency = 0;
		epoch = 0;

		LOG.debug("###### End update task metricsmanager");
//		}
	}

	@Override
	public String getJobVertexId() {
		return workerName + "-" + instanceId;
	}

	/**
	 * Once the current input buffer has been consumed, calculate and log useful and waiting durations
	 * for this buffer.
	 * @param timestamp the end buffer timestamp
	 * @param processing total duration of processing for this buffer
	 * @param numRecords total number of records processed
	 */
	@Override
	public void inputBufferConsumed(long timestamp, long deserializationDuration, long processing, long numRecords, long endToEndLatency) {
		if (currentWindowStart == 0) {
			currentWindowStart = timestamp;
		}

		status.setProcessingEnd(timestamp);

		// aggregate the metrics
		recordsIn += numRecords;
		recordsOut += status.getNumRecordsOut();
		usefulTime += processing + deserializationDuration;
//			usefulTime += processing + status.getSerializationDuration()
//				- status.getWaitingForWriteBufferDuration();

		latency += endToEndLatency;

		// clear status counters
		status.clearCounters();

		// if window size is reached => output
		if (timestamp - currentWindowStart > windowSize) {
			// compute rates
			long duration = timestamp - currentWindowStart;
//			double trueProcessingRate = (recordsIn / (usefulTime / 1000.0)) * 1000000;
//			double trueOutputRate = (recordsOut / (usefulTime / 1000.0)) * 1000000;
//			double observedProcessingRate = (recordsIn / (duration / 1000.0)) * 1000000;
//			double observedOutputRate = (recordsOut / (duration / 1000.0)) * 1000000;
			double trueProcessingRate = (recordsIn / (usefulTime / 1000.0)) * 1000;
			double trueOutputRate = (recordsOut / (usefulTime / 1000.0)) * 1000;
			double observedProcessingRate = (recordsIn / (duration / 1000.0)) * 1000;
			double observedOutputRate = (recordsOut / (duration / 1000.0)) * 1000;
			float endToEndLantecy = (float) latency / recordsIn;

			double utilization = (double) usefulTime / duration;

			// for network calculus
			totalRecordsIn += recordsIn;
			totalRecordsOut += recordsOut;

			// log the rates: one file per epoch
			String ratesLine = jobVertexId + ","
				+ workerName + "-" + instanceId + ","
				+ numInstances  + ","
				+ trueProcessingRate + ","
				+ trueOutputRate + ","
				+ observedProcessingRate + ","
				+ observedOutputRate + ","
				+ endToEndLantecy + ","
				+ utilization + ","
				+ System.currentTimeMillis();

			System.out.println(ratesLine);

			// clear counters
			recordsIn = 0;
			recordsOut = 0;
			usefulTime = 0;
			currentWindowStart = 0;
			latency = 0;
			epoch++;
		}
	}

	@Override
	public void groundTruth(int keyGroup, long arrivalTs, long completionTs) {
	}

	@Override
	public void groundTruth(long arrivalTs, long latency) {
		outputStreamDecorator.println(String.format("ts: %d endToEnd latency: %d", arrivalTs, latency));
	}

	/**
	 * A new input buffer has been retrieved with the given timestamp.
	 */
	@Override
	public void newInputBuffer(long timestamp) {
		status.setProcessingStart(timestamp);
		// the time between the end of the previous buffer processing and timestamp is "waiting for input" time
		status.setWaitingForReadBufferDuration(timestamp - status.getProcessingEnd());
	}

	@Override
	public void addSerialization(long serializationDuration) {
		status.addSerialization(serializationDuration);
	}

	@Override
	public void incRecordsOut() {
		status.incRecordsOut();
	}

	@Override
	public void incRecordsOutKeyGroup(int targetKeyGroup) {
		status.incRecordsOutChannel(targetKeyGroup);
	}

	@Override
	public void incRecordIn(int keyGroup) {
		status.incRecordsIn(keyGroup);
	}

	@Override
	public void addWaitingForWriteBufferDuration(long duration) {
		status.addWaitingForWriteBuffer(duration);

	}

	/**
	 * The source consumes no input, thus it must log metrics whenever it writes an output buffer.
	 * @param timestamp the timestamp when the current output buffer got full.
	 */
	@Override
	public void outputBufferFull(long timestamp) {
		if (taskId.contains("Source")) {

//			synchronized (status) {

			if (currentWindowStart == 0) {
				currentWindowStart = timestamp;
			}

			setOutBufferStart(timestamp);

			// aggregate the metrics
			recordsOut += status.getNumRecordsOut();
			if (status.getWaitingForWriteBufferDuration() > 0) {
				waitingTime += status.getWaitingForWriteBufferDuration();
			}

			// clear status counters
			status.clearCounters();

			// if window size is reached => output
			if (timestamp - currentWindowStart > windowSize) {

				// compute rates
				long duration = timestamp - currentWindowStart;
				usefulTime = duration - waitingTime;
//				double trueOutputRate = (recordsOut / (usefulTime / 1000.0)) * 1000000;
//				double observedOutputRate = (recordsOut / (duration / 1000.0)) * 1000000;
				double trueOutputRate = (recordsOut / (usefulTime / 1000.0)) * 1000;
				double observedOutputRate = (recordsOut / (duration / 1000.0)) * 1000;
				totalRecordsOut += recordsOut;

				// for network calculus
				totalRecordsIn += recordsIn;
				totalRecordsOut += recordsOut;

				// log the rates: one file per epoch
				String ratesLine = jobVertexId + ","
					+ workerName + "-" + instanceId + ","
					+ numInstances  + ","
					+ 0 + ","
					+ trueOutputRate + ","
					+ 0 + ","
					+ observedOutputRate + ","
					+ 0 + ","
					+ 0 + ","
					+ System.currentTimeMillis();

				System.out.println(ratesLine);

				// clear counters
				recordsOut = 0;
				usefulTime = 0;
				waitingTime = 0;
				currentWindowStart = 0;
				epoch++;
			}
		}
	}

	private void setOutBufferStart(long start) {
		status.setOutBufferStart(start);
	}


	@Override
	public void updateMetrics() {
	}
}
