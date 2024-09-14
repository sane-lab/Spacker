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

import java.io.Serializable;
import java.util.*;

import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.flink.runtime.spector.SpectorOptions.*;

/**
 * The MetricsManager is responsible for logging activity profiling information (except for messages).
 * It gathers start and end events for deserialization, processing, serialization, blocking on read and write buffers
 * and records activity durations. There is one MetricsManager instance per Task (operator instance).
 * The MetricsManager aggregates metrics in a {@link ProcessingStatus} object and outputs processing and output rates
 * periodically to a designated rates file.
 */
public class KafkaMetricsManager implements Serializable, MetricsManager {
	private static final Logger LOG = LoggerFactory.getLogger(KafkaMetricsManager.class);

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

	private final String TOPIC;
	private final String STATUS_TOPIC = "flink_keygroups_status";	// status topic, used to do recovery.
	private final String servers;
	private int nRecords;
	private KafkaProducer<String, String> producer;
	private KafkaProducer<String, String> statusProducer;

	private final int numKeygroups;

	private HashMap<Integer, Long> kgLatencyMap = new HashMap<>(); // keygroup -> avgLatency
	private HashMap<Integer, Integer> kgNRecordsMap = new HashMap<>(); // keygroup -> nRecords
	private long lastTimeSlot = 0l;


	/**
	 * @param taskDescription the String describing the owner operator instance
	 * @param jobConfiguration this job's configuration
	 */
	public KafkaMetricsManager(String taskDescription, JobVertexID jobVertexId, Configuration jobConfiguration, int idInModel, int maximumKeygroups) {
		numKeygroups = maximumKeygroups;

		taskId = taskDescription;
		String workerId = taskId.replace("Timestamps/Watermarks", "Timestamps-Watermarks");
		workerName = workerId.substring(0, workerId.indexOf("(")-1);
		instanceId = Integer.parseInt(workerId.substring(workerId.lastIndexOf("(")+1, workerId.lastIndexOf("/"))) - 1; // need to consistent with partitionAssignment
//		System.out.println("----new task with instance id: " + instanceId);
		instanceId = idInModel;
		System.out.println("----updated task with instance id is: " + workerName + "-" + instanceId);
		numInstances = Integer.parseInt(workerId.substring(workerId.lastIndexOf("/")+1, workerId.lastIndexOf(")")));
		status = new ProcessingStatus();

//		windowSize = jobConfiguration.getLong("policy.windowSize",  10_000_000_000L);
//		ratesPath = jobConfiguration.getString("policy.rates.path", "rates/");
		windowSize = jobConfiguration.getLong(WINDOW_SIZE);
//		System.out.println("window size is : " + windowSize);1000000000
		TOPIC = jobConfiguration.getString(METRICS_KAFKA_TOPIC);
		servers = jobConfiguration.getString(METRICS_KAFKA_SERVER);
		nRecords = jobConfiguration.getInteger(N_RECORDS);

		currentWindowStart = status.getProcessingStart();

		this.jobVertexId = jobVertexId;

		initKakfaProducer();
		initStatus();
	}

	public void updateTaskId(String taskDescription, Integer idInModel) {
//		synchronized (status) {
		LOG.info("++++++Starting update task metricsmanager from " + workerName + "-" + instanceId + " to " + workerName + "-" + idInModel);
		// not only need to update task id, but also the states.
		instanceId = idInModel; // need to consistent with partitionAssignment

		taskId = taskDescription;
		String workerId = taskId.replace("Timestamps/Watermarks", "Timestamps-Watermarks");
		numInstances = Integer.parseInt(workerId.substring(workerId.lastIndexOf("/") + 1, workerId.lastIndexOf(")")));

		status.reset();

		totalRecordsIn = 0;	// Total number of records ingested since the last flush
		totalRecordsOut = 0;	// Total number of records produced since the last flush

		currentWindowStart = status.getProcessingStart();
		// clear counters
		recordsIn = 0;
		recordsOut = 0;
		usefulTime = 0;
		currentWindowStart = 0;
		latency = 0;
		epoch = 0;

		initStatus();
		LOG.info("++++++End update task metricsmanager");
//		}
	}

	@Override
	public void groundTruth(long arrivalTs, long latency) {

	}

	@Override
	public String getJobVertexId() {
		return workerName + "-" + instanceId;
	}

	private void initKakfaProducer() {
		Properties props = new Properties();
		props.put("bootstrap.servers", servers);
		props.put("client.id", workerName+instanceId);
		props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		producer = new KafkaProducer<>(props);
		statusProducer = new KafkaProducer<>(props);
	}

	private void initStatus(){
		if (instanceId == Integer.MAX_VALUE/2) {
			return;
		}

		KafkaConsumer consumer;
		final Properties props = new Properties();
		props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, servers);
		props.put(ConsumerConfig.GROUP_ID_CONFIG, workerName+instanceId);
		props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
		props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
		props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 5000);
		consumer = new KafkaConsumer(props);
		// subscribe to the status topic
		consumer.subscribe(Collections.singletonList(STATUS_TOPIC));

		synchronized (consumer) {
			startFromOffset(consumer, nRecords);

			ConsumerRecords<String, String> records = consumer.poll(1000);
			LOG.info("++++++Number of records: " + records.count());

			// just for when all task deployed at beginning, should not read valid state
			if (records.isEmpty()) {
				consumer.close();
				return;
			}

			recoverState(records);

			// this code is buggy
//			int offset = nRecords;
//			// for every recovery, need to check sanity of state
			if (status.inputKeyGroupState.size() != numKeygroups) {
				LOG.info("++++++task: " + getJobVertexId() + " failed to recover correctly, num of keygroups recovered: " + status.inputKeyGroupState.size()
					+ ", restart again.");
//				// recover again
//				startFromOffset(consumer, offset);
//				records = consumer.poll(100);
//				LOG.info("++++++Number of records repoll: " + records.count());
//
//				if (records.isEmpty()) {
//					consumer.close();
//					return;
//				}
//				recoverState(records);
//				offset += nRecords;
			}

			consumer.close();
//			Preconditions.checkState(status.inputKeyGroupState.size() != numKeygroups, "task " + getJobVertexId()
//				+ " failed to recover correctly, num of keygroups recovered: " + status.inputKeyGroupState.size());
		}

//		StringBuilder keyGroupOutput = new StringBuilder();
//		if (!status.outputKeyGroupState.isEmpty()) {
//			for (Map.Entry<Integer, Long> entry : status.outputKeyGroupState.entrySet()) {
//				keyGroupOutput.append(entry.getKey()).append(":").append(entry.getValue()).append("&");
//			}
//			keyGroupOutput = new StringBuilder(keyGroupOutput.substring(0, keyGroupOutput.length() - 1));
//		}
//		System.out.println("output: " + keyGroupOutput.toString());
		StringBuilder keyGroupinput = new StringBuilder();
		if (!status.inputKeyGroupState.isEmpty()) {
			for (Map.Entry<Integer, Long> entry : status.inputKeyGroupState.entrySet()) {
				keyGroupinput.append(entry.getKey()).append(":").append(entry.getValue()).append("&");
			}
			keyGroupinput = new StringBuilder(keyGroupinput.substring(0, keyGroupinput.length() - 1));
		}
		LOG.info("++++++worker: " + getJobVertexId() + " status recovered with size: " + status.inputKeyGroupState.size() + " content: " + keyGroupinput.toString());
	}

	private void startFromOffset(KafkaConsumer consumer, int nRecords) {
		consumer.poll(100);
		Set<TopicPartition> assignedPartitions = consumer.assignment();
		consumer.seekToEnd(assignedPartitions);
		for (TopicPartition partition : assignedPartitions) {
			long endPosition = consumer.position(partition);
			long recentMessagesStartPosition = endPosition - nRecords < 0 ? 0 : endPosition - nRecords;
			LOG.info("++++++start position: " + recentMessagesStartPosition
				+ " end position: " + endPosition);

			consumer.seek(partition, recentMessagesStartPosition);
		}
	}

	private void recoverState(ConsumerRecords<String, String> records) {
		for (ConsumerRecord<String, String> record : records) {
			String[] ratesLine = record.value().split(",");
			JobVertexID jobVertexId = JobVertexID.fromHexString(ratesLine[0]);
			if (jobVertexId.equals(this.jobVertexId)) {
				if (!ratesLine[12].equals("0")) {
					String[] keyGroupsProcessed = ratesLine[12].split("&");
//						System.out.println("keygroups processed: " + ratesLine[12]);
					for (String keyGroup : keyGroupsProcessed) {
						int key = Integer.valueOf(keyGroup.split(":")[0]);
						long processed = Long.valueOf(keyGroup.split(":")[1]);
						if (processed > status.inputKeyGroupState.getOrDefault(key, 0l)) {
							status.inputKeyGroupState.put(key, processed);
						}
					}
				}
				if (!ratesLine[11].equals("0")) {
					String[] keyGroupsArrived = ratesLine[11].split("&");
					for (String keyGroup : keyGroupsArrived) {
						int key = Integer.valueOf(keyGroup.split(":")[0]);
						long arrived = Long.valueOf(keyGroup.split(":")[1]);
						if (arrived > status.outputKeyGroupState.getOrDefault(key, 0l)) {
							status.outputKeyGroupState.put(key, arrived);
						}
					}
				}
			}
		}
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

//		synchronized (status) {
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
			float endToEndLantecy = (float) latency/recordsIn;

			double utilization = (double) usefulTime / duration;

			// for network calculus
			totalRecordsIn += recordsIn;
			totalRecordsOut += recordsOut;

			String keyGroupOutput = new String();
			if (!status.outputKeyGroup.isEmpty()) {
				for (Map.Entry<Integer, Long> entry : status.outputKeyGroup.entrySet()) {
					keyGroupOutput += entry.getKey() + ":" + entry.getValue() + "&";
				}
				keyGroupOutput = keyGroupOutput.substring(0, keyGroupOutput.length() - 1);
			} else {
				keyGroupOutput = "0";
			}

			String keyGroupinput = new String();
			if (!status.inputKeyGroup.isEmpty()) {
				for (Map.Entry<Integer, Long> entry : status.inputKeyGroup.entrySet()) {
					keyGroupinput += entry.getKey() + ":" + entry.getValue() + "&";
				}
				keyGroupinput = keyGroupinput.substring(0, keyGroupinput.length() - 1);
			} else {
				keyGroupinput = "0";
			}

			// log the rates: one file per epoch
			String ratesLine = jobVertexId + ","
				+ workerName + "-" + instanceId + ","
				+ numInstances  + ","
				+ trueProcessingRate + ","
				+ trueOutputRate + ","
				+ observedProcessingRate + ","
				+ observedOutputRate + ","
				+ endToEndLantecy + ","
				+ recordsIn + ","
				+ recordsOut + ","
				+ utilization + ","
				+ keyGroupOutput + ","
				+ keyGroupinput + ","
				+ System.currentTimeMillis();

//				String ratesLine = jobVertexId + ","
//					+ workerName + "-" + instanceId + ","
//					+ " trueProcessingRate: " + trueProcessingRate + ","
//					+ " observedProcessingRate: " + observedProcessingRate + ","
//					+ " endToEndLantecy: " + endToEndLantecy + ","
//					+ " utilization: " + utilization;

//				System.out.println("workername: " + getJobVertexId() + " epoch: " + epoch + " keygroups: " + status.inputKeyGroup.keySet());

			ProducerRecord<String, String> newRecord = new ProducerRecord<>(TOPIC, ratesLine);
			producer.send(newRecord);

			// clear counters
			recordsIn = 0;
			recordsOut = 0;
			usefulTime = 0;
			currentWindowStart = 0;
			latency = 0;
			epoch++;
		}
//		}
	}

	@Override
	public void groundTruth(int keyGroup, long arrivalTs, long completionTs) {

		// send out comparative ground truth
		System.out.println("keygroup: " + keyGroup + " arrival_ts: " + arrivalTs + " completion_ts: " + completionTs);

		if (completionTs - lastTimeSlot >= 1000) {
			// print out to stdout, and clear the state
			Iterator it = kgLatencyMap.entrySet().iterator();
			while (it.hasNext()) {
				Map.Entry kv = (Map.Entry)it.next();
				int curKg = (int) kv.getKey();
				long sumLatency = (long) kv.getValue();
				int nRecords = kgNRecordsMap.get(curKg);
				float avgLatency = (float) sumLatency / nRecords;
//				System.out.println("timeslot: " + lastTimeSlot + " keygroup: "
//					+ curKg + " records: " + nRecords + " avglatency: " + avgLatency);
				System.out.println(String.format("GroundTruth: %d %d %d %f", lastTimeSlot, curKg, nRecords, avgLatency));
			}
			kgLatencyMap.clear();
			kgNRecordsMap.clear();
			lastTimeSlot = completionTs / 1000 * 1000;
		}
		kgNRecordsMap.put(keyGroup,
			kgNRecordsMap.getOrDefault(keyGroup, 0)+1);
		kgLatencyMap.put(keyGroup,
			kgLatencyMap.getOrDefault(keyGroup, 0L)+(completionTs - arrivalTs));
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

				String kegGroupOutput = new String();
				if (!status.outputKeyGroup.isEmpty()) {
					for (Map.Entry<Integer, Long> entry : status.outputKeyGroup.entrySet()) {
						kegGroupOutput = kegGroupOutput + entry.getKey() + ":" + entry.getValue() + "&";
					}
					kegGroupOutput = kegGroupOutput.substring(0, kegGroupOutput.length() - 1);
				} else {
					kegGroupOutput = "0";
				}

				// log the rates: one file per epoch
				String ratesLine = jobVertexId + ","
					+ workerName + "-" + instanceId + ","
					+ numInstances  + ","
					+ currentWindowStart + ","
					+ 0 + ","
//						+ trueOutputRate + ","
					+ 0 + ","
					+ observedOutputRate + ","
					+ 0 + "," // end to end latency should be 0.
					+ 0 + ","
					+ recordsOut + ","
					+ 0 + ","
					+ kegGroupOutput + ","
					+ 0 + ","
					+ System.currentTimeMillis();
				List<String> rates = Arrays.asList(ratesLine);
				ProducerRecord<String, String> newRecord = new ProducerRecord<>(TOPIC, ratesLine);
				producer.send(newRecord);

//					Path ratesFile = Paths.get(ratesPath + workerName.trim() + "-" + instanceId + "-" + epoch + ".log").toAbsolutePath();
//					try {
//						Files.write(ratesFile, rates, Charset.forName("UTF-8"));
//					} catch (IOException e) {
//						System.err.println("Error while writing rates file for epoch " + epoch
//							+ " on task " + taskId + ".");
//						e.printStackTrace();
//					}

				// clear counters
				recordsOut = 0;
				usefulTime = 0;
				waitingTime = 0;
				currentWindowStart = 0;
				epoch++;
			}
		}
//		}
	}

	private void setOutBufferStart(long start) {
		status.setOutBufferStart(start);
	}


	@Override
	public void updateMetrics() {
		if (instanceId == Integer.MAX_VALUE/2) {
			return;
		}

//		synchronized (status) {

		// compute rates
		long duration = System.nanoTime() - currentWindowStart;
		double trueProcessingRate = (recordsIn / (usefulTime / 1000.0)) * 1000000;
		double trueOutputRate = (recordsOut / (usefulTime / 1000.0)) * 1000000;
		double observedProcessingRate = (recordsIn / (duration / 1000.0)) * 1000000;
		double observedOutputRate = (recordsOut / (duration / 1000.0)) * 1000000;
		float endToEndLantecy = (float) latency/recordsIn;

		double utilization = (double) usefulTime / duration;

		// compute rates
		// for network calculus
		totalRecordsIn += recordsIn;
		totalRecordsOut += recordsOut;

		String kegGroupOutput = new String();
		if (!status.outputKeyGroup.isEmpty()) {
			for (Map.Entry<Integer, Long> entry : status.outputKeyGroup.entrySet()) {
				kegGroupOutput += entry.getKey() + ":" + entry.getValue() + "&";
			}
			kegGroupOutput = kegGroupOutput.substring(0, kegGroupOutput.length() - 1);
		} else {
			kegGroupOutput = "0";
		}

		String kegGroupinput = new String();
		if (!status.inputKeyGroup.isEmpty()) {
			for (Map.Entry<Integer, Long> entry : status.inputKeyGroup.entrySet()) {
				kegGroupinput += entry.getKey() + ":" + entry.getValue() + "&";
			}
			kegGroupinput = kegGroupinput.substring(0, kegGroupinput.length() - 1);
		} else {
			kegGroupinput = "0";
		}

		// log the rates: one file per epoch
		String ratesLine = jobVertexId + ","
			+ workerName + "-" + instanceId  + ","
			+ numInstances  + ","
			+ 0 + ","
			+ 0 + ","
			+ 0 + ","
			+ 0 + ","
			+ 0 + ","
			+ 0 + ","
			+ 0 + ","
			+ 0.1 + ","
			+ kegGroupOutput + ","
			+ kegGroupinput + ","
			+ System.currentTimeMillis();

		if (taskId.contains("MatchMaker")) {
			LOG.info("++++++force update - worker: " + workerName + "-" + instanceId + " keygroups processed:" + kegGroupinput);
		} else if (taskId.contains("Source")) {
			LOG.info("++++++force update - worker: " + workerName + "-" + instanceId + " keygroups output:" + kegGroupOutput);
		}
		// send to flink_metrics topic, for metrics retriever
		ProducerRecord<String, String> metricsRecord = new ProducerRecord<>(TOPIC, ratesLine);
		producer.send(metricsRecord);

		// send to flink keygroups status topic, for executor recovery
		ProducerRecord<String, String> statusRecord = new ProducerRecord<>(STATUS_TOPIC, ratesLine);
		statusProducer.send(statusRecord);
		statusProducer.flush();

		if (taskId.contains("MatchMaker") || taskId.contains("Source")) {
			LOG.info("++++++force update - worker: " + workerName + "-" + instanceId + " Completed!");
		}
		// clear counters
		recordsIn = 0;
		recordsOut = 0;
		usefulTime = 0;
		currentWindowStart = 0;
		latency = 0;
		epoch++;
//		}
	}
}
