package org.apache.flink.runtime.spector.metrics;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.jobgraph.JobEdge;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.spector.migration.JobExecutionPlan;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.apache.flink.runtime.spector.SpectorOptions.*;

public class KafkaMetricsRetriever implements MetricsRetriever {
	private static final Logger LOG = LoggerFactory.getLogger(KafkaMetricsRetriever.class);
	private AtomicBoolean closed = new AtomicBoolean();
	private CountDownLatch shutdownLatch = new CountDownLatch(1);
	private String servers;

	Set<String> containerIds;

	private Configuration jobConfiguration;
	private String TOPIC;
	private KafkaConsumer<String, String> consumer;
	private JobGraph jobGraph;
	private JobVertexID vertexID;
	private List<JobVertexID> upstreamVertexIDs = new ArrayList<>();
	private int nRecords;

	private long startTs = System.currentTimeMillis();
	private int metrcsWarmUpTime;
	private int numPartitions;

	private HashMap<String, Long> workerTimeStamp = new HashMap<>(); // latest timestamp of each partition, timestamp smaller than value in this should be invalid
	private HashMap<String, Double> lastExecutorServiceRate = new HashMap<>();
	private HashMap<String, ArrayList<Double>> executorServiceRateWindow = new HashMap<>();
	private int windowSize = 10;
	private double initialParallelism;

	@Override
	public void init(JobGraph jobGraph, JobVertexID vertexID, Configuration jobConfiguration, int numPartitions, int parallelism) {
		this.jobGraph = jobGraph;
		this.vertexID = vertexID;

		this.jobConfiguration = jobConfiguration;
		TOPIC = jobConfiguration.getString(METRICS_KAFKA_TOPIC);
		servers = jobConfiguration.getString(METRICS_KAFKA_SERVER);
		nRecords = jobConfiguration.getInteger(N_RECORDS);
		metrcsWarmUpTime = jobConfiguration.getInteger(MODEL_METRICS_RETRIEVE_WARMUP);

		JobVertex curVertex = jobGraph.findVertexByID(vertexID);
		for (JobEdge jobEdge : curVertex.getInputs()) {
			JobVertexID id = jobEdge.getSource().getProducer().getID();
			upstreamVertexIDs.add(id);
		}

		this.numPartitions = numPartitions;
		this.initialParallelism = parallelism;
		initConsumer();
	}

	public void initConsumer(){
		final Properties props = new Properties();
		props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, servers);
		props.put(ConsumerConfig.GROUP_ID_CONFIG, vertexID.toString());
		props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
		props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
		consumer = new KafkaConsumer(props);
		consumer.subscribe(Arrays.asList(TOPIC));
	}

	@Override
	public Map<String, Object> retrieveMetrics() {
		// TODO: source operator should be skipped
		// retrieve metrics from Kafka, consume topic and get upstream and current operator metrics

		Map<String, Object> metrics = new HashMap<>();

		HashMap<String, Long> partitionArrived = new HashMap<>();
		HashMap<String, Double> executorUtilization = new HashMap<>();
		// need to set a decay factor for service rate
		HashMap<String, Double> executorServiceRate = new HashMap<>();
		HashMap<String, Long> partitionProcessed = new HashMap<>();
		HashMap<String, Boolean> partitionValid = new HashMap<>();

		// to record the timestamp of current worker for update
		HashMap<String, Long> workerCurTimeStamp = new HashMap<>();

//		for (int i=0; i<numPartitions; i++) {
//			partitionValid.put(i+"", false);
//		}

		HashMap<String, HashMap<String, Long>> upstreamArrived = new HashMap<>(); // store all executors, not just vertex id


		synchronized (consumer) {
			consumer.poll(100);
			Set<TopicPartition> assignedPartitions = consumer.assignment();
			consumer.seekToEnd(assignedPartitions);
			for (TopicPartition partition : assignedPartitions) {
//				System.out.println(vertexID + ": " + consumer.position(partition));
				long endPosition = consumer.position(partition);
				long recentMessagesStartPosition = endPosition - nRecords < 0 ? 0 : endPosition - nRecords;
				consumer.seek(partition, recentMessagesStartPosition);
				LOG.info("------------------------------");
				LOG.info("start position: " + recentMessagesStartPosition
					+ " end position: " + endPosition);
				LOG.info("------------------------------");
			}

			ConsumerRecords<String, String> records = consumer.poll(100);
			if (!records.isEmpty()) {
				// parse records, should construct metrics hashmap
				for (ConsumerRecord<String, String> record : records) {
					if (record.value().equals("")) {
						continue;
					}
					String[] ratesLine = record.value().split(",");
					JobVertexID jobVertexId = JobVertexID.fromHexString(ratesLine[0]);
					if (jobVertexId.equals(this.vertexID)) {
						retrievePartitionProcessed(ratesLine, partitionProcessed, executorUtilization
							, executorServiceRate, partitionValid, workerTimeStamp, workerCurTimeStamp);
					}

					// find upstream numRecordOut
					if (upstreamVertexIDs.contains(jobVertexId)) {
						// get executor id
						String upstreamExecutorId = ratesLine[1];
						// put into the corresponding upstream queue, aggregate later
						HashMap<String, Long> curUpstreamArrived = upstreamArrived.getOrDefault(upstreamExecutorId, new HashMap<>());
						retrievePartitionArrived(ratesLine, curUpstreamArrived, partitionValid, workerTimeStamp, workerCurTimeStamp);
						upstreamArrived.put(upstreamExecutorId, curUpstreamArrived);
					}
				}
			}
		}

		// aggregate partitionArrived
		for (Map.Entry entry : upstreamArrived.entrySet()) {
			for (Map.Entry subEntry : ((HashMap<String, Long>) entry.getValue()).entrySet()) {
				String keygroup = (String) subEntry.getKey();
				long keygroupArrived = (long) subEntry.getValue();
				partitionArrived.put(keygroup, partitionArrived.getOrDefault(keygroup, 0l) + keygroupArrived);
			}
		}

		if (System.currentTimeMillis() - startTs < metrcsWarmUpTime*1000
			&& partitionProcessed.size() == 0 && partitionArrived.size() == 0) {
			// make all metrics be 0.0
			for (int i=0; i<numPartitions; i++) {
				String keyGroup = String.valueOf(i);
				partitionArrived.put(keyGroup, 0l);
				partitionProcessed.put(keyGroup, 0l);
				partitionValid.put(keyGroup, true);
			}
			for (int i=0; i<initialParallelism; i++) {
				String executorId = i+"";
				executorUtilization.put(executorId, 0.01);
			}
		}


		Boolean isAllValid = true;
		for (int i=0; i<numPartitions; i++) {
			String executorId = String.valueOf(i);
			if (partitionArrived.containsKey(executorId) && partitionProcessed.containsKey(executorId)
				&& partitionArrived.get(executorId) >= partitionProcessed.get(executorId)) {
				partitionValid.put(executorId, true);
			} else {
				partitionValid.put(executorId, false);
				isAllValid = false;
			}
		}

		if (isAllValid) {
			// put new timestamp only when all partitions are valid
			for (Map.Entry entry : workerCurTimeStamp.entrySet()) {
				String workerId = (String) entry.getKey();
				long timestamp = (long) entry.getValue();
				workerTimeStamp.put(workerId, timestamp);
			}
		}

		LOG.info("utilization: " + executorUtilization);

		metrics.put("Arrived", partitionArrived);
		metrics.put("Processed", partitionProcessed);
		metrics.put("Utilization", executorUtilization);
		metrics.put("ServiceRate", executorServiceRate);
		metrics.put("Validity", partitionValid);

		return metrics;
	}

	public JobVertexID getVertexId() {
		return vertexID;
	}

	public String getPartitionId(String keyGroup) {
		return  keyGroup.split(":")[0];
//		return  "Partition " + keyGroup.split(":")[0];
	}

	public void retrievePartitionArrived(String[] ratesLine, HashMap<String, Long> partitionArrived,
										 HashMap<String, Boolean> partitionValid, HashMap<String, Long> workerTimeStamp,
										 HashMap<String, Long> workerCurTimeStamp) {
		// TODO: need to consider multiple upstream tasks, we need to sum values from different upstream tasks
		if (!ratesLine[11].equals("0")) {
			long timestamp = Long.valueOf(ratesLine[13]);
			String workerId = ratesLine[1];
			// use timestamp to keeps the latest metrics of each executor.
			if (workerTimeStamp.getOrDefault(workerId, 0l) >= timestamp) {
				return;
			}

			workerCurTimeStamp.put(workerId, timestamp);

			String[] keyGroupsArrived = ratesLine[11].split("&");
			for (String keyGroup : keyGroupsArrived) {
				String partition = getPartitionId(keyGroup);
				long arrived = Long.valueOf(keyGroup.split(":")[1]);
				if (partitionArrived.getOrDefault(partition, 0l) <= arrived) {
					partitionArrived.put(partition, arrived);
//					partitionValid.put(partition, true);
				}
			}
		}
	}

	public void retrievePartitionProcessed(String[] ratesLine, HashMap<String, Long> partitionProcessed,
										   HashMap<String, Double> executorUtilization, HashMap<String, Double> executorServiceRate,
										   HashMap<String, Boolean> partitionValid,
										   HashMap<String, Long> workerTimeStamp, HashMap<String, Long> workerCurTimeStamp) {
		// keygroups processed
		if (!ratesLine[12].equals("0")) {
			long timestamp = Long.valueOf(ratesLine[13]);
			String workerId = ratesLine[1];
			String executorId = workerId.split("-")[1];

			// use timestamp to keeps the latest metrics of each executor.
			if (workerTimeStamp.getOrDefault(workerId, 0l) >= timestamp) {
				return;
			}

			workerCurTimeStamp.put(workerId, timestamp);

			// utilization of executor
			if (Integer.valueOf(executorId) == JobExecutionPlan.UNUSED_SUBTASK) {
				return;
			}

			String[] keyGroupsProcessed = ratesLine[12].split("&");
			int actual_processed = 0;
			for (String keyGroup : keyGroupsProcessed) {
				String partition = getPartitionId(keyGroup);
				long processed = Long.valueOf(keyGroup.split(":")[1]);
				if (partitionProcessed.getOrDefault(partition, 0l) <= processed) {
					partitionProcessed.put(partition, processed);
//					partitionValid.put(partition, true);
					actual_processed += processed;
				}
			}

			if (Double.valueOf(ratesLine[10]) > 0) {
				executorUtilization.put(executorId, Double.valueOf(ratesLine[10]));
			}
			if (Double.valueOf(ratesLine[3]) > 0) {
//				double lastServiceRate = lastExecutorServiceRate.getOrDefault(executorId, 0.0);
//
//				if(lastServiceRate == 0.0) {
//					lastServiceRate = Double.valueOf(ratesLine[3]);
//				}
//
//				double instantServiceRate = Double.valueOf(ratesLine[3]);
//
//				double serviceRate = decayFactor * lastServiceRate + (1 - decayFactor) * instantServiceRate;
//				executorServiceRate.put(executorId, serviceRate);
//				lastExecutorServiceRate.put(executorId, serviceRate);

				double serviceRate = Double.valueOf(ratesLine[3]);
				executorServiceRate.put(executorId, serviceRate);
				lastExecutorServiceRate.put(executorId, serviceRate);

//				double curServiceRate = Double.valueOf(ratesLine[3]);
//				ArrayList<Double> slidingWindow = executorServiceRateWindow.getOrDefault(executorId, new ArrayList<>());
//				// if sliding window is full, kick out first one, append the new one
//				if (slidingWindow.size() == windowSize) {
//					slidingWindow.remove(0);
//				}
//				slidingWindow.add(curServiceRate);
//				executorServiceRateWindow.put(executorId, slidingWindow);
//				// compute average service rate (sum slidingWidow/ window size)
//				double sumWindow = slidingWindow.stream().mapToDouble(a -> a).sum();
//				double serviceRate = sumWindow/slidingWindow.size();
//				executorServiceRate.put(executorId, serviceRate);
			}

//			LOG.info("Executor id: " + ratesLine[1] + " utilization: " + ratesLine[10] + " processed: " + ratesLine[8]
//				+ " true rate: " + ratesLine[3] + " observed rate: " + ratesLine[5]);
//			LOG.info("actual processed: " + actual_processed + " records in: " + ratesLine[8] + " partition processed: " + ratesLine[12]);
		}
	}
}
