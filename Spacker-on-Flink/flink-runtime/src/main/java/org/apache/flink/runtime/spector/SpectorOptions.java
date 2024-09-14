package org.apache.flink.runtime.spector;

import org.apache.flink.configuration.ConfigOption;

import static org.apache.flink.configuration.ConfigOptions.key;

public class SpectorOptions {
	// Main stat migration settings

	public final static ConfigOption<String> RECONFIG_SCENARIO =
		key("spector.reconfig.scenario")
			.defaultValue("shuffle");

	public final static ConfigOption<Double> WORKLOAD_ZIPF_SKEW =
		key("spector.reconfig.workload.zipf_skew")
			.defaultValue(1.0);


	public final static ConfigOption<String> ORDER_FUNCTION =
		key("spector.reconfig.order_function")
			.defaultValue("default");

	public final static ConfigOption<Integer> NUM_AFFECTED_KEYS =
			key("spector.reconfig.affected_keys")
			.defaultValue(64);

	public final static ConfigOption<Integer> NUM_AFFECTED_TASKS =
			key("spector.reconfig.affected_tasks")
			.defaultValue(2);

	public final static ConfigOption<String> RECONFIG_START_TIME =
			key("spector.reconfig.start")
			.defaultValue("5 * 1000");

	public final static ConfigOption<Integer> RECONFIG_INTERVAL =
			key("spector.reconfig.interval")
			.defaultValue(10 * 1000);

	public final static ConfigOption<Integer> SYNC_KEYS =
			key("spector.reconfig.sync_keys")
			.defaultValue(0);

	public final static ConfigOption<String> TARGET_OPERATORS =
			key("controller.target.operators")
			.defaultValue("flatmap");

	public final static ConfigOption<Integer> REPLICATE_KEYS_FILTER =
			key("spector.replicate_keys_filter")
			.defaultValue(1);

	// Netty optimization part configurations

	public final static ConfigOption<Boolean> NETTY_STATE_TRANSMISSION_ENABLED =
		key("netty.state.transmission.enabled")
			.defaultValue(true);
	public final static ConfigOption<Boolean> NETTY_OPTIMIZED_DEPLOYMENT_ENABLED =
			key("netty.optimized.deployment.enabled")
			.defaultValue(false);

	public final static ConfigOption<Boolean> NETTY_OPTIMIZED_ACK_ENABLED =
			key("netty.optimized.acknowledgement.enabled")
			.defaultValue(false);

	public final static ConfigOption<Boolean> NETTY_CHUNKED_DEPLOYMENT_ENABLED =
		key("netty.chunked.deployment.enabled")
			.defaultValue(true);

	public final static ConfigOption<Boolean> NETTY_CHUNKED_ACK_ENABLED =
		key("netty.chunked.acknowledgement.enabled")
			.defaultValue(true);

	public final static ConfigOption<Boolean> SNAPSHOT_CHANGELOG_ENABLED =
			key("snapshot.changelog.enabled")
			.defaultValue(true);


	// Old configuration that related to control logic and metrics retriever

	public final static ConfigOption<Long> WINDOW_SIZE =
			key("policy.windowSize")
			.defaultValue(1_000_000_000L);

	public final static ConfigOption<Integer> N_RECORDS =
			key("policy.metrics.nrecords")
			.defaultValue(15);

	public final static ConfigOption<String> EXP_DIR =
			key("spector.exp.dir")
			.defaultValue("/data/spector");

	public final static ConfigOption<String> METRICS_KAFKA_TOPIC =
			key("policy.metrics.topic")
			.defaultValue("flink_metrics");

	public final static ConfigOption<String> METRICS_KAFKA_SERVER =
			key("policy.metrics.servers")
			.defaultValue("localhost:9092");

	public final static ConfigOption<Integer> MODEL_METRICS_RETRIEVE_WARMUP =
			key("model.metrics.warmup")
			.defaultValue(100);

	public final static ConfigOption<Integer> STATE_TRANSFER_DELAY =
		key("model.state.transfer.delay")
			.defaultValue(200);


	// Netty server/client related configurations

	public final static ConfigOption<Integer> NETTY_CHANNEL_COUNT =
		key("spector.netty.channel.count")
			.defaultValue(1);

	public final static ConfigOption<Integer> NETTY_CONNECTION_TIMEOUT =
		key("spector.netty.connection.timeout")
			.defaultValue(10000);

	public final static ConfigOption<Integer> NETTY_LOW_WATERMARK =
		key("spector.netty.low_watermark")
//			.defaultValue(10 * 1024 * 1024);
			.defaultValue(32 * 1024);

	public final static ConfigOption<Integer> NETTY_HIGH_WATERMARK =
		key("spector.netty.high_watermark")
//			.defaultValue(50 * 1024 * 1024);
			.defaultValue(64 * 1024);
}
