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

package org.apache.flink.runtime.state.heap;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.state.AggregatingStateDescriptor;
import org.apache.flink.api.common.state.FoldingStateDescriptor;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReducingStateDescriptor;
import org.apache.flink.api.common.state.State;
import org.apache.flink.api.common.state.StateDescriptor;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.TypeSerializerSchemaCompatibility;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.fs.CloseableRegistry;
import org.apache.flink.runtime.checkpoint.CheckpointOptions;
import org.apache.flink.runtime.query.TaskKvStateRegistry;
import org.apache.flink.runtime.state.AbstractKeyedStateBackend;
import org.apache.flink.runtime.state.CheckpointStreamFactory;
import org.apache.flink.runtime.state.KeyExtractorFunction;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.KeyGroupedInternalPriorityQueue;
import org.apache.flink.runtime.state.Keyed;
import org.apache.flink.runtime.state.KeyedStateFunction;
import org.apache.flink.runtime.state.KeyedStateHandle;
import org.apache.flink.runtime.state.LocalRecoveryConfig;
import org.apache.flink.runtime.state.PriorityComparable;
import org.apache.flink.runtime.state.RegisteredKeyValueStateBackendMetaInfo;
import org.apache.flink.runtime.state.RegisteredPriorityQueueStateBackendMetaInfo;
import org.apache.flink.runtime.state.SnapshotResult;
import org.apache.flink.runtime.state.StateSerializerProvider;
import org.apache.flink.runtime.state.StateSnapshotRestore;
import org.apache.flink.runtime.state.StateSnapshotTransformer.StateSnapshotTransformFactory;
import org.apache.flink.runtime.state.StateSnapshotTransformers;
import org.apache.flink.runtime.state.StreamCompressionDecorator;
import org.apache.flink.runtime.state.ttl.TtlTimeProvider;
import org.apache.flink.util.FlinkRuntimeException;
import org.apache.flink.util.StateMigrationException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.RunnableFuture;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * A {@link AbstractKeyedStateBackend} that keeps state on the Java Heap and will serialize state to
 * streams provided by a {@link CheckpointStreamFactory} upon checkpointing.
 *
 * @param <K> The key by which state is keyed.
 */
public class HeapKeyedStateBackend<K> extends AbstractKeyedStateBackend<K> {

	private static final Logger LOG = LoggerFactory.getLogger(HeapKeyedStateBackend.class);

	private static final Map<Class<? extends StateDescriptor>, StateFactory> STATE_FACTORIES =
		Stream.of(
			Tuple2.of(ValueStateDescriptor.class, (StateFactory) HeapValueState::create),
			Tuple2.of(ListStateDescriptor.class, (StateFactory) HeapListState::create),
			Tuple2.of(MapStateDescriptor.class, (StateFactory) HeapMapState::create),
			Tuple2.of(AggregatingStateDescriptor.class, (StateFactory) HeapAggregatingState::create),
			Tuple2.of(ReducingStateDescriptor.class, (StateFactory) HeapReducingState::create),
			Tuple2.of(FoldingStateDescriptor.class, (StateFactory) HeapFoldingState::create)
		).collect(Collectors.toMap(t -> t.f0, t -> t.f1));

	/**
	 * Map of registered Key/Value states.
	 */
	private final Map<String, StateTable<K, ?, ?>> registeredKVStates;

	/**
	 * Map of registered priority queue set states.
	 */
	private final Map<String, HeapPriorityQueueSnapshotRestoreWrapper> registeredPQStates;

	/**
	 * The configuration for local recovery.
	 */
	private final LocalRecoveryConfig localRecoveryConfig;

	/**
	 * The snapshot strategy for this backend. This determines, e.g., if snapshots are synchronous or asynchronous.
	 */
	private final HeapSnapshotStrategy<K> snapshotStrategy;

	/**
	 * Factory for state that is organized as priority queue.
	 */
	private final HeapPriorityQueueSetFactory priorityQueueSetFactory;

	public HeapKeyedStateBackend(
		TaskKvStateRegistry kvStateRegistry,
		StateSerializerProvider<K> keySerializerProvider,
		ClassLoader userCodeClassLoader,
		int numberOfKeyGroups,
		KeyGroupRange keyGroupRange,
		ExecutionConfig executionConfig,
		TtlTimeProvider ttlTimeProvider,
		CloseableRegistry cancelStreamRegistry,
		StreamCompressionDecorator keyGroupCompressionDecorator,
		Map<String, StateTable<K, ?, ?>> registeredKVStates,
		Map<String, HeapPriorityQueueSnapshotRestoreWrapper> registeredPQStates,
		LocalRecoveryConfig localRecoveryConfig,
		HeapPriorityQueueSetFactory priorityQueueSetFactory,
		HeapSnapshotStrategy<K> snapshotStrategy
	) {
		super(kvStateRegistry, keySerializerProvider, userCodeClassLoader, numberOfKeyGroups,
			keyGroupRange, executionConfig, ttlTimeProvider, cancelStreamRegistry, keyGroupCompressionDecorator);
		this.registeredKVStates = registeredKVStates;
		this.registeredPQStates = registeredPQStates;
		this.localRecoveryConfig = localRecoveryConfig;
		LOG.info("Initializing heap keyed state backend with stream factory.");
		this.priorityQueueSetFactory = priorityQueueSetFactory;
		this.snapshotStrategy = snapshotStrategy;
	}

	// ------------------------------------------------------------------------
	//  state backend operations
	// ------------------------------------------------------------------------

	@SuppressWarnings("unchecked")
	@Nonnull
	@Override
	public <T extends HeapPriorityQueueElement & PriorityComparable & Keyed> KeyGroupedInternalPriorityQueue<T> create(
		@Nonnull String stateName,
		@Nonnull TypeSerializer<T> byteOrderedElementSerializer) {

		final HeapPriorityQueueSnapshotRestoreWrapper existingState = registeredPQStates.get(stateName);

		if (existingState != null) {
			// TODO we implement the simple way of supporting the current functionality, mimicking keyed state
			// because this should be reworked in FLINK-9376 and then we should have a common algorithm over
			// StateMetaInfoSnapshot that avoids this code duplication.

			TypeSerializerSchemaCompatibility<T> compatibilityResult =
				existingState.getMetaInfo().updateElementSerializer(byteOrderedElementSerializer);

			if (compatibilityResult.isIncompatible()) {
				throw new FlinkRuntimeException(new StateMigrationException("For heap backends, the new priority queue serializer must not be incompatible."));
			} else {
				registeredPQStates.put(
					stateName,
					existingState.forUpdatedSerializer(byteOrderedElementSerializer));
			}

			return existingState.getPriorityQueue();
		} else {
			final RegisteredPriorityQueueStateBackendMetaInfo<T> metaInfo =
				new RegisteredPriorityQueueStateBackendMetaInfo<>(stateName, byteOrderedElementSerializer);
			return createInternal(metaInfo);
		}
	}

	@Nonnull
	private <T extends HeapPriorityQueueElement & PriorityComparable & Keyed> KeyGroupedInternalPriorityQueue<T> createInternal(
		RegisteredPriorityQueueStateBackendMetaInfo<T> metaInfo) {

		final String stateName = metaInfo.getName();
		final HeapPriorityQueueSet<T> priorityQueue = priorityQueueSetFactory.create(
			stateName,
			metaInfo.getElementSerializer());

		HeapPriorityQueueSnapshotRestoreWrapper<T> wrapper =
			new HeapPriorityQueueSnapshotRestoreWrapper<>(
				priorityQueue,
				metaInfo,
				KeyExtractorFunction.forKeyedObjects(),
				keyGroupRange,
				numberOfKeyGroups);

		registeredPQStates.put(stateName, wrapper);
		return priorityQueue;
	}

	private <N, V> StateTable<K, N, V> tryRegisterStateTable(
		TypeSerializer<N> namespaceSerializer,
		StateDescriptor<?, V> stateDesc,
		@Nonnull StateSnapshotTransformFactory<V> snapshotTransformFactory) throws StateMigrationException {

		@SuppressWarnings("unchecked")
		StateTable<K, N, V> stateTable = (StateTable<K, N, V>) registeredKVStates.get(stateDesc.getName());

		TypeSerializer<V> newStateSerializer = stateDesc.getSerializer();

		if (stateTable != null) {
			RegisteredKeyValueStateBackendMetaInfo<N, V> restoredKvMetaInfo = stateTable.getMetaInfo();

			restoredKvMetaInfo.updateSnapshotTransformFactory(snapshotTransformFactory);

			TypeSerializerSchemaCompatibility<N> namespaceCompatibility =
				restoredKvMetaInfo.updateNamespaceSerializer(namespaceSerializer);
			if (namespaceCompatibility.isCompatibleAfterMigration() || namespaceCompatibility.isIncompatible()) {
				throw new StateMigrationException("For heap backends, the new namespace serializer must be compatible.");
			}

			restoredKvMetaInfo.checkStateMetaInfo(stateDesc);

			TypeSerializerSchemaCompatibility<V> stateCompatibility =
				restoredKvMetaInfo.updateStateSerializer(newStateSerializer);

			if (stateCompatibility.isIncompatible()) {
				throw new StateMigrationException("For heap backends, the new state serializer must not be incompatible.");
			}

			stateTable.setMetaInfo(restoredKvMetaInfo);
		} else {
			RegisteredKeyValueStateBackendMetaInfo<N, V> newMetaInfo = new RegisteredKeyValueStateBackendMetaInfo<>(
				stateDesc.getType(),
				stateDesc.getName(),
				namespaceSerializer,
				newStateSerializer,
				snapshotTransformFactory);

			stateTable = snapshotStrategy.newStateTable(this, newMetaInfo);
			registeredKVStates.put(stateDesc.getName(), stateTable);
		}

		return stateTable;
	}

	@SuppressWarnings("unchecked")
	@Override
	public <N> Stream<K> getKeys(String state, N namespace) {
		if (!registeredKVStates.containsKey(state)) {
			return Stream.empty();
		}

		final StateSnapshotRestore stateSnapshotRestore = registeredKVStates.get(state);
		StateTable<K, N, ?> table = (StateTable<K, N, ?>) stateSnapshotRestore;
		return table.getKeys(namespace);
	}

	@Override
	@Nonnull
	public <N, SV, SEV, S extends State, IS extends S> IS createInternalState(
		@Nonnull TypeSerializer<N> namespaceSerializer,
		@Nonnull StateDescriptor<S, SV> stateDesc,
		@Nonnull StateSnapshotTransformFactory<SEV> snapshotTransformFactory) throws Exception {
		StateFactory stateFactory = STATE_FACTORIES.get(stateDesc.getClass());
		if (stateFactory == null) {
			String message = String.format("State %s is not supported by %s",
				stateDesc.getClass(), this.getClass());
			throw new FlinkRuntimeException(message);
		}
		StateTable<K, N, SV> stateTable = tryRegisterStateTable(
			namespaceSerializer, stateDesc, getStateSnapshotTransformFactory(stateDesc, snapshotTransformFactory));
		return stateFactory.createState(stateDesc, stateTable, getKeySerializer());
	}

	@SuppressWarnings("unchecked")
	private <SV, SEV> StateSnapshotTransformFactory<SV> getStateSnapshotTransformFactory(
		StateDescriptor<?, SV> stateDesc,
		StateSnapshotTransformFactory<SEV> snapshotTransformFactory) {
		if (stateDesc instanceof ListStateDescriptor) {
			return (StateSnapshotTransformFactory<SV>) new StateSnapshotTransformers.ListStateSnapshotTransformFactory<>(snapshotTransformFactory);
		} else if (stateDesc instanceof MapStateDescriptor) {
			return (StateSnapshotTransformFactory<SV>) new StateSnapshotTransformers.MapStateSnapshotTransformFactory<>(snapshotTransformFactory);
		} else {
			return (StateSnapshotTransformFactory<SV>) snapshotTransformFactory;
		}
	}

	@Nonnull
	@Override
	@SuppressWarnings("unchecked")
	public RunnableFuture<SnapshotResult<KeyedStateHandle>> snapshot(
            final long checkpointId,
            final long timestamp,
            @Nonnull final CheckpointStreamFactory streamFactory,
            @Nonnull CheckpointOptions checkpointOptions,
            boolean isChangelogEnabled,
			Set<Integer> backupKeyGroups) throws IOException {

		long startTime = System.currentTimeMillis();

		final RunnableFuture<SnapshotResult<KeyedStateHandle>> snapshotRunner =
			snapshotStrategy.snapshot(checkpointId, timestamp, streamFactory, checkpointOptions, isChangelogEnabled, backupKeyGroups);

		snapshotStrategy.logSyncCompleted(streamFactory, startTime);
		return snapshotRunner;
	}

	public RunnableFuture<SnapshotResult<KeyedStateHandle>> snapshotAffectedKeyGroups(
		final long checkpointId,
		final long timestamp,
		@Nonnull final CheckpointStreamFactory streamFactory,
		@Nonnull CheckpointOptions checkpointOptions,
		@Nullable Collection<Integer> affectedKeyGroups,
		Set<Integer> backupKeyGroups) throws IOException {

		long startTime = System.currentTimeMillis();

		final RunnableFuture<SnapshotResult<KeyedStateHandle>> snapshotRunner =
			snapshotStrategy.snapshotAffectedKeygroups(checkpointId, timestamp, streamFactory, checkpointOptions, affectedKeyGroups, backupKeyGroups);

		snapshotStrategy.logSyncCompleted(streamFactory, startTime);
		return snapshotRunner;
	}

	@Override
	public void notifyCheckpointComplete(long checkpointId) {
		//Nothing to do
	}

	@Override
	public <N, S extends State, T> void applyToAllKeys(
		final N namespace,
		final TypeSerializer<N> namespaceSerializer,
		final StateDescriptor<S, T> stateDescriptor,
		final KeyedStateFunction<K, S> function) throws Exception {

		try (Stream<K> keyStream = getKeys(stateDescriptor.getName(), namespace)) {

			// we copy the keys into list to avoid the concurrency problem
			// when state.clear() is invoked in function.process().
			final List<K> keys = keyStream.collect(Collectors.toList());

			final S state = getPartitionedState(
				namespace,
				namespaceSerializer,
				stateDescriptor);

			for (K key : keys) {
				setCurrentKey(key);
				function.process(key, state);
			}
		}
	}

	public void updateKeyGroupOffset() {
		// TODO: update this part
//		for (StateTable<K, ?, ?> registeredKVState : registeredKVStates.values()) {
//			registeredKVState.updateKeyGroupOffset();
//		}
	}

	public void updateStateTable(int numberOfKeyGroups) {
		this.numberOfKeyGroups = numberOfKeyGroups;
	}

	@Override
	public String toString() {
		return "HeapKeyedStateBackend";
	}

	/**
	 * Returns the total number of state entries across all keys/namespaces.
	 */
	@VisibleForTesting
	@SuppressWarnings("unchecked")
	@Override
	public int numKeyValueStateEntries() {
		int sum = 0;
		for (StateSnapshotRestore state : registeredKVStates.values()) {
			sum += ((StateTable<?, ?, ?>) state).size();
		}
		return sum;
	}

	/**
	 * Returns the total number of state entries across all keys for the given namespace.
	 */
	@VisibleForTesting
	public int numKeyValueStateEntries(Object namespace) {
		int sum = 0;
		for (StateTable<?, ?, ?> state : registeredKVStates.values()) {
			sum += state.sizeOfNamespace(namespace);
		}
		return sum;
	}

	@Override
	public boolean supportsAsynchronousSnapshots() {
		return snapshotStrategy.isAsynchronous();
	}

	@VisibleForTesting
	public LocalRecoveryConfig getLocalRecoveryConfig() {
		return localRecoveryConfig;
	}

	private interface StateFactory {
		<K, N, SV, S extends State, IS extends S> IS createState(
			StateDescriptor<S, SV> stateDesc,
			StateTable<K, N, SV> stateTable,
			TypeSerializer<K> keySerializer) throws Exception;
	}

	public ClassLoader getUserCodeClassLoader() {
		return userCodeClassLoader;
	}

	public Map<String, StateTable<K, ?, ?>> getRegisteredKVStates() {
		return registeredKVStates;
	}

	public Map<String, HeapPriorityQueueSnapshotRestoreWrapper> getRegisteredPQStates() {
		return registeredPQStates;
	}
}
