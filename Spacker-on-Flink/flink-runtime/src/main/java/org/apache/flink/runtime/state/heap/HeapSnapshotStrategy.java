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

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.core.fs.CloseableRegistry;
import org.apache.flink.core.memory.DataOutputViewStreamWrapper;
import org.apache.flink.runtime.checkpoint.CheckpointOptions;
import org.apache.flink.runtime.state.*;
import org.apache.flink.runtime.state.metainfo.StateMetaInfoSnapshot;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.function.SupplierWithException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.IOException;
import java.io.OutputStream;
import java.util.*;
import java.util.concurrent.FutureTask;
import java.util.concurrent.RunnableFuture;

/**
 * Base class for the snapshots of the heap backend that outlines the algorithm and offers some hooks to realize
 * the concrete strategies. Subclasses must be threadsafe.
 */
class HeapSnapshotStrategy<K>
	extends AbstractSnapshotStrategy<KeyedStateHandle> implements SnapshotStrategySynchronicityBehavior<K> {

	private static final Logger LOG = LoggerFactory.getLogger(HeapSnapshotStrategy.class);

	private final SnapshotStrategySynchronicityBehavior<K> snapshotStrategySynchronicityTrait;
	private final Map<String, StateTable<K, ?, ?>> registeredKVStates;
	private final Map<String, HeapPriorityQueueSnapshotRestoreWrapper> registeredPQStates;
	private final StreamCompressionDecorator keyGroupCompressionDecorator;
	private final LocalRecoveryConfig localRecoveryConfig;
	private final KeyGroupRange keyGroupRange;
	private final CloseableRegistry cancelStreamRegistry;
	private final StateSerializerProvider<K> keySerializerProvider;

	HeapSnapshotStrategy(
		SnapshotStrategySynchronicityBehavior<K> snapshotStrategySynchronicityTrait,
		Map<String, StateTable<K, ?, ?>> registeredKVStates,
		Map<String, HeapPriorityQueueSnapshotRestoreWrapper> registeredPQStates,
		StreamCompressionDecorator keyGroupCompressionDecorator,
		LocalRecoveryConfig localRecoveryConfig,
		KeyGroupRange keyGroupRange,
		CloseableRegistry cancelStreamRegistry,
		StateSerializerProvider<K> keySerializerProvider) {
		super("Heap backend snapshot");
		this.snapshotStrategySynchronicityTrait = snapshotStrategySynchronicityTrait;
		this.registeredKVStates = registeredKVStates;
		this.registeredPQStates = registeredPQStates;
		this.keyGroupCompressionDecorator = keyGroupCompressionDecorator;
		this.localRecoveryConfig = localRecoveryConfig;
		this.keyGroupRange = keyGroupRange;
		this.cancelStreamRegistry = cancelStreamRegistry;
		this.keySerializerProvider = keySerializerProvider;
	}

	@Nonnull
	@Override
	public RunnableFuture<SnapshotResult<KeyedStateHandle>> snapshot(
		long checkpointId,
		long timestamp,
		@Nonnull CheckpointStreamFactory primaryStreamFactory,
		@Nonnull CheckpointOptions checkpointOptions,
		boolean isChangelogEnabled,
		Set<Integer> backupKeyGroups) throws IOException {

		// In HeapSnapshotStrategy, we have to snapshot affected keygroups that will be replicated remotely.

		if (!hasRegisteredState()) {
			return DoneFuture.of(SnapshotResult.empty());
		}

		int numStates = registeredKVStates.size() + registeredPQStates.size();

		Preconditions.checkState(numStates <= Short.MAX_VALUE,
			"Too many states: " + numStates +
				". Currently at most " + Short.MAX_VALUE + " states are supported");

		final List<StateMetaInfoSnapshot> metaInfoSnapshots = new ArrayList<>(numStates);
		final Map<StateUID, Integer> stateNamesToId =
			new HashMap<>(numStates);
		final Map<StateUID, StateSnapshot> cowStateStableSnapshots =
			new HashMap<>(numStates);

		processSnapshotMetaInfoForAllStates(
			metaInfoSnapshots,
			cowStateStableSnapshots,
			stateNamesToId,
			registeredKVStates,
			StateMetaInfoSnapshot.BackendStateType.KEY_VALUE);

		processSnapshotMetaInfoForAllStates(
			metaInfoSnapshots,
			cowStateStableSnapshots,
			stateNamesToId,
			registeredPQStates,
			StateMetaInfoSnapshot.BackendStateType.PRIORITY_QUEUE);

		final KeyedBackendSerializationProxy<K> serializationProxy =
			new KeyedBackendSerializationProxy<>(
				// TODO: this code assumes that writing a serializer is threadsafe, we should support to
				// get a serialized form already at state registration time in the future
				getKeySerializer(),
				metaInfoSnapshots,
				!Objects.equals(UncompressedStreamCompressionDecorator.INSTANCE, keyGroupCompressionDecorator));

		final SupplierWithException<CheckpointStreamWithResultProvider, Exception> checkpointStreamSupplier =

			localRecoveryConfig.isLocalRecoveryEnabled() ?

				() -> CheckpointStreamWithResultProvider.createDuplicatingStream(
					checkpointId,
					CheckpointedStateScope.EXCLUSIVE,
					primaryStreamFactory,
					localRecoveryConfig.getLocalStateDirectoryProvider()) :

				() -> CheckpointStreamWithResultProvider.createSimpleStream(
					CheckpointedStateScope.EXCLUSIVE,
					primaryStreamFactory);

		//--------------------------------------------------- this becomes the end of sync part

		final AsyncSnapshotCallable<SnapshotResult<KeyedStateHandle>> asyncSnapshotCallable =
			new AsyncSnapshotCallable<SnapshotResult<KeyedStateHandle>>() {
				@Override
				protected SnapshotResult<KeyedStateHandle> callInternal() throws Exception {

					final CheckpointStreamWithResultProvider streamWithResultProvider =
						checkpointStreamSupplier.get();

					snapshotCloseableRegistry.registerCloseable(streamWithResultProvider);

					final CheckpointStreamFactory.CheckpointStateOutputStream localStream =
						streamWithResultProvider.getCheckpointOutputStream();

					final DataOutputViewStreamWrapper outView = new DataOutputViewStreamWrapper(localStream);
					serializationProxy.write(outView);

					final long[] keyGroupRangeOffsets = new long[keyGroupRange.getNumberOfKeyGroups()];
					final boolean[] changelogs = new boolean[keyGroupRange.getNumberOfKeyGroups()];

					int changelogCount = 0;
					int totalStateCount = 0;


					if (isChangelogEnabled) {
						changlogedSnapshotHandle(localStream, outView, keyGroupRangeOffsets, changelogs, changelogCount, totalStateCount, backupKeyGroups);
					} else {
						nonChangelogedSnapshotHandle(localStream, outView, keyGroupRangeOffsets);
					}

					if (snapshotCloseableRegistry.unregisterCloseable(streamWithResultProvider)) {
						KeyGroupRangeOffsets kgOffs = new KeyGroupRangeOffsets(keyGroupRange, keyGroupRangeOffsets, changelogs);
						SnapshotResult<StreamStateHandle> result =
							streamWithResultProvider.closeAndFinalizeCheckpointStreamResult();
						return CheckpointStreamWithResultProvider.toKeyedStateHandleSnapshotResult(result, kgOffs);
					} else {
						throw new IOException("Stream already unregistered.");
					}
				}

				private void nonChangelogedSnapshotHandle(CheckpointStreamFactory.CheckpointStateOutputStream localStream,
														  DataOutputViewStreamWrapper outView,
														  long[] keyGroupRangeOffsets) throws IOException {
					for (int keyGroupPos = 0; keyGroupPos < keyGroupRange.getNumberOfKeyGroups(); ++keyGroupPos) {
						int alignedKeyGroupId = keyGroupRange.getKeyGroupId(keyGroupPos);
						keyGroupRangeOffsets[keyGroupPos] = localStream.getPos();

						int hashedKeyGroupId = keyGroupRange.mapFromAlignedToHashed(alignedKeyGroupId);
						checkAndSerializeAllKeyState(localStream, outView, keyGroupRangeOffsets, keyGroupPos,
							alignedKeyGroupId, hashedKeyGroupId, cowStateStableSnapshots, stateNamesToId);
					}
				}

				private void changlogedSnapshotHandle(CheckpointStreamFactory.CheckpointStateOutputStream localStream,
													  DataOutputViewStreamWrapper outView,
													  long[] keyGroupRangeOffsets,
													  boolean[] changelogs,
													  int changelogCount,
													  int totalStateCount,
													  Set<Integer> backupKeyGroups) throws IOException {
					for (int keyGroupPos = 0; keyGroupPos < keyGroupRange.getNumberOfKeyGroups(); ++keyGroupPos) {
						int alignedKeyGroupId = keyGroupRange.getKeyGroupId(keyGroupPos);
						keyGroupRangeOffsets[keyGroupPos] = localStream.getPos();

						int hashedKeyGroupId = keyGroupRange.mapFromAlignedToHashed(alignedKeyGroupId);

						totalStateCount++;

						if (backupKeyGroups.contains(hashedKeyGroupId)) {
							if (checkAndSerializeChanglogedKeyState(localStream, outView, keyGroupRangeOffsets, keyGroupPos,
								alignedKeyGroupId, hashedKeyGroupId, changelogs, cowStateStableSnapshots, stateNamesToId)) {
								changelogCount++;
							}
						}
					}

					LOG.info("++++--- Changeloged State: " + changelogCount + "/" + totalStateCount);
				}

				@Override
				protected void cleanupProvidedResources() {
					for (StateSnapshot tableSnapshot : cowStateStableSnapshots.values()) {
						tableSnapshot.release();
						// reset changelogs for current checkpoint.
						if (tableSnapshot.getChangelogs() != null) {
							tableSnapshot.releaseChangeLogs();
						}
					}
					LOG.info("++++--- Snapshot keygroups and cleanup completed");
				}

				@Override
				protected void logAsyncSnapshotComplete(long startTime) {
					if (snapshotStrategySynchronicityTrait.isAsynchronous()) {
						logAsyncCompleted(primaryStreamFactory, startTime);
					}
				}
			};

		final FutureTask<SnapshotResult<KeyedStateHandle>> task =
			asyncSnapshotCallable.toAsyncSnapshotFutureTask(cancelStreamRegistry);
		finalizeSnapshotBeforeReturnHook(task);

		return task;
	}

	@Nonnull
	public RunnableFuture<SnapshotResult<KeyedStateHandle>> snapshotAffectedKeygroups(
		long checkpointId,
		long timestamp,
		@Nonnull CheckpointStreamFactory primaryStreamFactory,
		@Nonnull CheckpointOptions checkpointOptions,
		@Nullable Collection<Integer> affectedKeyGroups,
		Set<Integer> backupKeyGroups) throws IOException {

		// Snapshot affected keygroup, if it is not in backup keygroup set, or it is in backup keygroup set but not changeloged.

		if (!hasRegisteredState()) {
			return DoneFuture.of(SnapshotResult.empty());
		}

		int numStates = registeredKVStates.size() + registeredPQStates.size();

		Preconditions.checkState(numStates <= Short.MAX_VALUE,
			"Too many states: " + numStates +
				". Currently at most " + Short.MAX_VALUE + " states are supported");

		final List<StateMetaInfoSnapshot> metaInfoSnapshots = new ArrayList<>(numStates);
		final Map<StateUID, Integer> stateNamesToId =
			new HashMap<>(numStates);
		final Map<StateUID, StateSnapshot> cowStateStableSnapshots =
			new HashMap<>(numStates);

		processSnapshotMetaInfoForAllStates(
			metaInfoSnapshots,
			cowStateStableSnapshots,
			stateNamesToId,
			registeredKVStates,
			StateMetaInfoSnapshot.BackendStateType.KEY_VALUE);

		processSnapshotMetaInfoForAllStates(
			metaInfoSnapshots,
			cowStateStableSnapshots,
			stateNamesToId,
			registeredPQStates,
			StateMetaInfoSnapshot.BackendStateType.PRIORITY_QUEUE);

		final KeyedBackendSerializationProxy<K> serializationProxy =
			new KeyedBackendSerializationProxy<>(
				// TODO: this code assumes that writing a serializer is threadsafe, we should support to
				// get a serialized form already at state registration time in the future
				getKeySerializer(),
				metaInfoSnapshots,
				!Objects.equals(UncompressedStreamCompressionDecorator.INSTANCE, keyGroupCompressionDecorator));

		final SupplierWithException<CheckpointStreamWithResultProvider, Exception> checkpointStreamSupplier =

			localRecoveryConfig.isLocalRecoveryEnabled() ?

				() -> CheckpointStreamWithResultProvider.createDuplicatingStream(
					checkpointId,
					CheckpointedStateScope.EXCLUSIVE,
					primaryStreamFactory,
					localRecoveryConfig.getLocalStateDirectoryProvider()) :

				() -> CheckpointStreamWithResultProvider.createSimpleStream(
					CheckpointedStateScope.EXCLUSIVE,
					primaryStreamFactory);

		//--------------------------------------------------- this becomes the end of sync part

		final AsyncSnapshotCallable<SnapshotResult<KeyedStateHandle>> asyncSnapshotCallable =
			new AsyncSnapshotCallable<SnapshotResult<KeyedStateHandle>>() {
				@Override
				protected SnapshotResult<KeyedStateHandle> callInternal() throws Exception {

					final CheckpointStreamWithResultProvider streamWithResultProvider =
						checkpointStreamSupplier.get();

					snapshotCloseableRegistry.registerCloseable(streamWithResultProvider);

					final CheckpointStreamFactory.CheckpointStateOutputStream localStream =
						streamWithResultProvider.getCheckpointOutputStream();

					final DataOutputViewStreamWrapper outView = new DataOutputViewStreamWrapper(localStream);
					serializationProxy.write(outView);

					final long[] keyGroupRangeOffsets = new long[keyGroupRange.getNumberOfKeyGroups()];
					final boolean[] changelogs = new boolean[keyGroupRange.getNumberOfKeyGroups()];

					Preconditions.checkState(affectedKeyGroups != null,
						"++++++ Need to provide an affected keygroup list ");

					int changelogCount = 0;
					int totalStateCount = 0;

					for (int keyGroupPos = 0; keyGroupPos < keyGroupRange.getNumberOfKeyGroups(); ++keyGroupPos) {
						int alignedKeyGroupId = keyGroupRange.getKeyGroupId(keyGroupPos);
						keyGroupRangeOffsets[keyGroupPos] = localStream.getPos();
						int hashedKeyGroupId = keyGroupRange.mapFromAlignedToHashed(alignedKeyGroupId);

						totalStateCount++;

						if (affectedKeyGroups.contains(hashedKeyGroupId)) {
							// Snapshot affected keygroup, if it is not in backup keygroup set, or it is in backup keygroup set but is not the latest
							if (!backupKeyGroups.contains(hashedKeyGroupId)) {
								checkAndSerializeAllKeyState(localStream, outView, keyGroupRangeOffsets, keyGroupPos,
									alignedKeyGroupId, hashedKeyGroupId, cowStateStableSnapshots, stateNamesToId);
								changelogCount++;
							} else if (checkAndSerializeChanglogedKeyState(localStream, outView, keyGroupRangeOffsets, keyGroupPos,
								alignedKeyGroupId, hashedKeyGroupId, changelogs, cowStateStableSnapshots, stateNamesToId)) {
								changelogCount++;
							}
						}
					}

					LOG.info("++++--- State Migration Changeloged State: " + changelogCount + "/" + totalStateCount);

					if (snapshotCloseableRegistry.unregisterCloseable(streamWithResultProvider)) {
						KeyGroupRangeOffsets kgOffs = new KeyGroupRangeOffsets(keyGroupRange, keyGroupRangeOffsets, changelogs);
						SnapshotResult<StreamStateHandle> result =
							streamWithResultProvider.closeAndFinalizeCheckpointStreamResult();
						return CheckpointStreamWithResultProvider.toKeyedStateHandleSnapshotResult(result, kgOffs);
					} else {
						throw new IOException("Stream already unregistered.");
					}
				}

				@Override
				protected void cleanupProvidedResources() {
					for (StateSnapshot tableSnapshot : cowStateStableSnapshots.values()) {
						tableSnapshot.release();
							// reset changelogs for current checkpoint.
						// TODO: do not release the changeloged keys when just doing state migration.
						// TODO: for window operations we need to clean up the priority queues which contains timers for the associated keys.
						// TODO: our current work around is to bypass those timer and rely on the timer clean scheme to clean them up, not elegant.
//						if (affectedKeyGroups != null && tableSnapshot instanceof HeapPriorityQueueStateSnapshot) {
//							tableSnapshot.releaseChangeLogs(affectedKeyGroups);
//						}
					}
					LOG.info("++++--- Snapshot affected keygroups completed");
				}

				@Override
				protected void logAsyncSnapshotComplete(long startTime) {
					if (snapshotStrategySynchronicityTrait.isAsynchronous()) {
						logAsyncCompleted(primaryStreamFactory, startTime);
					}
				}
			};

		final FutureTask<SnapshotResult<KeyedStateHandle>> task =
			asyncSnapshotCallable.toAsyncSnapshotFutureTask(cancelStreamRegistry);
		finalizeSnapshotBeforeReturnHook(task);

		return task;
	}

	private void checkAndSerializeAllKeyState(
		CheckpointStreamFactory.CheckpointStateOutputStream localStream,
		DataOutputViewStreamWrapper outView,
		long[] keyGroupRangeOffsets,
		int keyGroupPos,
		int alignedKeyGroupId,
		int hashedKeyGroupId,
		Map<StateUID, StateSnapshot> cowStateStableSnapshots,
		Map<StateUID, Integer> stateNamesToId) throws IOException {
		outView.writeInt(hashedKeyGroupId);

//		LOG.info("+++++--- keyGroupRange: " + keyGroupRange +
//			", alignedKeyGroupIndex: " + alignedKeyGroupId +
//			", offset: " + keyGroupRangeOffsets[keyGroupPos] +
//			", hashedKeyGroupIndex: " + hashedKeyGroupId);

		for (Map.Entry<StateUID, StateSnapshot> stateSnapshot :
			cowStateStableSnapshots.entrySet()) {
			StateSnapshot.StateKeyGroupWriter partitionedSnapshot =

				stateSnapshot.getValue().getKeyGroupWriter();
			try (
				OutputStream kgCompressionOut =
					keyGroupCompressionDecorator.decorateWithCompression(localStream)) {
				DataOutputViewStreamWrapper kgCompressionView =
					new DataOutputViewStreamWrapper(kgCompressionOut);
				kgCompressionView.writeShort(stateNamesToId.get(stateSnapshot.getKey()));
				partitionedSnapshot.writeStateInKeyGroup(kgCompressionView, alignedKeyGroupId);
			} // this will just close the outer compression stream
		}
	}

	private boolean checkAndSerializeChanglogedKeyState(
		CheckpointStreamFactory.CheckpointStateOutputStream localStream,
		DataOutputViewStreamWrapper outView,
		long[] keyGroupRangeOffsets,
		int keyGroupPos,
		int alignedKeyGroupId,
		int hashedKeyGroupId,
		boolean[] changelogs,
		Map<StateUID, StateSnapshot> cowStateStableSnapshots,
		Map<StateUID, Integer> stateNamesToId) throws IOException {

		// first read the changelogs, then compress all state from both kvstate and pqstate
		for (Map.Entry<StateUID, StateSnapshot> stateSnapshot :
			cowStateStableSnapshots.entrySet()) {

			if ((stateSnapshot.getValue()).getChangelogs() != null) {
				if (stateSnapshot.getValue().getChangelogs()
					.containsKey(hashedKeyGroupId)) {
					changelogs[keyGroupPos] = true;
				}
			}
		}

		if (changelogs[keyGroupPos]) {

			outView.writeInt(hashedKeyGroupId);

//			LOG.debug("+++++--- keyGroupRange: " + keyGroupRange +
//				", alignedKeyGroupIndex: " + alignedKeyGroupId +
//				", offset: " + keyGroupRangeOffsets[keyGroupPos] +
//				", hashedKeyGroupIndex: " + hashedKeyGroupId);

			for (Map.Entry<StateUID, StateSnapshot> stateSnapshot :
			cowStateStableSnapshots.entrySet()) {

				StateSnapshot.StateKeyGroupWriter partitionedSnapshot =
					stateSnapshot.getValue().getKeyGroupWriter();

				try (
					OutputStream kgCompressionOut =
						keyGroupCompressionDecorator.decorateWithCompression(localStream)) {
					DataOutputViewStreamWrapper kgCompressionView =
						new DataOutputViewStreamWrapper(kgCompressionOut);
					kgCompressionView.writeShort(stateNamesToId.get(stateSnapshot.getKey()));
					partitionedSnapshot.writeStateInKeyGroup(kgCompressionView, alignedKeyGroupId);
				} // this will just close the outer compression stream
			}
		}

		return changelogs[keyGroupPos];
	}

	@Override
	public void finalizeSnapshotBeforeReturnHook(Runnable runnable) {
		snapshotStrategySynchronicityTrait.finalizeSnapshotBeforeReturnHook(runnable);
	}

	@Override
	public boolean isAsynchronous() {
		return snapshotStrategySynchronicityTrait.isAsynchronous();
	}

	@Override
	public <N, V> StateTable<K, N, V> newStateTable(
		InternalKeyContext<K> keyContext,
		RegisteredKeyValueStateBackendMetaInfo<N, V> newMetaInfo) {
		return snapshotStrategySynchronicityTrait.newStateTable(keyContext, newMetaInfo);
	}

	private void processSnapshotMetaInfoForAllStates(
		List<StateMetaInfoSnapshot> metaInfoSnapshots,
		Map<StateUID, StateSnapshot> cowStateStableSnapshots,
		Map<StateUID, Integer> stateNamesToId,
		Map<String, ? extends StateSnapshotRestore> registeredStates,
		StateMetaInfoSnapshot.BackendStateType stateType) {

		for (Map.Entry<String, ? extends StateSnapshotRestore> kvState : registeredStates.entrySet()) {
			final StateUID stateUid = StateUID.of(kvState.getKey(), stateType);
			stateNamesToId.put(stateUid, stateNamesToId.size());
			StateSnapshotRestore state = kvState.getValue();
			if (null != state) {
				final StateSnapshot stateSnapshot = state.stateSnapshot();
				metaInfoSnapshots.add(stateSnapshot.getMetaInfoSnapshot());
				cowStateStableSnapshots.put(stateUid, stateSnapshot);
			}
		}
	}

	private boolean hasRegisteredState() {
		return !(registeredKVStates.isEmpty() && registeredPQStates.isEmpty());
	}

	public TypeSerializer<K> getKeySerializer() {
		return keySerializerProvider.currentSchemaSerializer();
	}
}
