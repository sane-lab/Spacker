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

package org.apache.flink.runtime.spector.migration;

import java.io.Serializable;

public class ReconfigOptions implements Serializable {

	private final boolean updatePartitions;

	private final boolean updateGates;

	private final boolean updateState;

	private final boolean updateKeyGroupRange;

	private final boolean setAffectedKeys;

	public ReconfigOptions(boolean updatePartitions, boolean updateGates, boolean updateState, boolean updateKeyGroupRange) {
		this(updatePartitions, updateGates, updateState, updateKeyGroupRange, false);
	}

	public ReconfigOptions(boolean updatePartitions, boolean updateGates, boolean updateState, boolean updateKeyGroupRange, boolean setAffectedKeys) {
		this.updatePartitions = updatePartitions;
		this.updateGates = updateGates;
		this.updateState = updateState;
		this.updateKeyGroupRange = updateKeyGroupRange;
		this.setAffectedKeys = setAffectedKeys;
	}

	public boolean isUpdatingPartitions() {
		return updatePartitions;
	}

	public boolean isUpdatingGates() {
		return updateGates;
	}

	public boolean isUpdatingState() {
		return updateState;
	}

	public boolean isUpdatingKeyGroupRange() {
		return updateKeyGroupRange;
	}

	public boolean isSettingAffectedkeys() {
		return setAffectedKeys;
	}

	public final static ReconfigOptions UPDATE_PARTITIONS_ONLY = new ReconfigOptions(true, false, false, false);

	public final static ReconfigOptions UPDATE_GATES_ONLY = new ReconfigOptions(false, true, false, false);

	public final static ReconfigOptions UPDATE_BOTH = new ReconfigOptions(true, true, false, false);

	public final static ReconfigOptions REDISTRIBUTE_STATE = new ReconfigOptions(false, false, true, false);

	public final static ReconfigOptions UPDATE_KEYGROUP_RANGE_ONLY = new ReconfigOptions(false, false, false, true);

	public final static ReconfigOptions PREPARE_AFFECTED_KEYGROUPS = new ReconfigOptions(true, true, false, false, true);

	@Override
	public int hashCode() {
		return (Boolean.hashCode(updatePartitions) << 11 + Boolean.hashCode(updateGates)) << 11 + Boolean.hashCode(updateState);
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj) {
			return true;
		} else if (obj instanceof ReconfigOptions) {
			final ReconfigOptions that = (ReconfigOptions) obj;
			return this.updatePartitions == that.updatePartitions &&
				this.updateGates == that.updateGates &&
				this.updateState == that.updateState;
		} else {
			return false;
		}
	}

	@Override
	public String toString() {
		return "update partitions: " + updatePartitions
			+ " update gates: " + updateGates
			+ " update state: " + updateState
			+ " update keygroup range: " + updateKeyGroupRange
			+ " set affected keys: " + setAffectedKeys;
	}
}
