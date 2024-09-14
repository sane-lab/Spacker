package org.apache.flink.runtime.spector.netty.data;

import java.io.Serializable;

/**
 * Socket address and port in task executor netty server.
 */
public class CheckpointCoordinatorSocketAddress implements Serializable {
	private static final long serialVersionUID = 1L;

	private final String address;
	private final int port;

	public CheckpointCoordinatorSocketAddress(String address, int port) {
		this.address = address;
		this.port = port;
	}

	public String getAddress() {
		return address;
	}

	public int getPort() {
		return port;
	}
}
