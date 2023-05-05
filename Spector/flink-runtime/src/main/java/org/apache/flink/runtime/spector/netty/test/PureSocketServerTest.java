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

package org.apache.flink.runtime.spector.netty.test;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.concurrent.CompletableFuture;

/**
 * Test case for netty socket server.
 */
public class PureSocketServerTest {
	private final byte[] message;


	PureSocketServerTest() {
		int stringLength = 1024 * 1024 * 1024;
		this.message = new byte[stringLength];
	}

	public void run() throws Exception {

//		Random rd = new Random();
//		rd.nextBytes(message);
		ByteBuffer byteBuffer = ByteBuffer.wrap(message);
		CompletableFuture<byte[]> receiveFuture = new CompletableFuture<>();

		SimpleServer simpleServer = new SimpleServer(message);
		Thread serverThread = new Thread(simpleServer);
		serverThread.start();

		SimpleClient simpleClient = new SimpleClient(message);
		Thread clientThread = new Thread(simpleClient);
		clientThread.start();
	}

	public static class SimpleServer implements Runnable {
		private final ServerSocket serverSocket;

		private final byte[] message;

		public SimpleServer(byte[] message) throws IOException {
			serverSocket = new ServerSocket(3333);
			this.message = message;
		}


		@Override
		public void run() {
			try {
				Socket socket = serverSocket.accept();
				long start = System.currentTimeMillis();

				ObjectInputStream ois = new ObjectInputStream(socket.getInputStream());
				Object ob = ois.readObject();
				System.out.println(Arrays.equals(message, (byte[]) ob));
				System.out.println("++++++complete: " + (System.currentTimeMillis() - start) + " " + ((byte[]) ob).length);
			} catch (IOException | ClassNotFoundException e) {
				e.printStackTrace();
			}
		}
	}

	public static class SimpleClient implements Runnable {
		private Socket socket;
		private final byte[] message;

		public SimpleClient(byte[] message) {
			this.message = message;
		}

		@Override
		public void run() {
			try {
				Socket socket = new Socket("localhost",3333);
				ByteBuffer byteBuffer = ByteBuffer.wrap(message);
				ObjectOutputStream oos = new ObjectOutputStream(socket.getOutputStream());
				oos.writeObject(message);
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}

	public static void main(String[] args) throws Exception {
		PureSocketServerTest nettySocketServerTest = new PureSocketServerTest();
		nettySocketServerTest.run();
	}
}
